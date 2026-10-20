//! Cut-over helper (spec R10): replace live tables by freshly loaded ones in one transaction.
//!
//! `swap` renames every `live` to `<live>__old_<yyyymmddhhmm>` (UTC) and every `new` to `live` with
//! `sp_rename`, all pairs in a single batch and transaction under `SET LOCK_TIMEOUT`, so a lock
//! that cannot be obtained in time rolls everything back. Blocking sessions are reported, never
//! killed. Dependent `SCHEMABINDING` modules and foreign keys referencing the tables are reported
//! before anything is changed. `drop_old` removes `__old_` tables older than a given time.
use super::{bracket, exec_batch_as, nstr, MsClient};
use crate::core::runlog::{self, Failure};
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use std::time::Instant;

/// SQL Server identifiers are at most 128 characters.
const MAX_IDENT: usize = 128;
const OLD_TAG: &str = "__old_";

/// `<live>__old_<yyyymmddhhmm>`; `live` is shortened so the result fits in 128 characters.
pub fn old_name(live: &str, now: NaiveDateTime) -> String {
    let suffix = format!("{OLD_TAG}{}", now.format("%Y%m%d%H%M"));
    let keep = MAX_IDENT - suffix.chars().count();
    let base: String = live.chars().take(keep).collect();
    format!("{base}{suffix}")
}

/// The timestamp of a `…__old_<yyyymmddhhmm>` name, if it has one.
pub fn parse_old_suffix(name: &str) -> Option<NaiveDateTime> {
    let i = name.rfind(OLD_TAG)?;
    let ts = &name[i + OLD_TAG.len()..];
    if ts.len() != 12 || !ts.bytes().all(|b| b.is_ascii_digit()) || i == 0 { return None; }
    NaiveDateTime::parse_from_str(&format!("{ts}00"), "%Y%m%d%H%M%S").ok()
}

/// `table`, `schema.table`, `[schema].[table]` or `"schema"."table"` -> (schema, table); the
/// schema defaults to `dbo`.
pub fn parse_name(s: &str) -> Result<(String, String)> {
    let mut parts: Vec<String> = vec![];
    let mut cur = String::new();
    let mut chars = s.trim().chars().peekable();
    let mut quoted = false;
    while let Some(ch) = chars.next() {
        match ch {
            '[' if cur.is_empty() && !quoted => {
                loop {
                    match chars.next() {
                        Some(']') if chars.peek() == Some(&']') => { chars.next(); cur.push(']'); }
                        Some(']') => break,
                        Some(c) => cur.push(c),
                        None => anyhow::bail!("unterminated [ in table name {s:?}"),
                    }
                }
                quoted = true;
            }
            '"' if cur.is_empty() && !quoted => {
                loop {
                    match chars.next() {
                        Some('"') if chars.peek() == Some(&'"') => { chars.next(); cur.push('"'); }
                        Some('"') => break,
                        Some(c) => cur.push(c),
                        None => anyhow::bail!("unterminated \" in table name {s:?}"),
                    }
                }
                quoted = true;
            }
            '.' => { parts.push(std::mem::take(&mut cur)); quoted = false; }
            c if quoted => anyhow::bail!("unexpected {c:?} after a quoted identifier in {s:?}"),
            c => cur.push(c),
        }
    }
    parts.push(cur);
    let parts: Vec<String> = parts.into_iter().map(|p| p.trim().to_string()).collect();
    if parts.iter().any(|p| p.is_empty()) { anyhow::bail!("bad table name {s:?}"); }
    match parts.len() {
        1 => Ok(("dbo".into(), parts[0].clone())),
        2 => Ok((parts[0].clone(), parts[1].clone())),
        _ => anyhow::bail!("table name {s:?}: use schema.table (database-qualified names are not supported; connect to the database instead)"),
    }
}

/// One resolved pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapPlan {
    pub new_schema: String,
    pub new_name: String,
    pub live_schema: String,
    pub live_name: String,
    pub old_name: String,
}

impl SwapPlan {
    pub fn new(new: &str, live: &str, now: NaiveDateTime) -> Result<SwapPlan> {
        let (ns, nn) = parse_name(new)?;
        let (ls, ln) = parse_name(live)?;
        if ns.eq_ignore_ascii_case(&ls) && nn.eq_ignore_ascii_case(&ln) { anyhow::bail!("swap pair {new}={live}: new and live are the same table"); }
        let old = old_name(&ln, now);
        Ok(SwapPlan { new_schema: ns, new_name: nn, live_schema: ls, live_name: ln, old_name: old })
    }
    fn live_fq(&self) -> String { format!("{}.{}", bracket(&self.live_schema), bracket(&self.live_name)) }
    fn new_fq(&self) -> String { format!("{}.{}", bracket(&self.new_schema), bracket(&self.new_name)) }
    fn old_fq(&self) -> String { format!("{}.{}", bracket(&self.live_schema), bracket(&self.old_name)) }
    /// Statements for this pair inside the transaction.
    fn statements(&self) -> Vec<String> {
        let mut v = vec![format!("EXEC sp_rename {}, {}, N'OBJECT';", nstr(&self.live_fq()), nstr(&self.old_name))];
        let mut cur = self.new_fq();
        if !self.new_schema.eq_ignore_ascii_case(&self.live_schema) {
            v.push(format!("ALTER SCHEMA {} TRANSFER {};", bracket(&self.live_schema), cur));
            cur = format!("{}.{}", bracket(&self.live_schema), bracket(&self.new_name));
        }
        if self.new_name != self.live_name {
            v.push(format!("EXEC sp_rename {}, {}, N'OBJECT';", nstr(&cur), nstr(&self.live_name)));
        }
        v
    }
}

/// The single swap batch: all renames in one transaction, rolled back as a whole on any error
/// (including a lock timeout, error 1222).
pub fn swap_batch(plans: &[SwapPlan], lock_timeout_ms: i64) -> String {
    let mut s = format!("SET LOCK_TIMEOUT {};\nSET XACT_ABORT ON;\nBEGIN TRY\n    BEGIN TRANSACTION;\n", lock_timeout_ms.max(-1));
    for p in plans {
        for st in p.statements() { s.push_str(&format!("    {st}\n")); }
    }
    s.push_str("    COMMIT TRANSACTION;\nEND TRY\nBEGIN CATCH\n    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;\n    THROW;\nEND CATCH;\n");
    s
}

async fn rows(c: &mut MsClient, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let t0 = Instant::now();
    let r = c.simple_query(sql).await.with_context(|| format!("SQL Server: {}", sql.chars().take(300).collect::<String>()))?.into_first_result().await?;
    let out: Vec<Vec<Option<String>>> = r.iter().map(|r| (0..r.len()).map(|i| r.try_get::<&str, _>(i).ok().flatten().map(|s| s.to_string())).collect()).collect();
    runlog::stmt("target", "swap", sql, out.len() as i64, t0);
    Ok(out)
}

/// `OBJECT_ID` of user tables (None when missing), in input order.
async fn object_ids(c: &mut MsClient, names: &[String]) -> Result<Vec<Option<i64>>> {
    if names.is_empty() { return Ok(vec![]); }
    let sel: Vec<String> = names.iter().enumerate().map(|(i, n)| format!("SELECT {i} AS i, CONVERT(varchar(12), OBJECT_ID({}, N'U')) AS id", nstr(n))).collect();
    let r = rows(c, &format!("SELECT CONVERT(varchar(12), i), id FROM ({}) x ORDER BY i", sel.join(" UNION ALL "))).await?;
    Ok(r.iter().map(|r| r.get(1).cloned().flatten().and_then(|s| s.parse().ok())).collect())
}

fn id_list(ids: &[i64]) -> String { ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ") }

/// Schema-bound modules (views, functions) that reference any of `ids`.
async fn schema_bound_dependents(c: &mut MsClient, ids: &[i64]) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT OBJECT_SCHEMA_NAME(d.referencing_id) + N'.' + OBJECT_NAME(d.referencing_id) + N' (' + o.type_desc + N', references ' + \
         OBJECT_SCHEMA_NAME(d.referenced_id) + N'.' + OBJECT_NAME(d.referenced_id) + N')' \
         FROM sys.sql_expression_dependencies d JOIN sys.sql_modules m ON m.object_id = d.referencing_id JOIN sys.objects o ON o.object_id = d.referencing_id \
         WHERE m.is_schema_bound = 1 AND d.referenced_id IN ({})", id_list(ids));
    Ok(rows(c, &sql).await?.into_iter().filter_map(|r| r.into_iter().next().flatten()).collect())
}

/// Foreign keys from other tables that point at any of `ids` (they would keep pointing at the
/// renamed object, not at the new live table).
async fn referencing_fks(c: &mut MsClient, ids: &[i64]) -> Result<Vec<String>> {
    let l = id_list(ids);
    let sql = format!(
        "SELECT fk.name + N' on ' + OBJECT_SCHEMA_NAME(fk.parent_object_id) + N'.' + OBJECT_NAME(fk.parent_object_id) + N' -> ' + \
         OBJECT_SCHEMA_NAME(fk.referenced_object_id) + N'.' + OBJECT_NAME(fk.referenced_object_id) \
         FROM sys.foreign_keys fk WHERE fk.referenced_object_id IN ({l}) AND fk.parent_object_id NOT IN ({l})");
    Ok(rows(c, &sql).await?.into_iter().filter_map(|r| r.into_iter().next().flatten()).collect())
}

/// Non-schema-bound views that reference the live tables (they resolve by name, so they see the
/// new table; `sp_refreshview` refreshes their column metadata).
async fn plain_views(c: &mut MsClient, ids: &[i64]) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT OBJECT_SCHEMA_NAME(d.referencing_id) + N'.' + OBJECT_NAME(d.referencing_id) \
         FROM sys.sql_expression_dependencies d JOIN sys.views v ON v.object_id = d.referencing_id JOIN sys.sql_modules m ON m.object_id = v.object_id \
         WHERE m.is_schema_bound = 0 AND d.referenced_id IN ({})", id_list(ids));
    Ok(rows(c, &sql).await?.into_iter().filter_map(|r| r.into_iter().next().flatten()).collect())
}

/// Sessions other than ours holding or waiting for locks on the given objects.
pub async fn blocking_sessions(c: &mut MsClient, ids: &[i64]) -> Result<Vec<String>> {
    if ids.is_empty() { return Ok(vec![]); }
    let sql = format!(
        "SELECT DISTINCT CONVERT(varchar(12), l.request_session_id) + N' ' + ISNULL(s.login_name, N'') + N'@' + ISNULL(s.host_name, N'') + N' (' + ISNULL(s.program_name, N'') + N'): ' + \
         l.request_mode + N' ' + l.request_status + N' on ' + ISNULL(OBJECT_SCHEMA_NAME(l.resource_associated_entity_id) + N'.' + OBJECT_NAME(l.resource_associated_entity_id), N'?') + \
         ISNULL(N', open transactions ' + CONVERT(varchar(12), s.open_transaction_count), N'') \
         FROM sys.dm_tran_locks l LEFT JOIN sys.dm_exec_sessions s ON s.session_id = l.request_session_id \
         WHERE l.resource_database_id = DB_ID() AND l.resource_type = N'OBJECT' AND l.resource_associated_entity_id IN ({}) AND l.request_session_id <> @@SPID",
        id_list(ids));
    Ok(rows(c, &sql).await?.into_iter().filter_map(|r| r.into_iter().next().flatten()).collect())
}

/// Swap `(new, live)` pairs in one transaction. Returns the statements executed (or planned with
/// `--plan`). With `keep_old = false` the `__old_` tables are dropped after the commit.
pub async fn swap(c: &mut MsClient, pairs: &[(String, String)], lock_timeout_ms: i64, keep_old: bool) -> Result<Vec<String>> {
    swap_at(c, pairs, lock_timeout_ms, keep_old, chrono::Utc::now().naive_utc()).await
}

/// [`swap`] with an explicit timestamp for the `__old_` names.
pub async fn swap_at(c: &mut MsClient, pairs: &[(String, String)], lock_timeout_ms: i64, keep_old: bool, now: NaiveDateTime) -> Result<Vec<String>> {
    if pairs.is_empty() { anyhow::bail!("swap: no table pairs given"); }
    let plans: Vec<SwapPlan> = pairs.iter().map(|(n, l)| SwapPlan::new(n, l, now)).collect::<Result<_>>()?;
    // every table at most once
    let mut seen = std::collections::HashSet::new();
    for p in &plans {
        for t in [format!("{}.{}", p.new_schema, p.new_name), format!("{}.{}", p.live_schema, p.live_name)] {
            if !seen.insert(t.to_lowercase()) { return Err(Failure::Check(format!("swap: table {t} appears in more than one pair")).into()); }
        }
    }
    // existence: new and live exist, the old name does not, nothing is in the way of a transfer
    let mut names = vec![];
    for p in &plans {
        names.push(p.new_fq());
        names.push(p.live_fq());
        names.push(p.old_fq());
        names.push(format!("{}.{}", bracket(&p.live_schema), bracket(&p.new_name)));
    }
    let ids = object_ids(c, &names).await?;
    let mut problems = vec![];
    let mut live_ids = vec![];
    let mut all_ids = vec![];
    for (i, p) in plans.iter().enumerate() {
        let (new_id, live_id, old_id, transfer_id) = (ids[4 * i], ids[4 * i + 1], ids[4 * i + 2], ids[4 * i + 3]);
        match new_id { Some(id) => all_ids.push(id), None => problems.push(format!("new table {}.{} does not exist", p.new_schema, p.new_name)) }
        match live_id { Some(id) => { all_ids.push(id); live_ids.push(id); } None => problems.push(format!("live table {}.{} does not exist", p.live_schema, p.live_name)) }
        if old_id.is_some() { problems.push(format!("{}.{} already exists (one swap per minute per table)", p.live_schema, p.old_name)); }
        if !p.new_schema.eq_ignore_ascii_case(&p.live_schema) && transfer_id.is_some() && !p.new_name.eq_ignore_ascii_case(&p.live_name) {
            problems.push(format!("cannot move {}.{} into schema {}: a table {}.{} already exists", p.new_schema, p.new_name, p.live_schema, p.live_schema, p.new_name));
        }
    }
    if !problems.is_empty() { return Err(Failure::Check(format!("swap refused: {}", problems.join("; "))).into()); }
    // dependencies that a rename would silently break
    let bound = schema_bound_dependents(c, &all_ids).await?;
    let fks = referencing_fks(c, &all_ids).await?;
    if !bound.is_empty() || !fks.is_empty() {
        let mut msg = String::from("swap refused before any change:");
        if !bound.is_empty() { msg.push_str(&format!(" schema-bound modules reference the tables (drop or alter them WITHOUT SCHEMABINDING first): {};", bound.join(", "))); }
        if !fks.is_empty() { msg.push_str(&format!(" foreign keys from other tables reference them (they would stay on the old table): {};", fks.join(", "))); }
        return Err(Failure::Check(msg.trim_end_matches(';').to_string()).into());
    }
    let views = plain_views(c, &live_ids).await?;
    if !views.is_empty() { runlog::note(&format!("swap: views referencing the live tables resolve by name and will see the new tables; consider EXEC sp_refreshview for: {}", views.join(", "))); }

    let batch = swap_batch(&plans, lock_timeout_ms);
    if let Err(e) = exec_batch_as(c, "target", "swap", &batch).await {
        let blockers = match blocking_sessions(c, &all_ids).await {
            Ok(b) if b.is_empty() => "no other session holds locks on the tables now".to_string(),
            Ok(b) => format!("sessions holding or waiting for locks (not killed): {}", b.join("; ")),
            Err(be) => format!("could not list blocking sessions: {be}"),
        };
        return Err(Failure::Run(format!("swap rolled back, nothing was renamed: {e:#}; {blockers}")).into());
    }
    let mut done = vec![batch];
    if !keep_old {
        for p in &plans {
            assert!(p.old_name.contains(OLD_TAG), "refusing to drop {}: not an __old_ table", p.old_name);
            let sql = format!("DROP TABLE {};", p.old_fq());
            exec_batch_as(c, "target", "swap", &sql).await.with_context(|| format!("swap committed, but dropping {}.{} failed", p.live_schema, p.old_name))?;
            done.push(sql);
        }
    }
    Ok(done)
}

/// Drop `…__old_<yyyymmddhhmm>` tables whose timestamp is before `before` (optionally in one
/// schema only). Returns the dropped `schema.table` names.
pub async fn drop_old(c: &mut MsClient, schema: Option<&str>, before: NaiveDateTime) -> Result<Vec<String>> {
    let filter = schema.map(|s| format!(" AND s.name = {}", nstr(s))).unwrap_or_default();
    let found = rows(c, &format!("SELECT s.name, t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name LIKE N'%[_][_]old[_]%'{filter} ORDER BY s.name, t.name")).await?;
    let mut dropped = vec![];
    for r in found {
        let (Some(s), Some(t)) = (r[0].clone(), r[1].clone()) else { continue };
        let Some(ts) = parse_old_suffix(&t) else { continue };
        if ts >= before { continue; }
        assert!(t.contains(OLD_TAG), "refusing to drop {t}: not an __old_ table");
        exec_batch_as(c, "target", "drop-old", &format!("DROP TABLE {}.{};", bracket(&s), bracket(&t))).await?;
        dropped.push(format!("{s}.{t}"));
    }
    Ok(dropped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> NaiveDateTime { NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M").unwrap() }

    #[test]
    fn old_names() {
        assert_eq!(old_name("Sales", at("2026-10-20 09:05")), "Sales__old_202610200905");
        assert_eq!(parse_old_suffix("Sales__old_202610200905"), Some(at("2026-10-20 09:05")));
        assert_eq!(parse_old_suffix("a__old_b__old_202601010000"), Some(at("2026-01-01 00:00")));
        assert_eq!(parse_old_suffix("Sales__old_ABC"), None);
        assert_eq!(parse_old_suffix("Sales__old_20261020090"), None);
        assert_eq!(parse_old_suffix("Sales__old_202613200905"), None);
        assert_eq!(parse_old_suffix("__old_202610200905"), None);
        assert_eq!(parse_old_suffix("Sales"), None);
        let long = "x".repeat(200);
        let n = old_name(&long, at("2026-10-20 09:05"));
        assert_eq!(n.chars().count(), 128);
        assert!(n.ends_with("__old_202610200905"));
        assert_eq!(parse_old_suffix(&n), Some(at("2026-10-20 09:05")));
    }

    #[test]
    fn names() {
        assert_eq!(parse_name("t").unwrap(), ("dbo".into(), "t".into()));
        assert_eq!(parse_name("stage.t").unwrap(), ("stage".into(), "t".into()));
        assert_eq!(parse_name("[my schema].[a.b]]c]").unwrap(), ("my schema".into(), "a.b]c".into()));
        assert_eq!(parse_name("\"s\".\"t\"").unwrap(), ("s".into(), "t".into()));
        assert!(parse_name("db.s.t").is_err());
        assert!(parse_name("s.").is_err());
        assert!(parse_name("[s").is_err());
        assert!(parse_name("[s]x.t").is_err());
    }

    #[test]
    fn batch_text() {
        let now = at("2026-10-20 09:05");
        let a = SwapPlan::new("dbo.Sales_new", "dbo.Sales", now).unwrap();
        let b = SwapPlan::new("stage.Items", "Items", now).unwrap();
        assert_eq!(swap_batch(&[a, b], 5000),
"SET LOCK_TIMEOUT 5000;
SET XACT_ABORT ON;
BEGIN TRY
    BEGIN TRANSACTION;
    EXEC sp_rename N'[dbo].[Sales]', N'Sales__old_202610200905', N'OBJECT';
    EXEC sp_rename N'[dbo].[Sales_new]', N'Sales', N'OBJECT';
    EXEC sp_rename N'[dbo].[Items]', N'Items__old_202610200905', N'OBJECT';
    ALTER SCHEMA [dbo] TRANSFER [stage].[Items];
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
");
        assert!(SwapPlan::new("dbo.t", "t", now).is_err());
        let q = SwapPlan::new("dbo.[it's]", "dbo.live", now).unwrap();
        assert_eq!(q.statements()[1], "EXEC sp_rename N'[dbo].[it''s]', N'live', N'OBJECT';");
        assert!(swap_batch(&[q], -5).starts_with("SET LOCK_TIMEOUT -1;"));
    }
}
