//! Run log, plan mode, progress, summary and exit codes (spec R8).
//!
//! * `--plan`: [`planning`] is true and `mssql::exec_batch` records statements instead of executing them.
//! * `--log <file>`: one line per statement/event, appended with shared read access; writes never fail the run.
//! * `--summary <file>`: JSON written by [`finish`].
//! * Exit codes: [`Failure`] variants map to 1 (check), 2 (could not run), 3 (guard).
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// A failure with a well-defined exit code.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// A check failed (types, PK, fingerprint, row counts, strict-mode warnings): exit 1.
    #[error("{0}")]
    Check(String),
    /// Could not run (connection, permissions, I/O): exit 2.
    #[error("{0}")]
    Run(String),
    /// Stopped by the safety guard: exit 3.
    #[error("{0}")]
    Guard(String),
}

/// Exit code for an error: 1 for a failed check, 3 for a guard refusal, 2 for everything else.
pub fn exit_code(e: &anyhow::Error) -> i32 {
    for cause in e.chain() {
        if let Some(f) = cause.downcast_ref::<Failure>() {
            return match f { Failure::Check(_) => 1, Failure::Run(_) => 2, Failure::Guard(_) => 3 };
        }
    }
    2
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SummaryEntry {
    pub table: String,
    pub job: String,
    pub rows: i64,
    pub seconds: f64,
    pub strategy: String,
    pub checks: Vec<String>,
    pub ok: bool,
}

struct State {
    log: Option<std::fs::File>,
    log_path: Option<PathBuf>,
    summary_path: Option<PathBuf>,
    entries: Vec<SummaryEntry>,
    statements: Vec<String>,
    started: Instant,
    progress_every_ms: u64,
    last_progress_ms: u64,
    rows_by_job: std::collections::BTreeMap<String, i64>,
}

static PLAN: AtomicBool = AtomicBool::new(false);
static SEQ: AtomicU64 = AtomicU64::new(0);
static STATE: Mutex<Option<State>> = Mutex::new(None);

fn with_state<R>(f: impl FnOnce(&mut State) -> R) -> Option<R> {
    let mut g = match STATE.lock() { Ok(g) => g, Err(p) => p.into_inner() };
    if g.is_none() {
        *g = Some(State { log: None, log_path: None, summary_path: None, entries: vec![], statements: vec![], started: Instant::now(), progress_every_ms: 10_000, last_progress_ms: 0, rows_by_job: Default::default() });
    }
    g.as_mut().map(f)
}

/// Configure the run log. Safe to call once per process; later calls replace the settings.
pub fn init(log_file: Option<&Path>, summary_file: Option<&Path>, plan_only: bool, progress_seconds: u64) {
    PLAN.store(plan_only, Ordering::Relaxed);
    with_state(|s| {
        s.log_path = log_file.map(|p| p.to_path_buf());
        s.log = log_file.and_then(|p| std::fs::OpenOptions::new().create(true).append(true).open(p).ok());
        s.summary_path = summary_file.map(|p| p.to_path_buf());
        s.progress_every_ms = progress_seconds.max(1) * 1000;
        s.started = Instant::now();
    });
    if plan_only { note("PLAN MODE: statements are listed, not executed"); }
}

/// True when `--plan` is active: write statements are recorded, never sent.
pub fn planning() -> bool { PLAN.load(Ordering::Relaxed) }

fn now_iso() -> String { chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string() }

fn write_line(line: &str) {
    with_state(|s| {
        if let Some(f) = s.log.as_mut() {
            // never let a logging problem stop a load
            let _ = writeln!(f, "{line}");
        }
    });
}

/// Record one statement sent to a server (or planned). `role` is `source`, `target`, `master`, `duckdb`.
pub fn stmt(role: &str, job: &str, sql: &str, rows: i64, started: Instant) {
    let n = SEQ.fetch_add(1, Ordering::Relaxed) + 1;
    let one_line: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    let short: String = one_line.chars().take(4000).collect();
    let tag = if planning() { "PLAN" } else { "STMT" };
    let line = format!("{}\t{tag}\t#{n}\t{role}\t{job}\t{:.3}s\t{rows}\t{short}", now_iso(), started.elapsed().as_secs_f64());
    write_line(&line);
    if planning() {
        with_state(|s| s.statements.push(format!("[{role}] {}", one_line)));
    }
}

/// Free-form event line.
pub fn note(msg: &str) {
    let line = format!("{}\tNOTE\t{}", now_iso(), msg.replace('\n', " "));
    write_line(&line);
}

/// Rate-limited progress: rows/s per job and in total, written to stderr and the log.
pub fn progress(job: &str, rows: i64) {
    let Some(out) = with_state(|s| {
        *s.rows_by_job.entry(job.to_string()).or_insert(0) = rows;
        let ms = s.started.elapsed().as_millis() as u64;
        if ms - s.last_progress_ms < s.progress_every_ms { return None; }
        s.last_progress_ms = ms;
        let total: i64 = s.rows_by_job.values().sum();
        let secs = (ms as f64 / 1000.0).max(0.001);
        let mut parts: Vec<String> = s.rows_by_job.iter().map(|(k, v)| format!("{k}={v}")).collect();
        parts.sort();
        Some(format!("progress {:.0}s: {total} rows total ({:.0} rows/s); {}", secs, total as f64 / secs, parts.join(" ")))
    }).flatten() else { return };
    eprintln!("{out}");
    note(&out);
}

pub fn summary_add(e: SummaryEntry) { with_state(|s| s.entries.push(e)); }

/// Statements recorded in plan mode, in order.
pub fn planned_statements() -> Vec<String> { with_state(|s| s.statements.clone()).unwrap_or_default() }

/// Write the summary JSON (if requested) and close the log. Returns the error only for the summary file.
pub fn finish(ok: bool) -> anyhow::Result<()> {
    let (path, body) = match with_state(|s| {
        let secs = s.started.elapsed().as_secs_f64();
        let body = serde_json::json!({
            "ok": ok, "seconds": secs, "started": now_iso(), "plan_only": planning(),
            "tables": s.entries, "rows": s.entries.iter().map(|e| e.rows).sum::<i64>(),
            "log": s.log_path.as_ref().map(|p| p.display().to_string()),
        });
        (s.summary_path.clone(), body)
    }) { Some(x) => x, None => return Ok(()) };
    note(&format!("finished ok={ok}"));
    if let Some(p) = path {
        std::fs::write(&p, serde_json::to_string_pretty(&body)?).map_err(|e| Failure::Run(format!("writing summary {}: {e}", p.display())))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes() {
        let g: anyhow::Error = Failure::Guard("x".into()).into();
        assert_eq!(exit_code(&g), 3);
        let c: anyhow::Error = anyhow::anyhow!(Failure::Check("x".into())).context("outer");
        assert_eq!(exit_code(&c), 1);
        assert_eq!(exit_code(&anyhow::anyhow!("boom")), 2);
    }

    #[test]
    fn log_and_summary_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("run.log");
        let sum = dir.path().join("run.json");
        init(Some(&log), Some(&sum), false, 1);
        stmt("target", "t1", "CREATE TABLE x (a int)", 0, Instant::now());
        summary_add(SummaryEntry { table: "dbo.x".into(), job: "t1".into(), rows: 5, seconds: 0.1, strategy: "serial".into(), checks: vec![], ok: true });
        finish(true).unwrap();
        let text = std::fs::read_to_string(&log).unwrap();
        assert!(text.contains("STMT") && text.contains("CREATE TABLE x"));
        let j: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&sum).unwrap()).unwrap();
        assert_eq!(j["rows"], 5);
        assert_eq!(j["tables"][0]["table"], "dbo.x");
    }
}
