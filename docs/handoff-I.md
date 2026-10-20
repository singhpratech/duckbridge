# Handoff: stream I (string type in create mode)

## Changed (src/core/typemap.rs, src/core/catalog.rs only)
- `typemap::StringType { Varchar, Nvarchar (default), Auto }` with `StringType::parse(&str) -> Result`.
- `typemap::map_column_with(col, is_key, mode, target, string_type, is_ascii, pinned)`; `map_column` keeps its signature and delegates with `Nvarchar, None, None`.
  - pinned: type text used verbatim, Conv::Text, warning "type pinned by --string-sizes file".
  - Varchar: `VARCHAR(n)` bucketed like today, `VARCHAR(MAX)` in Max mode / above 4000; keys capped at 900 (warning + `VARCHAR(900)` beyond).
  - Auto: VARCHAR if `is_ascii == Some(true)` else NVARCHAR; pushes "auto string type: varchar (all values ASCII)" or "auto string type: nvarchar (non-ASCII values present)" into `col.warnings` (shown by inspect). No warning when not Auto.
- `catalog::StringSizes { FromData (default), Max, File(HashMap) }`, `catalog::load_string_sizes_file(path)` (CSV `table,column,type`, optional header, `#` comments; table may be `schema.table` or bare; keys lowercased).
- `CatalogOptions` gained `string_type` and `string_sizes` (Default: Nvarchar, FromData). `StringSizes::Max` acts like `StringMode::Max`.
- read_catalog: with Auto it adds `bool_and(col IS NULL OR strlen(col) = length(col))` per string column to the existing single measurement query (no extra scan). The flag is kept locally (model.rs untouched). File lookups try `schema.table.column` then `table.column`.

## Integrator wiring
- BREAKING for callers: `CatalogOptions { .. }` literals in src/api.rs:247 and src/main.rs:338, 448 need the two new fields (or `..Default::default()`).
- CLI: `--string-type varchar|nvarchar|auto` -> `StringType::parse(..)?`; `--string-sizes from-data|max|<file>` -> `FromData` / `Max` / `catalog::load_string_sizes_file(path)?`. Set `string_type` and `string_sizes` on CatalogOptions (keep `string_mode`).
- Extension: same via `string_type` / `string_sizes` params (strings), same parsing.
- inspect already prints column warnings, so auto decisions show up there; use the same options in inspect and export so DDL matches.

## Open issues
- Varchar with non-ASCII data (explicit `varchar`, not auto) can lose characters depending on the target collation; max_len is a UTF-16-unit upper bound, so sizes are safe in bytes for ASCII only. No warning is emitted for this case.
- Pinned types are not validated against SQL Server syntax or data length.
- Auto and pinned apply to VARCHAR/CHAR/TEXT columns only (not ENUM, JSON, etc.).
- Tests were run against a patched scratch copy because src/api.rs and src/main.rs construct CatalogOptions literally; 10 core tests pass, no warnings in owned files.
