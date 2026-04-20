# Redshift Bootstrap

Run these files in order:

```text
001_create_schemas.sql
002_create_raw_tables.sql
003_create_audit_tables.sql
004_copy_raw_tables_template.sql
005_create_correction_tables.sql
```

The raw table DDL is generated from:

```text
docs/source_profile.json
```

Regenerate DDL after changing the source contract:

```powershell
python scripts/utilities/generate_redshift_raw_ddl.py
```

In the Codex Desktop bundled runtime, use:

```powershell
C:\Users\fyujv\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe scripts\utilities\generate_redshift_raw_ddl.py
```
