# dbt Docs

This directory contains the committed static dbt documentation site.

Regenerate it from the repository root with:

```powershell
Set-Location dbt\olist_analytics
uv run dbt docs generate --static --target-path ..\..\docs\dbt --vars '{batch_date: "2018-09-01", lookback_days: 3}'
Set-Location ..\..
```

Open `static_index.html` in a browser to inspect the docs without running a
dbt docs server.
