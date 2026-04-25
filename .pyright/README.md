# Editor Typings

This folder holds local-only type stubs for VS Code and Pylance.

The project runs Airflow inside Docker, but the local Windows virtual
environment does not include Airflow's runtime dependencies. The stubs under
`/.pyright/typings/` let VS Code resolve Airflow imports in DAG files without
affecting the actual container runtime.

`pyrightconfig.json` points Pyright/Pylance at `/.pyright/typings` so these
stubs stay out of the repository root while still being available to the
editor.
