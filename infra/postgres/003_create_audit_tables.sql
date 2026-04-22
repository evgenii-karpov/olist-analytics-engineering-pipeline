-- Audit tables for local PostgreSQL load and dbt observability.

create table if not exists audit.load_runs (
    load_run_id varchar(128) not null,
    batch_id varchar(128) not null,
    entity_name varchar(128) not null,
    source_uri varchar(1024),
    target_table varchar(256) not null,
    status varchar(32) not null,
    rows_loaded bigint,
    started_at timestamp not null,
    finished_at timestamp,
    error_message varchar(65535)
);

create table if not exists audit.dbt_runs (
    dbt_run_id varchar(128) not null,
    batch_id varchar(128) not null,
    command varchar(1024) not null,
    status varchar(32) not null,
    started_at timestamp not null,
    finished_at timestamp,
    error_message varchar(65535)
);
