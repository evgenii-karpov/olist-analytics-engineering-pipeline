-- Schemas for the local PostgreSQL warehouse.

create schema if not exists raw_data;
create schema if not exists staging;
create schema if not exists intermediate;
create schema if not exists snapshots;
create schema if not exists core;
create schema if not exists marts;
create schema if not exists audit;
