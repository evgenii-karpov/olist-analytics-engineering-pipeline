-- Create warehouse schemas for the Olist Modern Data Stack project.
-- Run as a Redshift user with schema creation privileges.

create schema if not exists raw_data;
create schema if not exists staging;
create schema if not exists intermediate;
create schema if not exists snapshots;
create schema if not exists core;
create schema if not exists marts;
create schema if not exists audit;
