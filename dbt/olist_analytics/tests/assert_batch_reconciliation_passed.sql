with reconciliation as (
    select status
    from audit.batch_reconciliation
    where batch_id = '{{ var("batch_date") }}'
),

summary as (
    select
        count(*) as reconciliation_rows,
        sum(case when status = 'PASS' then 1 else 0 end) as passed_rows
    from reconciliation
)

select *
from summary
where
    reconciliation_rows = 0
    or passed_rows <> reconciliation_rows
