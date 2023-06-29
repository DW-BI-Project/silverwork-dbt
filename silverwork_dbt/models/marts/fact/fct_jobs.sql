with jobs as (
    select * from {{ ref('stg_raw_data__jobs') }}
)
select * from jobs