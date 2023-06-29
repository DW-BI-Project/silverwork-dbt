with projects as (
    select * from {{ ref('stg_raw_data__projects') }}
)
select * from projects