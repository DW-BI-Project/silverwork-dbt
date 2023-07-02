with jobs as (
    select etc_item as prefer from {{ ref('stg_raw_data__jobs') }}
    where etc_item is not null

)
select * from jobs