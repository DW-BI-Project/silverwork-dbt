with jobs as (
    select * from {{ ref('stg_raw_data__jobs') }}
),

annual_count as(
    select EXTRACT(YEAR FROM start_date) as year
         , employment_shape
         , count(1) as count
      from jobs
  group by 1 , 2
)
select * from annual_count