with projects as (
    select * from {{ ref('stg_raw_data__projects') }}
),

filter as(
      select *
           , year - continuous_project_start_year AS duration_years
        from projects
       where continuous_project_start_year IS NOT NULL
         and start_date - end_date >= 0
)
select * from filter