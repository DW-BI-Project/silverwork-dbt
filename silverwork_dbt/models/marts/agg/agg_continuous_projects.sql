with projects as (
    select * from {{ ref('stg_raw_data__projects') }}
    where continuous_project_start_year IS NOT NULL
      and continuous_project_start_year not in ('nan' , 'None')
      and end_date >= start_date
),

filter as(
    {% if target.type == 'snowflake' %}
      select *
           , year - continuous_project_start_year AS duration_years
        from projects
    {% else %}
      select *
           , CAST(year AS int) - CAST(continuous_project_start_year AS int) AS duration_years
        from projects
    {% endif %}
)
select * from filter