with projects as (
        select project_start_year as year
            , project_start_month as month
        from {{ ref('stg_raw_data__projects') }}
),

sum as (
SELECT year
    , month
    , SUM(COUNT(1)) OVER (PARTITION BY year) AS year_cnt
    , SUM(COUNT(1)) OVER (PARTITION BY year||'-'||month) AS month_cnt
FROM
    projects
GROUP BY 1, 2
),
avg as (
    select year, month, ROUND((month_cnt * 100.0 / year_cnt), 2) AS percent
    from sum
    group by 1,2,3
    order by 1,2
)
select * from avg
