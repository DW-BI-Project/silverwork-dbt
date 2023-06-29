with projects as (
    select * from {{ ref('stg_raw_data__projects') }}
), extract as (
    select projstartdd
        , EXTRACT(YEAR FROM projstartdd::date) as year
        , EXTRACT(MONTH FROM projstartdd::date) as month
    FROM raw_data.projects
),
sum as (
SELECT year
    , month
    , projstartdd
    , SUM(COUNT(1)) OVER (PARTITION BY year) AS year_cnt
    , SUM(COUNT(1)) OVER (PARTITION BY left(projstartdd,7)) AS month_cnt
FROM
    extract
GROUP BY 1, 2, 3
),
avg as (
    select year, month, ROUND((month_cnt * 100.0 / year_cnt), 2) AS percent
    from sum
    group by 1,2,3
    order by 1,2
)
select * from avg
