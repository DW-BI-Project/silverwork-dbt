-- 사업정보 데이터는 연단위로 들어온다
with projects as (
    select * from {{ ref('stg_raw_data__projects') }}
    {% if is_incremental() %}
     where project_year >= (SELECT MAX(project_year) FROM {{ this }})
    {% endif %}
)
select * from projects