with source as (
    select * from {{ source('raw', 'jobs') }}
),

renamed as (
    select

        -- ids
        jobid as job_id,

        -- strings
        acptmthd as accept_method,
        deadline,
        jobclsnm as job_class_name,
        orannm as organization_name,
        recrttitle as recruitment_title,
        age,
        detcnts as detail_contents,
        etcitm as etc_item,
        workplc as work_place,
        pldetaddr as place_detail_address,
        plbiznm as place_business_name,

        CASE
            WHEN emplymshp = 'CM0101' THEN '정규직'
            WHEN emplymshp = 'CM0102' THEN '계약직'
            WHEN emplymshp = 'CM0103' THEN '시간제일자리'
            WHEN emplymshp = 'CM0104' THEN '일당직'
            WHEN emplymshp = 'CM0105' THEN '기타'
            ELSE '기타' END as employment_shape
        ,

        -- booleans
        case
            when ageyn = 'Y' then true
            else false
        end as has_age_limit,


        -- numerics / dates / timestamps
        {% if target.type == 'snowflake' %}

        clltprnnum::int as worker_number,

        startdd::date as start_date,
        enddd::date as end_date,

        case
            when createDt != 'nan' then createdt::timestamp
            else createdy::timestamp
        end AS created_at,

        case
            when upddt != 'nan' then upddt::timestamp
            else null
        end AS updated_at

        {% else %}

        CAST(clltprnnum AS INT64) AS worker_number,

        PARSE_DATE('%Y-%m-%d', startdd) AS start_date,
        PARSE_DATE('%Y-%m-%d', enddd) AS end_date,

        case
            when createDt != 'nan' then PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createDt)
            else PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', createDy)
        end AS created_at,

        case
            when upddt != 'nan' then PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', upddt)
            else null
        end AS updated_at

        {% endif %}

    from source
),

added as (
    select *

        {% if target.type == 'snowflake' %}
        , EXTRACT(YEAR FROM start_date)::string as year
        , EXTRACT(MONTH FROM start_date)::string as month

        {% else %}
        , CAST(EXTRACT(YEAR FROM start_date) AS STRING) AS year
        , CAST(EXTRACT(MONTH FROM start_date) AS STRING) AS month
        {% endif %}

    from renamed
)

select * from added
