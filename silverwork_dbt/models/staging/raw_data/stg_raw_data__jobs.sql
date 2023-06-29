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

        -- numerics
        clltprnnum::int as worker_number,

        -- booleans
        case
            when ageyn = 'Y' then true
            else false
        end as has_age_limit,

        -- dates
        startdd::date as start_date,
        enddd::date as end_date,

        -- timestamps
        createdt::timestamp as created_at,
        upddt::timestamp as updated_at

    from source
),

added as (
    select *
        , EXTRACT(YEAR FROM start_date)::string as start_date_year
        , EXTRACT(MONTH FROM start_date)::string as start_date_month
    from renamed
)

select * from added
