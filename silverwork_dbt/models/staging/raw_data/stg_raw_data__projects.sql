with source as (
    select * from {{ source('raw', 'projects') }}
),

filterd as (
    select * from source
    where admprovnm is not null
),

renamed as (
    select

        -- ids
        projno as project_id,

        -- strings
        projtype as project_type,
        projyear as project_year,
        contprojstartyear as  continuous_project_start_year,

        projtypenm as project_type_name,
        projnm as project_name,

        admprovnm as administrative_province_name,
        admdistnm as administrative_district_name,
        planstatuscd as plan_status,

        -- numerics
        projplanchangeno::int as project_plan_changed_number,
        targetemployment::int as target_employment,

        -- booleans
        CASE
            WHEN contprojyn = 'Y' THEN true
            ELSE false
        END as is_continuous_project,
        case
            when nonbudgyn = 'Y' then true
            else false
        end as is_non_bugdet,
        case
            when delyn = 'Y' then true
            else false
        end as is_deleted,

        -- dates
        projstartdd::date as project_start_date,
        projenddd::date as project_end_date

        -- timestamps

    from filterd
),

added as (
    select *
        , EXTRACT(YEAR FROM project_start_date)::string as project_start_year
        , EXTRACT(MONTH FROM project_start_date)::string as project_start_month
        , CASE administrative_province_name
             WHEN '서울특별시' THEN 'KR-11'
             WHEN '부산광역시' THEN 'KR-26'
             WHEN '대구광역시' THEN 'KR-27'
             WHEN '인천광역시' THEN 'KR-28'
             WHEN '광주광역시' THEN 'KR-29'
             WHEN '대전광역시' THEN 'KR-30'
             WHEN '울산광역시' THEN 'KR-31'
             WHEN '세종특별자치시' THEN 'KR-50'
             WHEN '경기도' THEN 'KR-41'
             WHEN '강원도' THEN 'KR-42'
             WHEN '충청북도' THEN 'KR-43'
             WHEN '충청남도' THEN 'KR-44'
             WHEN '전라북도' THEN 'KR-45'
             WHEN '전라남도' THEN 'KR-46'
             WHEN '경상북도' THEN 'KR-47'
             WHEN '경상남도' THEN 'KR-48'
             WHEN '제주특별자치도' THEN 'KR-49'
          END as administrative_province_iso
    from renamed
)
select * from added