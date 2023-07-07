with jobs as (
    select *
    from {{ ref('stg_raw_data__jobs') }}
    {% if is_incremental() %}
     where created_at >= (SELECT MAX(created_at) FROM {{ this }})
        or updated_at >= (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
)
select * from jobs