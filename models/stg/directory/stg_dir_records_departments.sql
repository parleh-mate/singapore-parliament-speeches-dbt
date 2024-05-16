with
    source as (select * from {{ source("sg_govt_dir", "departments") }}),
    renamed as (
        select
            {{ adapter.quote("parent_name") }},
            {{ adapter.quote("department_name") }},
            {{ adapter.quote("department_link") }} as department_url,
            {{ adapter.quote("ministry_name") }},
            {{ adapter.quote("_accessed_at") }}

        from source
    ),
    latest_flag as (
        select
            *,
            _accessed_at
            = max(_accessed_at) over (partition by ministry_name) as is_latest
        from renamed
    )
select *
from latest_flag
