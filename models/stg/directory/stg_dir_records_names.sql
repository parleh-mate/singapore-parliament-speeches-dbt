with
    source as (select * from {{ source("sg_govt_dir", "names") }}),
    renamed as (
        select
            {{ adapter.quote("name") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("position") }},
            {{ adapter.quote("url") }} as department_url,
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
