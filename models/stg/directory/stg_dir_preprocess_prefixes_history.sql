with
    source as (
        select * from {{ source("sg_govt_dir_preprocess", "prefixes_history") }}
    ),
    renamed as (
        select
            {{ adapter.quote("extracted_name") }},
            {{ adapter.quote("prefix") }},
            -- first run had some values in 2024-05-06, should be grouped as part of
            -- 2024-05-07
            if(
                cast({{ adapter.quote("effective_from") }} as date) = '2024-05-06',
                '2024-05-07',
                cast({{ adapter.quote("effective_from") }} as date)
            ) as effective_from,
            if(
                cast({{ adapter.quote("effective_to") }} as date) = '2024-05-06',
                '2024-05-07',
                cast({{ adapter.quote("effective_to") }} as date)
            ) as effective_to

        from source
    ),
    dedup as (
        select *
        from renamed
        qualify
            row_number() over (
                partition by extracted_name, effective_from, effective_to
                order by length(prefix) desc
            )
            = 1
    ),

    latest_flag as (
        select
            *,
            effective_to
            = max(effective_to) over (partition by extracted_name, prefix) as is_latest
        from dedup
    )
select *
from latest_flag
