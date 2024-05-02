with
    source as (select * from {{ source("google_sheets", "member_of_parliament") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("constituency") }} as member_constituency,
            cast({{ adapter.quote("from_date") }} as date) as effective_from_date,
            cast({{ adapter.quote("to_date") }} as date) as effective_to_date,
            cast({{ adapter.quote("accessed_at") }} as date) as accessed_at

        from source
    )
select *
from renamed
