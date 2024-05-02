with
    source as (select * from {{ source("google_sheets", "office_holding") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("position") }} as member_appointment,
            cast({{ adapter.quote("from_date") }} as date) as effective_from_date,
            cast({{ adapter.quote("to_date") }} as date) as effective_to_date,
            cast({{ adapter.quote("accessed_at") }} as date) as accessed_at

        from source
    )
select *
from renamed
