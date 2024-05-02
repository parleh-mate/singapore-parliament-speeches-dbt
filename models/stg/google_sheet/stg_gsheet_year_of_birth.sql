with
    source as (select * from {{ source("google_sheets", "year_of_birth") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            cast({{ adapter.quote("year_of_birth") }} as int) as member_birth_year,
            cast({{ adapter.quote("accessed_at") }} as date) as accessed_at

        from source
    )
select *
from renamed
