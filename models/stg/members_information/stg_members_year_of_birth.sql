with
    source as (select * from {{ source("raw", "members_year_of_birth") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("year_of_birth") }} as member_birth_year,

        from source
    )
select *
from renamed
