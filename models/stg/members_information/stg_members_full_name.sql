with
    source as (select * from {{ source("raw", "members_full_name") }}),
    renamed as (
        select
            {{ adapter.quote("original") }} as member_name,
            {{ adapter.quote("converted") }} as member_name_website

        from source
    )
select *
from renamed
