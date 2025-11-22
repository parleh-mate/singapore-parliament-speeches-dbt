with
    source as (select * from {{ source("google_sheets", "member_name_link") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("member_link_name") }} as member_name_website,
            cast(accessed_at as date) as accessed_at
        from source
    )
select *
from renamed
