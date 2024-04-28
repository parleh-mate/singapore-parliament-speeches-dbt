with
    source as (select * from {{ source("raw", "members_image") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("image_link") }} as member_image_link

        from source
    )
select *
from renamed
