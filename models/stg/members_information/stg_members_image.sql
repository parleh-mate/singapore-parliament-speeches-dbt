with
    source as (select * from {{ source("raw", "members_image") }}),
    gsheet as (select * from {{ ref("stg_gsheet_member_image_link") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("image_link") }} as member_image_link
        from source
    ),
    unioned as (
        select member_name, member_image_link
        from renamed
        union all
        select member_name, member_image_link
        from gsheet
    )
select *
from unioned
