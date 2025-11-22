with
    source as (select * from {{ source("raw", "members_image") }}),
    gsheet as (select * from {{ ref("stg_gsheet_member_image_link") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("image_link") }} as member_image_link,
            cast(accessed_at as date) as accessed_at
        from source
    ),
    unioned as (
        select member_name, member_image_link, accessed_at
        from renamed
        union all
        select member_name, member_image_link, accessed_at
        from gsheet
    ),
     -- filter latest entries; nulls not dropped because there are nulls in manual gsheet --
    filter_latest as (
        select member_name, member_image_link
        from unioned
        qualify row_number() over(
            partition by member_name order by accessed_at desc
        ) = 1
    )
select *
from filter_latest
