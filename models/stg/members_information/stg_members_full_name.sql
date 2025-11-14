with
    source as (select * from {{ source("raw", "members_full_name") }}),
    renamed as (
        select
            {{ adapter.quote("original") }} as member_name,
            {{ adapter.quote("converted") }} as member_name_website

        from source
    ),
    gsheet as (
        select * from {{ ref("stg_gsheet_member_name_link") }}
    ),

    unioned as (
        select *, 
        -- set accessed at as last modified date
        cast("2024-05-16" as date) as accessed_at
        from renamed
        union all
        select *
        from gsheet
    ),

    -- filter latest non null entries --
    filter_latest as (
        select member_name, member_name_website
        from unioned
        where member_name_website is not null
        qualify row_number() over(
            partition by member_name order by accessed_at desc
        ) = 1
    )
select *
from filter_latest
