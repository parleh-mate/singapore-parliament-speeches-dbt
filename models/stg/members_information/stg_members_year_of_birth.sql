with
    source as (select * from {{ source("raw", "members_year_of_birth") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            cast({{ adapter.quote("year_of_birth") }} as int) as member_birth_year,
            cast(accessed_at as date) as accessed_at
        from source
    ),
    -- union manually-filled information
    manual_gsheet as (
        select member_name, 
        member_birth_year,
        cast(accessed_at as date) as accessed_at 
        from {{ ref("stg_gsheet_year_of_birth") }}
    ),

    unioned as (
        select member_name, member_birth_year, accessed_at
        from renamed
        union all
        select member_name, member_birth_year, accessed_at
        from manual_gsheet
    ),

    -- filter latest non null entries --
    filter_latest as (
        select member_name, member_birth_year
        from unioned
        where member_birth_year is not null
        qualify row_number() over(
            partition by member_name order by accessed_at desc
        ) = 1
    )
select *
from filter_latest
