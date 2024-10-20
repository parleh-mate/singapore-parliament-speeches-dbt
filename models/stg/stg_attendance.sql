{{ config(materialized="view") }}

with
    type_cast as (
        select
            cast(date as date) as date,
            cast(member_name as string) as member_name,
            cast(is_present as bool) as is_present
        from {{ source("raw", "attendance") }}
        where member_name is not null
    ),

    standardise_member_name as (
        select
            * except (member_name),
            -- mapping of names here:
            -- https://docs.google.com/spreadsheets/d/1Wk_PDlQbWWViTV9NmDPsXwu54TD96ltEycLAKOHe1fA/edit#gid=1498942254
            coalesce(mapping.member_name, type_cast.member_name) as member_name
        from type_cast
        left join
            {{ source("google_sheets", "member_name_mapping") }} as mapping
            on mapping.possible_variation = type_cast.member_name
    ),

    filter_empty_member_name as (
        select * from standardise_member_name where member_name != ''
    ),

    filter_non_present_members as (
        select *
        from filter_empty_member_name
        where
            not (
                -- 13th parliament
                (date between '2016-01-15' and '2020-06-23')
                -- lee li lian was offered NCMP but did not take it up
                -- parliament records her as an absent MP
                and lower(member_name) = 'lee li lian'
            )
    )

select *
from filter_non_present_members
