with
    source as (select * from {{ source("raw", "members_select_committee") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("parliament") }} as parliament,
            {{ adapter.quote("session") }} as session,
            {{ adapter.quote("committee") }} as member_committee,
            {{ adapter.quote("position") }} as member_committee_position

        from source
    ),
    dedup as (
        select
            member_name,
            parliament,
            session,
            member_committee,
            member_committee_position
        from renamed
        qualify
            row_number() over (
                partition by
                    member_name,
                    parliament,
                    session,
                    member_committee,
                    member_committee_position
            )
            = 1
    ),
    array_agg as (
        select
            member_name,
            parliament,
            session,
            member_committee,
            array_agg(member_committee_position) as member_committee_position
        from dedup
        group by all
    )
select *
from array_agg
