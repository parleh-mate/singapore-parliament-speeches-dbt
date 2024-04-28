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
    ),
    -- include dates
    max_date as (select max(date) as latest_date from {{ ref("stg_sittings") }}),

    parliament_sesion_dates as (
        select
            parliament,
            session,
            case
                when parliament = 12 and session = 1 then '2010-10-10' else min(date)  -- hardcode dates
            end as effective_from_date,
            case
                when max(date) = (select latest_date from max_date)
                then null
                else max(date)
            end as effective_to_date
        from {{ ref("stg_sittings") }}
        group by all
    ),
    -- join dates
    join_dates as (
        select
            array_agg.member_name,
            array_agg.parliament,
            array_agg.session,
            array_agg.member_committee,
            array_agg.member_committee_position,
            parliament_sesion_dates.effective_from_date,
            parliament_sesion_dates.effective_to_date
        from array_agg
        left join
            parliament_sesion_dates
            on cast(array_agg.parliament as int) = parliament_sesion_dates.parliament
            and cast(array_agg.session as int) = parliament_sesion_dates.session
    )
select *
from join_dates
