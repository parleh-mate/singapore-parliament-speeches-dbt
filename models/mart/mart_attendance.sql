{{ config(
    materialized='table'
) }}

-- sources

with attendance as (
    select
        date,
        member_name,
        is_present
    from {{ ref('fact_attendance') }}
),

sittings as (
    select
        date,
        parliament,
        session
    from {{ ref('fact_sittings') }}
),

members as (
    select
        member_name,
        party,
        gender
    from {{ ref('dim_members') }}
),

joined as (
    select
        -- metadata
        attendance.date,
        sittings.parliament,
        sittings.session,

        -- member information
        attendance.member_name,
        members.party as member_party,
        members.gender as member_gender,

        -- attendance information
        attendance.is_present

    from attendance
    left join sittings
        on attendance.date = sittings.date
    left join members
        on attendance.member_name = members.member_name
)

select *
from joined
