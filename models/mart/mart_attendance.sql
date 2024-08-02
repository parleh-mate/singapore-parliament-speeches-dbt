{{ config(materialized="table") }}

-- sources
with
    attendance as (
        select date, member_name, is_present from {{ ref("fact_attendance") }}
    ),

    sittings as (select date, parliament, session from {{ ref("fact_sittings") }}),

    members as (
        select member_name, party, gender, ethnicity from {{ ref("dim_members") }}
    ),

    member_constituency as (
        select
            member_name,
            member_position as constituency,
            effective_from_date,
            coalesce(effective_to_date, current_date()) as effective_to_date
        from {{ ref("fact_member_positions") }}
        where type = 'constituency'
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
            members.member_ethnicity as member_ethnicity,
            member_constituency.constituency as member_constituency,

            -- attendance information
            attendance.is_present

        from attendance
        left join sittings on attendance.date = sittings.date
        left join members on attendance.member_name = members.member_name
        left join
            member_constituency
            on attendance.member_name = member_constituency.member_name
            and attendance.date
            between member_constituency.effective_from_date
            and member_constituency.effective_to_date
    )

select *
from joined
