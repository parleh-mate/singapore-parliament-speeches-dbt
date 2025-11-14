{{ config(materialized="table") }}

-- sources
with
    attendance as (
        select date, member_name, is_present from {{ ref("fact_attendance") }}
    ),

    sittings as (select date, parliament, session from {{ ref("fact_sittings") }}),

    member_demographics as (
        select member_name, gender, member_ethnicity
        from {{ ref("dim_members") }}
    ),

    member_party as (
        select member_name, party, parliament
        from {{ ref("stg_gsheet_member_party") }}
    ),

    member_constituency as (
        select
            member_name,
            member_position as constituency,
            effective_from_date,
            coalesce(effective_to_date, current_date()) as effective_to_date
        from {{ ref("dim_prep_member_positions") }}
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
            demo.gender as member_gender,
            demo.member_ethnicity as member_ethnicity,
            member_constituency.constituency as member_constituency,
            party.party,

            -- attendance information
            attendance.is_present

        from attendance
        left join sittings on attendance.date = sittings.date
        left join member_demographics as demo on attendance.member_name = demo.member_name
        left join member_party as party on attendance.member_name = party.member_name
            and sittings.parliament = party.parliament
        left join
            member_constituency
            on attendance.member_name = member_constituency.member_name
            and attendance.date
            between member_constituency.effective_from_date
            and member_constituency.effective_to_date
    )

select *
from joined
