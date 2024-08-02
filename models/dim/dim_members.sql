{{ config(materialized="table") }}

-- sources
with
    attendance as (
        select date, member_name, is_present
        from {{ ref("stg_attendance") }}
        where member_name is not null
    ),

    seed_member as (
        select mp_name as member_name, party, gender from {{ ref("member") }}
    ),

    birth_year as (
        select member_name, member_birth_year
        from {{ ref("stg_members_year_of_birth") }}
    ),

    ethnicity as (
        select member_name, member_ethnicity
        from {{ ref("stg_gsheet_member_ethnicity") }}
    ),

    full_name as (
        select member_name, member_name_website from {{ ref("stg_members_full_name") }}
    ),

    image as (
        select member_name, member_image_link from {{ ref("stg_members_image") }}
    ),

    constituency as (
        select member_name, member_constituency
        from {{ ref("stg_members_member_of_parliament") }}
        where is_latest_constituency = true
    ),

    appointments as (
        select member_name, array_agg(member_appointment) as member_appointments
        from {{ ref("stg_members_office_holding") }}
        where is_latest_appointment = true
        group by member_name
    ),

    select_committees as (
        select member_name, array_agg(member_committee) as member_committees
        from {{ ref("stg_members_select_committee") }}
        where is_latest_committee = true
        group by member_name
    ),

    -- transform
    agg_attendance as (
        select
            member_name,
            min(date) as earliest_sitting,
            max(date) as latest_sitting,
            count(distinct if(is_present, date, null)) as count_sittings_present,
            count(distinct date) as count_sittings_total
        from attendance
        group by member_name
    ),

    -- join
    joined as (
        select
            agg.member_name,
            birth_year.member_birth_year,
            ethnicity.member_ethnicity,
            seed.party,
            seed.gender,
            constituency.member_constituency as latest_member_constituency,
            appointments.member_appointments as latest_member_appointments,
            select_committees.member_committees as latest_member_committees,
            agg.earliest_sitting,
            agg.latest_sitting,
            agg.count_sittings_present,
            agg.count_sittings_total,
            full_name.member_name_website,
            image.member_image_link
        from agg_attendance as agg
        left join seed_member as seed on agg.member_name = seed.member_name
        left join birth_year on agg.member_name = birth_year.member_name
        left join ethnicity on agg.member_name = ethnicity.member_name
        left join full_name on agg.member_name = full_name.member_name
        left join image on agg.member_name = image.member_name
        left join constituency on agg.member_name = constituency.member_name
        left join appointments on agg.member_name = appointments.member_name
        left join select_committees on agg.member_name = select_committees.member_name
    )

select *
from joined
