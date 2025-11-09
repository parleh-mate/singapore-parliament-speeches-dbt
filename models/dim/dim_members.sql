{{ config(materialized="table") }}

-- sources
with
    attendance as (
        select date, member_name, is_present
        from {{ ref("stg_attendance") }}
        where member_name is not null
    ),

    gender as (
        select mp_name as member_name, gender from {{ ref("stg_gsheet_member_gender") }}
    ),

    birth_year as (
        select member_name, member_birth_year
        from {{ ref("stg_members_year_of_birth") }}
    ),

    ethnicity as (
        select member_name, member_ethnicity
        from {{ ref("stg_gsheet_member_ethnicity") }}
    ),

    party as (
        select
            member_name,
            array_agg(
                struct(party, parliament)
            ) as party
        from {{ ref("stg_gsheet_member_party") }}
        group by all
    ),

    full_name as (
        select member_name, member_name_website from {{ ref("stg_members_full_name") }}
    ),

    image as (
        select member_name, member_image_link from {{ ref("stg_members_image") }}
    ),

    constituency as (
        select
            member_name,
            array_agg(
                struct(member_position, effective_from_date, effective_to_date)
                order by effective_from_date
            ) as member_constituencies
        from {{ ref("dim_prep_member_positions") }}
        where type = 'constituency'
        group by all
    ),

    latest_constituency as (
        select member_name, member_position
        from {{ ref("dim_prep_member_positions") }}
        where type = 'constituency' and is_latest_position = true
    ),

    appointments as (
        select
            member_name,
            array_agg(
                struct(member_position, effective_from_date, effective_to_date)
                order by effective_from_date
            ) as member_appointments
        from {{ ref("dim_prep_member_positions") }}
        where type = 'appointment'
        group by member_name
    ),

    latest_appointments as (
        select member_name, array_agg(member_position) as member_positions
        from {{ ref("dim_prep_member_positions") }}
        where type = 'appointment' and is_latest_position = true
        group by all
    ),

    select_committees as (
        select
            member_name,
            array_agg(
                struct(member_position, effective_from_date, effective_to_date)
                order by effective_from_date
            ) as member_committees
        from {{ ref("dim_prep_member_positions") }}
        where type = 'select_committee'
        group by member_name
    ),

    latest_select_committees as (
        select member_name, array_agg(member_position) as member_positions
        from {{ ref("dim_prep_member_positions") }}
        where type = 'select_committee' and is_latest_position = true
        group by all
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
            gender.gender,
            party.party
            constituency.member_constituencies,
            latest_constituency.member_position as latest_member_constituency,
            appointments.member_appointments,
            latest_appointments.member_positions as latest_member_appointments,
            select_committees.member_committees,
            latest_select_committees.member_positions as latest_member_committees,
            agg.earliest_sitting,
            agg.latest_sitting,
            agg.count_sittings_present,
            agg.count_sittings_total,
            full_name.member_name_website,
            image.member_image_link
        from agg_attendance as agg
        left join gender on agg.member_name = gender.member_name
        left join party on agg.member_name = party.member_name
        left join birth_year on agg.member_name = birth_year.member_name
        left join ethnicity on agg.member_name = ethnicity.member_name
        left join full_name on agg.member_name = full_name.member_name
        left join image on agg.member_name = image.member_name
        left join constituency on agg.member_name = constituency.member_name
        left join
            latest_constituency on agg.member_name = latest_constituency.member_name
        left join appointments on agg.member_name = appointments.member_name
        left join
            latest_appointments on agg.member_name = latest_appointments.member_name
        left join select_committees on agg.member_name = select_committees.member_name
        left join
            latest_select_committees
            on agg.member_name = latest_select_committees.member_name
    )

select *
from joined
