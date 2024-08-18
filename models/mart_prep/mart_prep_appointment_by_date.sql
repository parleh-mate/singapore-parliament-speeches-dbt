{{ config(materialized="table") }}

with
    sittings as (select date from {{ ref("fact_sittings") }}),

    appointments as (
        select
            member_name,
            member_position as appointment,
            effective_from_date,
            coalesce(date(effective_to_date), current_date()) as effective_to_date
        from {{ ref("dim_prep_member_positions") }}
        where type = 'appointment'
    ),

    appointment_by_date as (
        select
            sittings.date,
            appointments.member_name,
            appointments.appointment,
            appointments.effective_from_date,
            appointments.effective_to_date
        from sittings
        left join
            appointments
            on sittings.date
            between date(effective_from_date) and date(effective_to_date)
        where member_name is not null
    ),

    agg_member_date as (
        select
            date,
            member_name,
            array_agg(appointment) as appointments,
            count(appointment) as count_concurrent_appointments
        from appointment_by_date
        group by all
    )

select *
from agg_member_date
