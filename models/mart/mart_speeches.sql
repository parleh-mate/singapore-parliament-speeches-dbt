{{ config(materialized="table") }}

-- sources
with
    speeches as (
        select
            speech_id,
            date,
            topic_id,
            member_name,
            text,
            count_words,
            count_characters,
            count_sentences,
            count_syllables
        from {{ ref("fact_speeches") }}
    ),

    topics as (
        select topic_id, title, section_type, section_type_name, is_constitutional
        from {{ ref("dim_topics") }}
    ),

    sittings as (
        select date, parliament, session, volume, sittings
        from {{ ref("fact_sittings") }}
    ),

    members as (select member_name, party, gender from {{ ref("dim_members") }}),

    constituencies as (
        select
            member_name,
            member_position as constituency,
            date(effective_from_date) as effective_from_date,
            coalesce(date(effective_to_date), current_date()) as effective_to_date
        from {{ ref("fact_member_positions") }}
        where type = 'constituency'
    ),

    appointments as (
        select date, member_name, appointments, count_concurrent_appointments
        from {{ ref("mart_prep_appointment_by_date") }}
    ),

    joined as (
        select
            -- metadata
            speeches.date,
            speeches.speech_id,
            speeches.topic_id,
            sittings.parliament,
            sittings.session,
            sittings.volume,
            sittings.sittings,

            -- member information
            speeches.member_name,
            members.party as member_party,
            members.gender as member_gender,
            case
                when members.party = 'NMP'
                then 'Nominated Member of Parliament'
                else constituencies.constituency
            end as member_constituency,
            appointments.appointments as member_appointments,
            appointments.count_concurrent_appointments
            as member_count_concurrent_appointments,

            -- topic information
            topics.title as topic_title,
            topics.section_type as topic_type,
            topics.section_type_name as topic_type_name,
            topics.is_constitutional as is_constitutional,

            -- speech information
            speeches.text as speech_text,
            speeches.count_words as count_speeches_words,
            speeches.count_characters as count_speeches_characters,
            speeches.count_sentences as count_speeches_sentences,
            speeches.count_syllables as count_speeches_syllables

        from speeches
        left join topics on speeches.topic_id = topics.topic_id
        left join sittings on speeches.date = sittings.date
        left join members on speeches.member_name = members.member_name
        left join
            constituencies
            on speeches.member_name = constituencies.member_name
            and speeches.date
            between constituencies.effective_from_date
            and constituencies.effective_to_date
        left join
            appointments
            on speeches.member_name = appointments.member_name
            and speeches.date = appointments.date
    )

select *
from joined
