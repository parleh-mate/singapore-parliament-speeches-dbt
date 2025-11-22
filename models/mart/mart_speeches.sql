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
            count_syllables,
            is_vernacular_speech,
            vernacular_speech_language,
        from {{ ref("fact_speeches") }}
    ),

    topics as (
        select
            topic_id,
            title,
            section_type,
            section_type_name,
            is_topic_constitutional,
            is_topic_procedural
        from {{ ref("dim_topics") }}
    ),

    sittings as (
        select date, parliament, session, volume, sittings
        from {{ ref("fact_sittings") }}
    ),

    member_demographics as (
        select member_name, gender, member_ethnicity
        from {{ ref("dim_members") }}
    ),

    member_party as (
        select member_name, party, parliament
        from {{ ref("stg_gsheet_member_party") }}
    ),

    constituencies as (
        select
            member_name,
            member_position as constituency,
            date(effective_from_date) as effective_from_date,
            coalesce(date(effective_to_date), current_date()) as effective_to_date
        from {{ ref("dim_prep_member_positions") }}
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
            demo.gender as member_gender,
            demo.member_ethnicity as member_ethnicity,
            party.party as member_party,
            case
                when party.party = 'NMP'
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
            topics.is_topic_constitutional as is_topic_constitutional,
            topics.is_topic_constitutional as is_topic_procedural,

            -- speech information
            speeches.text as speech_text,
            speeches.count_words as count_speeches_words,
            speeches.count_characters as count_speeches_characters,
            speeches.count_sentences as count_speeches_sentences,
            speeches.count_syllables as count_speeches_syllables,

            -- speech flags
            speeches.is_vernacular_speech,
            speeches.vernacular_speech_language,

        from speeches
        left join topics on speeches.topic_id = topics.topic_id
        left join sittings on speeches.date = sittings.date
        left join member_demographics as demo on speeches.member_name = demo.member_name
        left join member_party as party on speeches.member_name = party.member_name
            and sittings.parliament = party.parliament
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
    ),

    -- primary questions are identified by the standard text which looks like:
    -- 'asked the minister for ...'
    -- 'to ask the minister for ...'
    -- 'the following question stood in the name of'
    -- '
    primary_question as (
        select
            *,
            case
                when
                    topic_type in ('OA', 'WA', 'WANA')
                    and (
                        (
                            -- condition 1: ask
                            -- note there are a few variations because it varies
                            lower(substring(speech_text, 1, 20)) like '%asked%'
                            or lower(substring(speech_text, 1, 20)) like '%to ask%'
                            or lower(substring(speech_text, 1, 20)) like '%as ked%'
                        )
                        and (
                            -- condition 2: addressee
                            lower(substring(speech_text, 1, 100)) like '%minister%'
                            or lower(substring(speech_text, 1, 100))
                            like '%parliamentary secretary%'
                        )
                    )
                    or (
                        lower(substring(speech_text, 1, 100))
                        like '%following question stood in the name%'
                    )
                then true
                else false
            end as is_primary_question
        from joined
    ),

    -- determining which ministry this was addressed to (gets the first few words)
    extract_minister_for as (
        select
            *,
            if(
                is_primary_question = true,
                case
                    when
                        not regexp_contains(speech_text, r'Deputy Prime Minister')
                        and regexp_contains(speech_text, r'Prime Minister')
                    then 'Prime Minister'
                    else
                        regexp_extract(
                            regexp_replace(speech_text, r'[,();]+', ''),
                            r'Minister for (\S+(?: \S+){0,6})'
                        )
                end,
                null
            ) as extracted_words
        from primary_question
    ),

    -- extracted words will look something like the regexp_contains expressions
    extract_ministry as (
        select
            * except (extracted_words),
            case
                when regexp_contains(extracted_words, r'Communications and Information')
                then 'Communications and Information'
                when
                    regexp_contains(
                        extracted_words, r'Information Communications and the Arts'
                    )
                then 'Information Communications and the Arts'
                when regexp_contains(extracted_words, r'Culture Community and Youth')
                then 'Culture Community and Youth'
                when
                    regexp_contains(
                        extracted_words, r'Community Development Youth and Sports'
                    )
                then 'Community Development Youth and Sports'
                when regexp_contains(extracted_words, r'Defence')
                then 'Defence'
                when regexp_contains(extracted_words, r'Education')
                then 'Education'
                when regexp_contains(extracted_words, r'Finance')
                then 'Finance'
                when regexp_contains(extracted_words, r'Foreign Affairs')
                then 'Foreign Affairs'
                when regexp_contains(extracted_words, r'Health')
                then 'Health'
                when regexp_contains(extracted_words, r'Home Affairs')
                then 'Home Affairs'
                when regexp_contains(extracted_words, r'Law')
                then 'Law'
                when regexp_contains(extracted_words, r'Manpower')
                then 'Manpower'
                when regexp_contains(extracted_words, r'National Development')
                then 'National Development'
                when regexp_contains(extracted_words, r'Social and Family Development')
                then 'Social and Family Development'
                when
                    regexp_contains(
                        extracted_words, r'Sustainability and the Environment'
                    )
                then 'Sustainability and the Environment'
                when
                    regexp_contains(extracted_words, r'Environment and Water Resources')
                then 'Environment and Water Resources'
                when regexp_contains(extracted_words, r'Trade and Industry')
                then 'Trade and Industry'
                when regexp_contains(extracted_words, r'Transport')
                then 'Transport'
                when regexp_contains(extracted_words, r'Prime Minister')
                then 'Prime Minister'
                else null
            end as ministry_addressed
        from extract_minister_for
    ),

    agg_ministry_counts_by_topic as (
        select
            topic_id,
            ministry_addressed,
            count(*) as ministry_count,
            rank() over (partition by topic_id order by count(*) desc) as ministry_rank
        from extract_ministry
        where ministry_addressed is not null
        group by topic_id, ministry_addressed
    ),

    get_majority_ministry as (
        select
            topic_id,
            ministry_addressed,
            ministry_rank,
            safe_divide(
                ministry_count, sum(ministry_count) over (partition by topic_id)
            ) as topic_ministry_proportion
        from agg_ministry_counts_by_topic
        group by topic_id, ministry_addressed, ministry_count, ministry_rank
    ),
    pivot_pri_and_sec_ministry as (
        select
            topic_id,
            max(
                case when ministry_rank = 1 then ministry_addressed end
            ) as ministry_addressed_primary,
            max(
                case when ministry_rank = 1 then topic_ministry_proportion end
            ) as ministry_proportion_primary,
            max(
                case when ministry_rank = 2 then ministry_addressed end
            ) as ministry_addressed_secondary,
            max(
                case when ministry_rank = 2 then topic_ministry_proportion end
            ) as ministry_proportion_secondary
        from get_majority_ministry
        group by topic_id
    ),

    join_majority_ministry_for_topic as (
        select
            extract_ministry.*,
            pivot_pri_and_sec_ministry.ministry_addressed_primary,
            pivot_pri_and_sec_ministry.ministry_proportion_primary,
            pivot_pri_and_sec_ministry.ministry_addressed_secondary,
            pivot_pri_and_sec_ministry.ministry_proportion_secondary
        from extract_ministry
        left join
            pivot_pri_and_sec_ministry
            on extract_ministry.topic_id = pivot_pri_and_sec_ministry.topic_id
    )

select *
from join_majority_ministry_for_topic
