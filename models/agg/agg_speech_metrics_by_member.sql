with
    speeches as (
        select
            parliament,
            extract(year from date) as year,
            extract(month from date) as month,
            member_name,
            member_constituency,
            -- written answers topic_types should not be considered as spoken in the
            -- session
            count(
                distinct case when not topic_type in ('WA', 'WANA') then date end
            ) as count_sittings_spoken,
            count(distinct topic_id) as count_topics,
            count(*) as count_speeches,
            sum(count_speeches_words) as count_words,
            countif(is_primary_question) as count_pri_questions,
            sum(count_speeches_sentences) as count_sentences,
            sum(count_speeches_syllables) as count_syllables
        from {{ ref("mart_speeches") }}
        where
            member_name != ''
            and not lower(member_name) like any ('%deputy%', '%speaker%', '%chairman%')
            and not is_short_speech
            and not is_topic_procedural
        group by all
    ),

    attendance as (
        select
            parliament,
            extract(year from date) as year,
            extract(month from date) as month,
            member_name,
            member_constituency,
            countif(is_present) as count_sittings_present,
            count(*) as count_sittings_total
        from {{ ref("mart_attendance") }}
        where member_name != ''
        group by all

    ),

    join_metrics as (
        select
            coalesce(speeches.parliament, attendance.parliament) as parliament,
            coalesce(speeches.year, attendance.year) as year,
            coalesce(speeches.month, attendance.month) as month,
            coalesce(speeches.member_name, attendance.member_name) as member_name,
            coalesce(
                attendance.member_constituency, speeches.member_constituency
            ) as member_constituency,

            -- attendance-related
            attendance.count_sittings_total,
            attendance.count_sittings_present,

            -- participation-related
            coalesce(speeches.count_sittings_spoken, 0) as count_sittings_spoken,
            coalesce(speeches.count_topics, 0) as count_topics,
            coalesce(speeches.count_pri_questions, 0) as count_pri_questions,
            coalesce(speeches.count_speeches, 0) as count_speeches,

            -- readability-related
            coalesce(speeches.count_words, 0) as count_words,
            coalesce(speeches.count_sentences, 0) as count_sentences,
            coalesce(speeches.count_syllables, 0) as count_syllables

        from speeches
        full join attendance using (parliament, year, month, member_name)
    ),

    enrich_member_party as (
        select join_metrics.*, members.party as member_party
        from join_metrics
        left join {{ ref("dim_members") }} as members using (member_name)
    ),

    reorder_columns as (
        select
            parliament,
            year,
            month,
            member_name,
            member_party,
            member_constituency,

            count_sittings_total,
            count_sittings_present,
            count_sittings_spoken,
            count_topics,
            count_pri_questions,
            count_speeches,

            count_words,
            count_sentences,
            count_syllables
        from enrich_member_party
    )

select *
from reorder_columns
order by member_name, parliament, year, month
