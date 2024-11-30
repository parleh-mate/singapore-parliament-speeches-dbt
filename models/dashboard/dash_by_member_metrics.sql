with
    cte_speeches as (
        select *, count_speeches - count_pri_questions as count_only_speeches
        from {{ ref("agg_speech_metrics_by_member") }}
    ),
    cte_readability as (
        select
            member_name,
            parliament,
            206.835
            - 1.015 * (count_speeches_words / count_speeches_sentences)
            - 84.6
            * (count_speeches_syllables / count_speeches_words) as readability_score
        from {{ ref("mart_speeches") }}
        where
            not is_vernacular_speech
            and not is_primary_question
            and count_speeches_words > 0
            and member_constituency is not null
            and 'Speaker' not in unnest(member_appointments)
    ),
    by_parl_speeches as (
        select
            member_name,
            cast(parliament as string) as parliament,
            member_party,
            member_constituency,
            round(
                case
                    when sum(count_sittings_present) = 0
                    then 0
                    else sum(count_pri_questions) / sum(count_sittings_present)
                end,
                2
            ) as questions_per_sitting,
            cast(
                case
                    when sum(count_only_speeches) = 0
                    then 0
                    else sum(count_words) / sum(count_only_speeches)
                end as integer
            ) as words_per_speech,
            round(
                case
                    when sum(count_sittings_present) = 0
                    then 0
                    else sum(count_only_speeches) / sum(count_sittings_present)
                end,
                2
            ) as speeches_per_sitting
        from cte_speeches
        group by parliament, member_party, member_constituency, member_name
    ),
    by_parl_join_readability as (
        select *
        from by_parl_speeches
        left join
            (
                select
                    member_name,
                    cast(parliament as string) as parliament,
                    round(avg(readability_score), 1) as readability_score
                from cte_readability
                group by member_name, parliament
            ) using (member_name, parliament)
    ),
    all_parl_speeches as (
        select
            member_name,
            'All' as parliament,
            member_party,
            'All' as member_constituency,
            round(
                case
                    when sum(count_sittings_present) = 0
                    then 0
                    else sum(count_pri_questions) / sum(count_sittings_present)
                end,
                2
            ) as questions_per_sitting,
            cast(
                case
                    when sum(count_only_speeches) = 0
                    then 0
                    else sum(count_words) / sum(count_only_speeches)
                end as integer
            ) as words_per_speech,
            round(
                case
                    when sum(count_sittings_present) = 0
                    then 0
                    else sum(count_only_speeches) / sum(count_sittings_present)
                end,
                2
            ) as speeches_per_sitting
        from cte_speeches
        where member_constituency is not null
        group by member_name, member_party
    ),
    all_parl_join_readability as (
        select *
        from all_parl_speeches
        left join
            (
                select
                    member_name,
                    'All' as parliament,
                    round(avg(readability_score), 1) as readability_score
                from cte_readability
                group by member_name
            ) using (member_name, parliament)
    ),
    speech_agg as (
        select *
        from by_parl_join_readability
        union all
        select *
        from all_parl_join_readability
    ),
    by_parl as (
        select
            member_name,
            cast(parliament as string) as parliament,
            member_party,
            member_constituency,
            sum(count_sittings_total) as sittings_total,
            sum(count_sittings_present) as sittings_present,
            sum(count_sittings_spoken) as sittings_spoken
        from {{ ref("agg_speech_metrics_by_member") }}
        group by member_name, parliament, member_party, member_constituency
    ),
    all_parl as (
        select
            member_name,
            'All' as parliament,
            member_party,
            'All' as member_constituency,
            sum(count_sittings_total) as sittings_total,
            sum(count_sittings_present) as sittings_present,
            sum(count_sittings_spoken) as sittings_spoken
        from {{ ref("agg_speech_metrics_by_member") }}
        group by member_name, member_party
    ),
    participation as (
        select *
        from by_parl
        union all
        select *
        from all_parl
    ),

    round_participation as (
        select
            member_name,
            parliament,
            member_party,
            member_constituency,
            round(100 * sittings_present / nullif(sittings_total, 0), 1) as attendance,
            round(
                100 * sittings_spoken / nullif(sittings_present, 0), 1
            ) as participation
        from participation
    ),
    join_speech_and_participation as (
        select *
        from speech_agg
        left join
            round_participation using (
                member_name, member_party, member_constituency, parliament
            )
    )

select *
from join_speech_and_participation
where member_constituency is not null
