with
    cte as (
        select
            parliament,
            `date`,
            member_party,
            b.member_constituency,
            member_name,
            topic_assigned
        from `singapore-parliament-speeches.prod_mart.mart_speech_positions`  -- anti-pattern, needs to be refactored
        left join (select * from {{ ref("mart_speeches") }}) as a
        left join
            (
                select distinct
                    member_name, member_party, parliament, member_constituency
                from {{ ref("agg_speech_metrics_by_member") }}
            ) as b using (member_name, member_party, parliament) using (speech_id)
        where b.member_constituency is not null
    ),
    by_parl as (
        select
            cast(parliament as string) as parliament,
            member_party,
            member_constituency,
            member_name,
            topic_assigned,
            count(*) as count_topic_speeches
        from cte
        group by
            parliament, member_party, member_constituency, member_name, topic_assigned
    ),
    all_parl as (
        select
            'All' as parliament,
            member_party,
            'All' as member_constituency,
            member_name,
            topic_assigned,
            count(*) as count_topic_speeches
        from cte
        group by member_party, member_name, topic_assigned
    )
select *
from by_parl
union all
select *
from all_parl
