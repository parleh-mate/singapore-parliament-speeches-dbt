with
    speech_table as (
        select
            parliament,
            `date`,
            member_party,
            member_constituency,
            member_name,
            speech_summary,
            topic_id,
            topic_assigned,
            topic_title
        from `singapore-parliament-speeches.prod_mart.mart_speech_summaries` --anti-pattern, needs to be refactored
        left join
            (
                select * from {{ ref("mart_speeches") }}
            ) using (speech_id)
        where member_constituency is not null
    ),
    get_topic_engagements as (
        select topic_id, count(*) as topic_engagements
        from {{ ref("mart_speeches") }}
        where
            topic_type_name not like "%Correction by Written Statements%"
            and topic_type_name not like "%Bill Introduced%"
        group by topic_id
    ),
    join_topic_engagements as (
        select
            parliament,
            `date`,
            member_party,
            member_constituency,
            member_name,
            speech_summary,
            topic_assigned,
            topic_title,
            topic_engagements
        from speech_table
        left join get_topic_engagements using (topic_id)
    )
select *
from join_topic_engagements
