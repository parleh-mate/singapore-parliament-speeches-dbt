{{ config(materialized="table") }}

with
    topics as (
        select topic_id, date, topic_order, title, section_type
        from {{ ref("stg_topics") }}
    ),

    seed_topic_type as (
        select section_type_code, section_type_name from {{ ref("topic_type") }}
    ),

    topic_group as (
        select topic_id, topic_group_id, topic_group_distribution
        from {{ ref("stg_topic_group") }}
    ),

    topic_name as (
        select topic_group_id, topic_group_name from {{ ref("stg_topic_names") }}
    ),

    joined as (
        select
            topics.topic_id,
            topics.date,
            topics.topic_order,
            topics.title,
            topics.section_type,
            seed_topic_type.section_type_name,
            topic_group.topic_group_id,
            topic_name.topic_group_name,
            topic_group.topic_group_distribution
        from topics
        left join
            seed_topic_type on topics.section_type = seed_topic_type.section_type_code
        left join topic_group on topics.topic_id = topic_group.topic_id
        left join topic_name on topic_group.topic_group_id = topic_name.topic_group_id
    ),

    flags as (
        select
            -- from previous cte
            topic_id,
            date,
            topic_order,
            title,
            section_type,
            section_type_name,
            topic_group_id,
            topic_group_name,
            topic_group_distribution,
            -- introduced in this cte
            section_type in ("BI", "BP")
            and lower(title) like "%constitution%" as is_constitutional
        from joined
    )

select *
from flags
