{{
    config(
        materialized='table'
    )
}}

with topics as (
    select
        topic_id,
        date,
        topic_order,
        title,
        section_type
    from {{ ref('stg_topics') }}
),

seed_topic_type as (
    select
        section_type_code,
        section_type_name
    from {{ ref('topic_type') }}
),

joined as (
    select
        topics.topic_id,
        topics.date,
        topics.topic_order,
        topics.title,
        topics.section_type,
        seed_topic_type.section_type_name
    from topics
    left join seed_topic_type
        on topics.section_type = seed_topic_type.section_type_code
)

select *
from joined
