{{
    config(
        materialized='table'
    )
}}

-- sources

with topics as (
    select
        topic_id,
        title,
        date,
        section_type
    from {{ ref('dim_topics') }}
    where section_type in ('BI', 'BP')
),

-- 1. first readings

first_reading as (
    select
        topic_id,
        '1 First' as reading
    from topics
    where section_type = 'BI'
),

-- 2. second and third readings

second_third_reading_topics as (
    select
        topic_id,
        title
    from topics
    where section_type = 'BP'
),

flag_second_third_reading_from_speeches as (
    select
        speeches.topic_id,
        lower(speeches.text) like any('%second reading%', '%second time%')
            as is_second_reading,
        lower(speeches.text) like any('%third reading%', '%third time%')
            as is_third_reading
    from {{ ref('fact_speeches') }} as speeches
    inner join second_third_reading_topics
        on speeches.topic_id = second_third_reading_topics.topic_id
),

summarise_flags_by_topic as (
    select
        topic_id,
        max(is_second_reading) as is_second_reading,
        max(is_third_reading) as is_third_reading
    from flag_second_third_reading_from_speeches
    group by topic_id
),

second_reading as (
    select
        topic_id,
        '2 Second' as reading
    from summarise_flags_by_topic
    where is_second_reading
),

third_reading as (
    select
        topic_id,
        '3 Third' as reading
    from summarise_flags_by_topic
    where is_third_reading
),

unioned as (
    select
        topic_id,
        reading
    from first_reading
    union all
    select
        topic_id,
        reading
    from second_reading
    union all
    select
        topic_id,
        reading
    from third_reading
),

joined as (
    select
        topics.date,
        unioned.topic_id,
        topics.title,
        unioned.reading
    from unioned
    left join topics
        on unioned.topic_id = topics.topic_id
)

select *
from joined
