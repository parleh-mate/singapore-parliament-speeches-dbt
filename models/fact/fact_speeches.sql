{{
    config(
        materialized='table'
    )
}}

with speeches as (
  select 
    speeches.*,
    topics.section_type,
    row_number() over(partition by speeches.topic_id order by speeches.speech_order) as speech_order_by_topic
  from {{ ref('stg_speeches') }} as speeches
  left join p{{ ref('stg_topics') }} as topics
    on speeches.topic_id = topics.topic_id
),

replying_member as (
  select
    topic_id,
    member_name
  from speeches
  where speech_order_by_topic = 2
    and section_type in ('OA', 'WA')
),

flag_primary_supplementary_question as (
  select
    speeches.*,
    replying_member.member_name as replying_member_name,

    speeches.speech_order_by_topic = 1 
      and lower(speeches.text) like '%ask%' as is_primary_question,

    speeches.speech_order_by_topic > 1 
      and lower(speeches.text) like any ('%ask%', '%supplementary question%') 
      and speeches.member_name != replying_member.member_name
      as is_supplementary_question

  from speeches
  left join replying_member
    on speeches.topic_id = replying_member.topic_id
)

select  
  speech_id,
  date,
  topic_id,
  speech_order,
  member_name,
  text,
  count_words,
  count_characters,
  speech_order_by_topic,
  replying_member_name,
  coalesce(is_primary_question, false) as is_primary_question,
  coalesce(is_supplementary_question, false) as is_supplementary_question
from flag_primary_supplementary_question
