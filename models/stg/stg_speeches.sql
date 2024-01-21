{{
    config(
        materialized='view',
        sort='speech_id'
    )
}}

with type_cast as (
    select
        cast(speech_id as string) as speech_id,
        cast(topic_id as string) as topic_id,
        cast(speech_order as int64) as speech_order,
        cast(member_name as string) as member_name,
        cast(text as string) as text,
        date(left(topic_id, 10)) as date

    from {{ source('raw', 'speeches') }}
),

reorder as (
    select
        speech_id,
        date,
        topic_id,
        speech_order,
        member_name,
        text
    from type_cast
)

select *
from reorder
