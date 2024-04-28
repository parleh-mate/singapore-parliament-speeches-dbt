{{ config(materialized="view", sort="speech_id") }}

with
    type_cast as (
        select
            cast(speech_id as string) as speech_id,
            cast(topic_id as string) as topic_id,
            cast(speech_order as int64) as speech_order,
            cast(
                case
                    when member_name_original like '%Chairman%'
                    then 'Speaker'
                    else member_name
                end as string
            ) as member_name,
            cast(text as string) as text,
            cast(num_words as int64) as count_words,
            cast(num_characters as int64) as count_characters,
            date(left(topic_id, 10)) as date

        from {{ source("raw", "speeches") }}
    )

select *
from type_cast
