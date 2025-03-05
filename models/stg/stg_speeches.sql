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
                    when member_name like '%Deputy%'
                    then 'Deputy Speaker'
                    when member_name like '%Speaker%'
                    then 'Speaker'
                    else trim(replace(member_name, chr(160), ' '))
                end as string
            ) as member_name,
            cast(text as string) as text,
            if(cast(num_characters as int64) != 0, cast(num_words as int64), 0) as count_words,
            cast(num_characters as int64) as count_characters,
            if(cast(num_characters as int64) != 0, cast(num_sentences as int64), 0) as count_sentences,
            if(cast(num_characters as int64) != 0, cast(num_syllables as int64), 0) as count_syllables,
            cast(date as date) as date

        from {{ source("raw", "speeches") }}
    ),

    standardise_member_name as (
        select
            * except (member_name),
            -- mapping of names here:
            -- https://docs.google.com/spreadsheets/d/1Wk_PDlQbWWViTV9NmDPsXwu54TD96ltEycLAKOHe1fA/edit#gid=1498942254
            coalesce(mapping.member_name, type_cast.member_name) as member_name
        from type_cast
        left join
            {{ source("google_sheets", "member_name_mapping") }} as mapping
            on mapping.possible_variation = type_cast.member_name
    )

select *
from standardise_member_name
