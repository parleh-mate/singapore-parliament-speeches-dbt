{{ config(materialized="table") }}

with
    source as (
        select
            speech_id,
            date,
            topic_id,
            speech_order,
            member_name,
            text,
            count_words,
            count_characters,
            count_sentences,
            count_syllables
        from {{ ref("stg_speeches") }}
    ),

    /*
        Speech Characteristics to be flagged:
        1. If the speech was made in vernacular (non-English)
    */
    flag_speech_characteristics as (
        select *, contains_substr(text, 'vernacular speech') as is_vernacular_speech,
        from source
    ),

    get_vernacular_speech_language as (
        select
            *,
            case
                when is_vernacular_speech
                then regexp_extract(lower(text), r'\(in (mandarin|malay|tamil)\)')
            end as vernacular_speech_language
        from flag_speech_characteristics
    )

select *
from get_vernacular_speech_language
