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

    calculate_percentile_speeches as (
        select
            *,
            percentile_cont(count_words, 0.1) over () as percentile10_count_words,
            percentile_cont(count_words, 0.9) over () as percentile90_count_words,
        from source
    ),

    /*
        Speech Characteristics to be flagged:
        1. If the speech was made in vernacular (non-English)
        2. If the speech was shorter than the 10th percentile number of words in a speech.
        3. If the speech was longer than the 90th percentile number of words in a speech.
    */
    flag_speech_characteristics as (
        select
            * except(percentile10_count_words, percentile90_count_words),
            count_words <= percentile10_count_words as is_short_speech,
            count_words >= percentile90_count_words as is_long_speech,
            contains_substr(text, 'vernacular speech') as is_vernacular_speech,
        from calculate_percentile_speeches
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
