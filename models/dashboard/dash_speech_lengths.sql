with
    speech_lengths as (
        select count_speeches_words
        from {{ ref("mart_speeches") }}
        where
            topic_type_name not like "%Correction by Written Statements%"
            and topic_type_name not like "%Bill Introduced%"
            and member_name != ''
            and member_name != 'Speaker'
    )
select *
from speech_lengths
