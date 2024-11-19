with speech_lengths as (SELECT count_speeches_words
FROM `singapore-parliament-speeches.prod_mart.mart_speeches`
WHERE topic_type_name not like "%Correction by Written Statements%"
AND topic_type_name not like "%Bill Introduced%"
AND member_name != ''
AND member_name != 'Speaker'
)
select *
from speech_lengths