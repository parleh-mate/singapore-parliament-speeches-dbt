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
                    when member_name = 'Alex Yam Ziming'
                    then 'Alex Yam'
                    when member_name = 'David Ong Kim Huat'
                    then 'David Ong'
                    when member_name = 'Dr Yaacob Ibrahim'
                    then 'Yaacob Ibrahim'
                    when member_name = 'Josephine'
                    then 'Josephine Teo'
                    when member_name = 'Melvin Yong'
                    then 'Melvin Yong Yik Chye'
                    when member_name = 'Mohd Fahmi Bin Aliman'
                    then 'Mohd Fahmi Aliman'
                    else trim(replace(member_name, chr(160), ' '))
                end as string
            ) as member_name,
            cast(text as string) as text,
            cast(num_words as int64) as count_words,
            cast(num_characters as int64) as count_characters,
            cast(num_sentences as int64) as count_sentences,
            cast(num_syllables as int64) as count_syllables,
            date(left(topic_id, 10)) as date

        from {{ source("raw", "speeches") }}
    ),

    standardise_member_name as (
        select
            * except (member_name),
            case
                when member_name = 'Edwin Tong'
                then 'Edwin Tong Chun Fai'
                when member_name = 'Pritam'
                then 'Pritam Singh'
                when member_name = 'Josephine'
                then 'Josephine Teo'
                when member_name = 'Melvin Yong'
                then 'Melvin Yong Yik Chye'
                when member_name = 'Depty Speaker'
                then 'Deputy Speaker'
                when member_name = 'Leong Wai Mun'
                then 'Leong Mun Wai'
                when member_name = 'Mr K Shanmugam'
                then 'K Shanmugam'
                when member_name = 'Mr Ong Ye Kung'
                then 'Ong Ye Kung'
                when member_name = 'Speaker in the Chair'
                then 'Speaker'
                when member_name = 'Foo Mee Har West Coast'
                then 'Foo Mee Har'
                when member_name = 'Denise Phua Lay'
                then 'Denise Phua Lay Peng'
                else member_name
            end as member_name
        from type_cast
    )

select *
from standardise_member_name
