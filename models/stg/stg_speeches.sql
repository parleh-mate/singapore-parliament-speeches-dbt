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
                    -- amy khor, but with different encoding
                    when
                        to_hex(cast(member_name as bytes))
                        = '416d79204b686f72c2a04c65616e205375616e'
                    then 'Amy Khor Lean Suan'
                    when member_name = 'David Ong Kim Huat'
                    then 'David Ong'
                    when
                        to_hex(cast(member_name as bytes))
                        = '44656e6973652050687561204c6179c2a050656e67'
                    then 'Denise Phua Lay Peng'
                    when member_name = 'Dr Yaacob Ibrahim'
                    then 'Yaacob Ibrahim'
                    when
                        to_hex(cast(member_name as bytes))
                        = '48656e672053776565c2a04b656174'
                    then 'Heng Swee Keat'
                    when
                        to_hex(cast(member_name as bytes))
                        = '496e6472616e6565c2a052616a6168'
                    then 'Indranee Rajah'
                    when
                        to_hex(cast(member_name as bytes))
                        = '4972656e65204e67205068656bc2a0486f6f6e67'
                    then 'Irene Ng Phek Hoong'
                    when member_name = 'Josephine'
                    then 'Josephine Teo'
                    when
                        to_hex(cast(member_name as bytes))
                        = '4c696d2042696f77c2a0436875616e'
                    then 'Lim Biow Chuan'
                    when member_name = 'Melvin Yong'
                    then 'Melvin Yong Yik Chye'
                    when member_name = 'Mohd Fahmi Bin Aliman'
                    then 'Mohd Fahmi Aliman'
                    when
                        to_hex(cast(member_name as bytes))
                        = '4d7568616d61642046616973616c2042696e20416264756cc2a04d616e6170'
                    then 'Muhamad Faisal Bin Abdul Manap'
                    when
                        to_hex(cast(member_name as bytes))
                        = '5269746120536f682053696f77c2a04c616e'
                    then 'Rita Soh Siow Lan'
                    when
                        to_hex(cast(member_name as bytes))
                        = '596565204a656e6ec2a04a6f6e67'
                    else trim(member_name)
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
