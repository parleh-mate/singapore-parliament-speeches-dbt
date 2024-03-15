{{ config(
    materialized='table'
) }}


with processing as (
  select
    -- cleaning some titles to standardise them so they can be merged together
    case
      when ba.title = 'Central Provident Fund (Amendment No.2) Bill'
        then 'Central Provident Fund (Amendment No 2) Bill'
      when ba.title = 'Co-operatives Societies (Amendment) Bill'
        then 'Co-operative Societies (Amendment) Bill'
      when ba.title = 'Criminal Law (Temporary Provisions) Amendment Bill'
        then 'Criminal Law (Temporary Provisions) (Amendment) Bill'
      when ba.title = 'Final Supply (FY2015) Bill'
        then 'Final Supply (FY 2015) Bill'
      when ba.title = 'Economic Expansion Incentives (Relief from Income Tax) (Amendment) Bill'
        then 'Economic Expansion Incentives (Relief From Income Tax) (Amendment) Bill'
      when trim(ba.title) = 'Financial Services and Markets (Amendments) Bill'
        then 'Financial Services and Markets (Amendment) Bill'
      when ba.title = 'Good and Services Tax (Amendment) Bill'
        then 'Goods and Services Tax (Amendment) Bill'
      when ba.title = 'Housing and Development (Amendment) Bill'
        then 'Housing and Development Board (Amendment) Bill'
      when ba.title = 'Income Tax (Amendment) (No 3) Bill'
        then 'Income Tax (Amendment No 3) Bill'
      when ba.title = 'Motor Vehicles (Third-party Risks and Compensation) (Amendment) Bill'
        then 'Motor Vehicles (Third-Party Risks and Compensation) (Amendment) Bill'
      when ba.title = 'Second Supplementary Supply (2021) Bill'
        then 'Second Supplementary Supply (FY 2021) Bill'
      when ba.title = 'Statue Law Reform Bill'
        then 'Statute Law Reform Bill'
      when ba.title = 'Supplementary Supply (FY2016) Bill'
        then 'Supplementary Supply (FY 2016) Bill'
      when ba.title = 'Supplementary Supply (FY2019) Bill'
        then 'Supplementary Supply (FY 2019) Bill'
      when trim(ba.title) = 'Supply BIll'
        then 'Supply Bill'
      when ba.title = 'Tobacco (Control of Advertisements and Sale (Amendment) Bill'
        then 'Tobacco (Control of Advertisements and Sale) (Amendment) Bill'
      else trim(ba.title)
    end
      as title,
    ba.reading,
    ba.topic_id,
    ba.date,
    extract(year from ba.date) as year,
  from {{ ref('fact_bill_activity') }} as ba
),

summarise as (
select
  year,
  replace(trim(title), '.', '') as title,
  -- 1 first reading
  min(case
    when left(reading, 1) = '1'
      then date
  end) as first_reading_date,
  min_by(topic_id, case
    when left(reading, 1) = '1'
      then date
  end) as first_reading_topic,
  -- 2 second reading
  min(case
    when left(reading, 1) = '2'
      then date
  end) as second_reading_date,
  min_by(topic_id, case
    when left(reading, 1) = '2'
      then date
  end) as second_reading_topic,
  -- 3 third reading
  max(case
    when left(reading, 1) = '3'
      then date
  end) as third_reading_date,
  max_by(topic_id, case
    when left(reading, 1) = '3'
      then date
  end) as third_reading_topic
from processing
group by 1, 2
),

-- process bills where dates were joined across two years

complete as (
  select
    summarise.year,
    summarise.title,
    summarise.first_reading_date,
    summarise.first_reading_topic,
    summarise.second_reading_date,
    summarise.second_reading_topic,
    summarise.third_reading_date,
    summarise.third_reading_topic
  from summarise
  where not(first_reading_date is null or second_reading_date is null)
),

incomplete as (
  select
    summarise.year,
    summarise.year+1 as year_plus_1,
    summarise.title,
    summarise.first_reading_date,
    summarise.first_reading_topic,
    summarise.second_reading_date,
    summarise.second_reading_topic,
    summarise.third_reading_date,
    summarise.third_reading_topic
  from summarise
  where first_reading_date is null or second_reading_date is null
),

-- it is possible that the bill passage was raised across 2 years, to find this information
incomplete_combine_across_different_years as (
  select
    i0.year,
    i0.title,
    coalesce(i0.first_reading_date, i1.first_reading_date) as first_reading_date,
    coalesce(i0.first_reading_topic, i1.first_reading_topic) as first_reading_topic,
    coalesce(i0.second_reading_date, i1.second_reading_date) as second_reading_date,
    coalesce(i0.second_reading_topic, i1.second_reading_topic) as second_reading_topic,
    coalesce(i0.third_reading_date, i1.third_reading_date) as third_reading_date,
    coalesce(i0.third_reading_topic, i1.third_reading_topic) as third_reading_topic,
  from incomplete as i0
  left join incomplete as i1
    on i0.year_plus_1 = i1.year
    and i0.title = i1.title
  -- to drop rows where information has been filled
  qualify not lag(i0.first_reading_date, 1) over(partition by i0.title order by i0.year) is not null
  order by i0.title
),

unioned as (
  select
    year,
    title,
    first_reading_date,
    first_reading_topic,
    second_reading_date,
    second_reading_topic,
    third_reading_date,
    third_reading_topic
  from incomplete_combine_across_different_years
  union all
  select
    year,
    title,
    first_reading_date,
    first_reading_topic,
    second_reading_date,
    second_reading_topic,
    third_reading_date,
    third_reading_topic
  from complete
)

select
    year,
    title,
    first_reading_date,
    first_reading_topic,
    date_diff(second_reading_date, first_reading_date, DAY) as day_diff_first_second_reading,
    second_reading_date,
    second_reading_topic,
    date_diff(third_reading_date, second_reading_date, DAY) as day_diff_second_third_reading,
    third_reading_date,
    third_reading_topic
from unioned
order by title

