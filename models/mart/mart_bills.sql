{{ config(materialized="table") }}


with
    bill_activity as (
        select
            year,
            title,
            first_reading_date,
            first_reading_topic,
            second_reading_date,
            second_reading_topic,
            third_reading_date,
            third_reading_topic
        from {{ ref("mart_prep_bill_activity") }}
    ),

    bill_introduced as (
        select
            bill_number,
            extract(year from first_reading_date) as year,
            title,
            first_reading_date,
            second_reading_date,
            passed_date,
            bill_pdf_link
        from {{ ref("fact_bills_introduced") }}
    ),

    joined as (
        select
            introduced.bill_number,
            activity.year,
            coalesce(introduced.title, activity.title) as title,
            coalesce(
                introduced.first_reading_date, activity.first_reading_date
            ) as first_reading_date,
            activity.first_reading_topic,
            coalesce(
                introduced.second_reading_date, activity.second_reading_date
            ) as second_reading_date,
            activity.second_reading_topic,
            activity.third_reading_date,
            activity.third_reading_topic,
            introduced.passed_date,
            introduced.bill_pdf_link
        from bill_introduced as introduced
        full join
            bill_activity as activity
            on introduced.title = activity.title
            and introduced.year = activity.year
    )

select
    bill_number,
    year,
    title,
    first_reading_date,
    first_reading_topic,
    second_reading_date,
    second_reading_topic,
    third_reading_date,
    third_reading_topic,
    passed_date,
    date_diff(
        second_reading_date, first_reading_date, day
    ) as day_diff_first_second_reading,
    date_diff(
        third_reading_date, second_reading_date, day
    ) as day_diff_second_third_reading,
    date_diff(
        third_reading_date, second_reading_date, day
    ) as day_diff_third_reading_passed,
    bill_pdf_link
from joined
