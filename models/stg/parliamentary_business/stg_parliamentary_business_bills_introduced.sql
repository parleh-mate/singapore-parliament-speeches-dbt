with
    source as (
        select * from {{ source("parliamentary_business", "bills_introduced") }}
    ),
    renamed as (
        select
            {{ adapter.quote("title") }} as title,
            {{ adapter.quote("number") }} as bill_number,
            {{ adapter.quote("pdf_link") }} as bill_pdf_link,
            cast({{ adapter.quote("date_introduced") }} as date) as first_reading_date,
            cast(
                {{ adapter.quote("date_2nd_reading") }} as date
            ) as second_reading_date,
            cast({{ adapter.quote("date_passed") }} as date) as passed_date

        from source
    )
select *
from renamed
