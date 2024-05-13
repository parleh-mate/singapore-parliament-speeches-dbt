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
    ),
    cleaned as (
        select
            * except (title),
            case
                when title = 'Foreign Interference (countermeasures) Bill'
                then 'Foreign Interference (Countermeasures) Bill'
                when title = 'Monetary Authority of  Singapore (Amendment) Bill'
                then 'Monetary Authority of Singapore (Amendment) Bill'
                when bill_number = '22/2023'
                then 'Constitution of the Republic of Singapore (Amendment No. 2) Bill'
                else title
            end as title
        from renamed
    )
select *
from cleaned
