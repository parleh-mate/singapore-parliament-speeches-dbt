with
    source as (select * from {{ source("raw", "members_office_holding") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("position") }} as member_appointment,
            {{ adapter.quote("from_date") }} as effective_from_date,
            {{ adapter.quote("to_date") }} as effective_to_date

        from source
    ),
    add_latest_flag as (
        select
            *,
            effective_to_date
            is null  -- when it is null, this is the current appointment
            as is_latest_appointment
        from renamed
    )
select *
from add_latest_flag