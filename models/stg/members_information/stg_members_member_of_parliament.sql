with
    source as (select * from {{ source("raw", "members_member_of_parliament") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("constituency") }} as member_constituency,
            {{ adapter.quote("from_date") }} as effective_from_date,
            {{ adapter.quote("to_date") }} as effective_to_date

        from source
    )
select *
from renamed
