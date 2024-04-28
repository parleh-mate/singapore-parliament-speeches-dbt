with
    source as (select * from {{ source("raw", "members_select_committee") }}),
    renamed as (
        select
            {{ adapter.quote("member_name") }} as member_name,
            {{ adapter.quote("parliament") }} as parliament,
            {{ adapter.quote("session") }} as session,
            {{ adapter.quote("committee") }} as member_committee,
            {{ adapter.quote("position") }} as member_committe_position

        from source
    )
select *
from renamed
