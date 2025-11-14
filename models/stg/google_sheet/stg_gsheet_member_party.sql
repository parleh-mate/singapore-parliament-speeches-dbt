with
    source as (select * from {{ source("google_sheets", "party") }}),
    renamed as (select member_name, party as member_party, parliament from source)
select *
from renamed
