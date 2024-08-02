with
    source as (select * from {{ source("google_sheets", "ethnicity") }}),
    renamed as (select member_name, ethnicity as member_ethnicity from source)
select *
from renamed
