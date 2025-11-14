with
    source as (select * from {{ source("google_sheets", "gender") }}),
    renamed as (select member_name, gender as member_gender from source)
select *
from renamed
