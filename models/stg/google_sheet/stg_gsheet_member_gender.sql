with
    source as (select * from {{ source("google_sheets", "gender") }})
select *
from source
