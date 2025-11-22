with
    source as (select * from {{ source("google_sheets", "party") }})
select *
from source
