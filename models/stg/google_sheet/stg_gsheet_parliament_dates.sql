with
    source as (select * from {{ source("google_sheets", "parliament_dates") }})
select *
from source
