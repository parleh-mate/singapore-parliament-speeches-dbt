-- materialize as table to allow for use without needing to expand drive scopes for other projects --
{{ config(materialized='table') }}

with
    source as (select * from {{ source("google_sheets", "parliament_dates") }})
select *
from source
