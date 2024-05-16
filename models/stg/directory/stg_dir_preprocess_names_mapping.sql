with
    source as (select * from {{ source("sg_govt_dir_preprocess", "names_mapping") }}),
    renamed as (
        select {{ adapter.quote("extracted_name") }}, {{ adapter.quote("name") }}

        from source
    )
select *
from renamed
