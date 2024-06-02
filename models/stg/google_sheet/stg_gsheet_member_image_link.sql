with source as (
      select * from {{ source('google_sheets', 'member_image_link') }}
),
renamed as (
    select
        {{ adapter.quote("member_name") }},
        {{ adapter.quote("member_image_link") }}

    from source
)
select * from renamed
