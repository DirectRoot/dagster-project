with u as (
    select *
    from {{ source('okta_users', 'users') }}
)

select *
from u