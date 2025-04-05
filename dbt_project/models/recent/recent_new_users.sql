with 
    e as (
        select *
        from {{ ref('stg_new_user_events') }}
        where published > now() - interval '{{ var("recency_interval") }}'
            and outcome_result = 'SUCCESS'
    ),

    u as (
        select *
        from {{ ref('stg_users') }}
    )

select
    u.created as created_at,
    u.profile__first_name as first_name,
    u.profile__last_name as last_name,
    u.profile__login as "login",
    e.created_by,
    e.uuid as event_uuid

from e
left join u on e.user_id = u.id