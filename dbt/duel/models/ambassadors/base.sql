{{ config(materialized='table') }}

SELECT
        index,
        NULLIF(user_id, 'None') AS user_id,
        NULLIF(name, '???') AS name,
        NULLIF(LOWER(email), 'invalid-email') AS email,
        NULLIF(instagram_handle, 'None') AS instagram_handle,
        NULLIF(tiktok_handle, '#error_handle') AS tiktok_handle,
        to_timestamp(NULLIF(joined_at, 'not-a-date'), 'YYYY-MM-DDT0HH24:MI:SS') AS joined_at
FROM 
        {{ source('rawpg', 'source')}}

WHERE
        COALESCE(user_id, name, email, instagram_handle, tiktok_handle) <> 'None'