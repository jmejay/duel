{{ config(materialized='table') }}

SELECT
        index,
        NULLIF(task_id, 'None') AS task_id,
        NULLIF(platform, '123') AS platform,
        NULLIF(post_url, 'broken_link') AS post_url,
        NULLIF(likes, 'NaN') AS likes,
        NULLIF(comments, 'nan')::numeric::integer AS comments,
        shares::int,
        reach::bigint
FROM
        {{ source('rawpg', 'source__tasks_completed') }} AS base



