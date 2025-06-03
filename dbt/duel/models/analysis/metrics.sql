{{ config(
    materialized='table',
    schema='analysis') 
}}

SELECT 
    Platform, 
    COUNT(*) AS count, 
    AVG(reach) AS avg_reach, 
    SUM(reach) AS total_reach, 
    date_part('year', joined_at) AS year, 
    to_char(joined_at, 'Month') AS month
FROM {{ ref('base') }} b
LEFT JOIN {{ ref('tasks_completed') }} t
ON b.index = t.index
WHERE joined_at IS NOT Null 
GROUP BY Platform, to_char(joined_at, 'Month'), date_part('year', joined_at)