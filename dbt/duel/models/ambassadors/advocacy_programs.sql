{{ config(materialized='table') }}

SELECT
        index,
        NULLIF(program_id, '') AS program_id,
        NULLIF(brand, '12345') AS brand,
        NULLIF(total_sales_attributed, 'no-data') AS total_sales_attributed
FROM
        {{ source('rawpg', 'source__advocacy_programs') }} AS base

WHERE
        COALESCE(program_id, brand, total_sales_attributed) <> ''

