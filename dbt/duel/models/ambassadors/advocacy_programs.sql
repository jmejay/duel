{{ config(materialized='table') }}

SELECT
        index,
        NULLIF(program_id, '') AS program_id,
        brand.brand_id,
        NULLIF(total_sales_attributed, 'no-data') AS total_sales_attributed
FROM
        {{ source('rawpg', 'source__advocacy_programs') }} AS base

LEFT JOIN 

        {{ ref('brand') }} AS brand 
        ON NULLIF(base.brand, '12345') = brand.brand

WHERE
        COALESCE(program_id, base.brand, total_sales_attributed) <> ''

