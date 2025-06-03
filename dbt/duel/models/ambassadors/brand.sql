{{ config(
    materialized='incremental',
    unique_key='brand',
    schema='lookup') 
}}


WITH cte AS (

SELECT DISTINCT brand 
FROM {{ source('rawpg', 'source__advocacy_programs') }} source
WHERE NULLIF(brand, '12345') IS NOT Null

)

SELECT 
gen_random_uuid() AS brand_id,
cte.brand
FROM cte 
{% if is_incremental() %}
LEFT JOIN {{ this }} AS old
ON cte.brand = old.brand 

WHERE old.brand is Null
{% endif %}