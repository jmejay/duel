This folder is the base ingestion and clearing of the raw data, effectively the raw -> structured.
Because of the decision to use postgres (for the first time!) it's a rather structured data pipeline as opposed to something like mongodb, however as a proof of concept it shows data cleansing, incremental table creation (brand.sql) and a lookup / dimensional system in place (advocacy_programs left join).

There is a clear flaw in this design in that if this was to process more data files, the index_id from the dataframe would conflict. This could be solved easily by again using incremental tables, snapshots for SCD2, or by hashmap's on composite keys across the data such that subsequent runs would not conflict with previously ingested data.


```sql
SELECT 
    b.index, 
    b.user_id, 
    b.name, 
    b.email, 
    b.instagram_handle, 
    b.tiktok_handle, 
    b.joined_at, 
    ap.index, 
    ap.program_id, 
    ap.total_sales_attributed,
    l_br.brand_id,
    l_br.brand,
    tc.index, 
    tc.task_id, 
    tc.platform, 
    tc.post_url, 
    tc.likes, 
    tc.comments, 
    tc.shares, 
    tc.reach
FROM dbt.base b 
LEFT JOIN dbt.advocacy_programs ap
ON b.index = ap.index 
LEFT JOIN dbt_lookup.brand l_br
ON ap.brand_id = l_br.brand_id
LEFT JOIN dbt.tasks_completed tc
ON b.index = tc.index 
```

