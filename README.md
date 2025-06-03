

# 0) Please install;

- python 3.12.2
- Docker desktop 

The requirements.txt can be applied to your venv for everything you need to run.

# 1) Basic data exploration:
TLDR;
- data_debugging.ipynb is a sandbox to explore and fix data then upload to postgres

The initial data exploration has been done and documented in the data_debugging.ipynb notebook. 
This is a sandbox for debugging the data, finding issues and exploring the basic structure. Between python pandas and a data viewer like datawrangler, you can get a good feel for the state of the data. 
This notebook reads all the correct json files, then parses the incorrect files and loads them too, for a 100% ingestion of data. It was also must easier to manipulate the nested JSON objects in pandas, as they were broken and not all rows could be parsed correctly.
The bigger picture for a task like this would be that it would be implemented as a task inside an airflow DAG for more robust error tracking and scheduling. I did try to set up an Airflow instance to showcase this, but time did not permit!

# 2) dbt-core

dbt/duel/models/ambassadors is the base ingestion and clearing of the raw data, effectively the raw -> structured.
Because of the decision to use postgres (for the first time!) it's a rather structured data pipeline as opposed to something like mongodb, however as a proof of concept it shows data cleansing, incremental table creation (brand.sql) and a lookup / dimensional system in place (see advocacy_programs left join).

This design would be no good for production in that if this was to process more data files, the index_id from the dataframe would conflict every run. This could be solved easily by again using incremental tables, snapshots for SCD2, or by hashmaps on composite keys across the data such that subsequent runs would not conflict with previously ingested data.

The JSON structures are fully unpacked into their own tables and some assumptions were made on the value of the data, with an approach taken to throw away data we are sure is not useful. Below is a map of the full unpacked dataset. The nested JSON objects in the dataset were always of length 1 and thus easy to map to the base data, but the logic that it could be implemented here as a one to many map works fine.


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
    l_br.brand, -- a lookup to showcase incremental models
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

dbt/duel/models/analysis can contain metrics as built in tandem with data scientists / or built in a way to apply ML design patterns to. The maths and analysis can be offered by an analyst and the data structure and efficiency built by a data engineer - dbt is great for this integration.

I have included a basic model here that references the prerequisite tables as proof of concept of analysis in dbt. The data is neatly unpacked now so any metric can easily be aggregated.

# 3) data output
The data created so far can now be viewed at localhost:5432 in a postgres explorer like pgAdmin. At this stage the data should be delivered automatically for consideration, via visualisation or shared data dumps with a notification to interested parties. This would be again an Airflow task, to either deliver the file / notification of completion of new datasets, or upon failure to notify by email that the pipeline has failed.