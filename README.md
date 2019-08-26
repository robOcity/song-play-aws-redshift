# AWS Redshift Data Warehouse

## SQL Commands

* Load data from S3:

* Copy JSON data into table parsing time stamps

```bash
COPY event_staging FROM 's3://udacity-dend/log-data/2018/11/2018-11-11-events.json'
IAM_ROLE 'arn:aws:iam::921412997039:role/dwhRole'
JSON 's3://dend-util/events_log_jsonpath.json' 
TIMEFORMAT 'epochmillisecs'
REGION 'us-west-2';
```

* Map columns from JSON log files to columns in a table that have different names.  This example is based on the event logs of user activity.  

```json
{
    "jsonpaths": [
        "$['artist']",
        "$['auth']",
        "$['firstName']",
        "$['gender']",
        "$['itemInSession']",
        "$['lastName']",
        "$['length']",
        "$['level']",
        "$['location']",
        "$['method']",
        "$['page']",
        "$['registration']",
        "$['sessionId']",
        "$['status']",
        "$['ts']",
        "$['userAgent']"
    ]
}
```

* Create the Redshift table using SQL using revised column names.  JSON data elements are extracted based on the name given in the jsonpaths file and inserted into the table based on the order they are defined jsonpaths file.  The order of the data elements in the jsonpaths file must match that of the SQL table.  For example, `itemInSession` is inserted into the 5th column of the SQL table.  In the process the name is effectively changed from `itemInSession` to `item_in_session`.  This name translation only occurs because I am using a jsonpaths files that maps the values between representations.  

```sql
CREATE TABLE IF NOT EXISTS event_staging (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender text,
    item_in_session int,
    last_name text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    user_id text,
    session_id int,
    status int,
    start_time timestamp,
    user_agent text
)
DISTSTYLE even
SORTKEY (start_time);
```
* Listing the tables in the database using SQL since PSQL in not an option of Redshift

```
SELECT table_schema,table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_schema,table_name;
```


## AWS Command Line Notes

* List all files in S3 bucket: `aws s3 ls s3://udacity-dend/log-data --recursive`

* Display S3 text file: `aws s3 cp --quiet  s3://udacity-dend/song-data/G/A/W/TRGAWQH128F4222C36.json /dev/stdout`
