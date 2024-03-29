# Delta flow library

[![codecov](https://codecov.io/github/Temabit-FOZZY-Group/mirroring/branch/develop/graph/badge.svg?token=HT36LXMF80)](https://codecov.io/github/Temabit-FOZZY-Group/mirroring)

This library is used to read data from MS SQL only source and save them into _Delta_ format.

## Basic Usage

```shell script
spark-submit --master yarn --name demo_load --deploy-mode client /path/sparktest_2.12-0.1.jar
tab==Tbl schema==dbo numpart==5 splitby==colId dtflt==modified path_to_save==/path/on/hdfs/
jdbcUrl==jdbc:jtds:sqlserver://server\;instance=inst\;domain=dmn\;useNTLMv2=true\;user=usr\;databasename=db\;password=****** calc_min_dt==2021-06-01 calc_max_dt==2021-09-01
```

Library compares source and destination schemes. If column names don't match strictly `SchemaNotMatchException` is raised.

# API documentation

* **path_to_save**          `required` Your path to save the data.
* **tab**                   `required` Only table name without any enclosing characters like "[]".
* **schema**                `required` Only schema name without any enclosing characters like "[]". Default - "dbo".
* **query**                 `optional` Custom query to the source. If used with "where" condition within query, param "
  where" must not be specified.
* **where**                 `optional` Where clause to filter data, e.g. `date = '2011-02-02'`. Must be interchangeable
  with Spark SQL
* **jdbcUrl**               `required` JDBC Url to the source database.
* **mode**                  `optional` Must be set unless `useMerge` or `isChangeTrackingEnabled` is set to `true`.
  Default behaviour - Error is thrown if delta table exists.
* **numpart**               `optional` Number of partitions for Spark (to be used with splitby).
* **splitby**               `optional` Partition column, its type should be numeric, date, or timestamp.
* **calc_min_dt**           `optional` Lower bound of the time range (included) e.g. '2021-10-1'.
* **calc_max_dt**           `optional` Upper bound of the time range (excluded), e.g. '2021-10-10'.
* **dtflt**                 `optional` Column of type Date/Timestamp on the source. It's a column to apply a filter
  upon.
* **exec_date**             `optional` Day for which to perform the load, e.g. '2021-10-10'.
* **write_partitioned**     `optional` Should data be written in partitions, values: `true`, `false`.
* **partition_col**         `optional` Column on the source to use as partitioned column. If
  specified, `write_partitioned` must be `true`.
* **hive_db**               `optional` A database to create the table in, if no provided - tables creation part will be
  omitted.
* **hive_db_location**      `optional` A desired location of the hive_db, if doesn't exist. Default
  is `s3a://warehouse/`.
* **use_merge**             `optional` True or False. Inserts all missing records and updates found records. Does not
  delete rows. Viz https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge.
* **merge_keys**            `optional` Array of columns with unique values on the source table for use for upsert. If
  specified, `use_merge` must be `true`
* **generate_column**       `optional` If you wish to generate additional column on target, set to `true`.
* **generated_column_name** `optional` select column name for the generated column, if the name exists on the source -
  it will be overwritten.
* **generated_column_exp**  `optional` An expression to generate the column, for
  example `replace(cast(date as date), '-', '')`. Note that DATE type will be read as TIMESTAMP due to SPARK-39993.
* **generated_column_type** `optional` Datatype of the generated column. Viz SQL
  types [here](https://spark.apache.org/docs/latest/sql-ref-datatypes.html#supported-data-types).
* **force_partition**       `optional` Use to omit checks on dtflt == partition_col, values: `true`, `false`. Only if
  you know what you're doing!
* **timezone**              `optional` Timezone in which the source database stores its data. Data on target will be
  stored in UTC. Default - 'Europe/Kiev'.
* **change_tracking**       `optional` Indicates whether Change Tracking should be used to sync table,
  values: `true`, `false`. Default - `false`.
* **custom_ct**             `optional` Indicates whether custom procedures will be used handling multiple tables
  mirroring as the whole. Expects `parent_key`. Values: `true`, `false`. Default - `false`.
* **primary_key**           `optional` Primary key of the source table, e.g. `"id,[FilId]"`. If
  specified, `change_tracking` must be `true`.
* **parent_key**            `optional` The key of the parent table (in case of the child table) or
  primary_key of the table (in case of the parent table), e.g. `"id,[FilId]"`.
  If specified, `custom_ct` must be `true`.
* **zorderby_col**          `optional` Columns on which perform z-ordering,
  viz [docs](https://docs.delta.io/2.0.0/optimizations-oss.html#z-ordering-multi-dimensional-clustering). Note, the
  effectiveness of the locality drops with each extra column.
* **log_lvl**               `optional` Default log level is info. Log level may
  be `"off, error, warn, info, debug, trace, all"`.
* **log_spark_lvl**         `optional` Default log level is info. Log level may
  be `"ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"`.
* **ct_min_valid_version_query**  `optional` Custom query to get minimum valid Change Tracking version,
  e.g. `exec dbo.getCTMinValidVersionQuery ?`.
* **ct_min_valid_version_params** `optional` Parameters for `ct_min_valid_version_query`.
* **ct_current_version_query**      `optional` Custom query to get current Change Tracking version,
  e.g. `exec dbo.getCTCurrentVersion`.
* **ct_current_version_params**      `optional` Parameters for `ct_current_version_query`.
* **ct_changes_query**            `optional` custom query to get Change Tracking changes,
  e.g. `exec dbo.getCTChanges ?, ?`. Requires exactly two bigint params in order to get metadata.
* **ct_changes_query_params**     `optional` Parameters for `ct_changes_query`.
* **ct_window**     `optional` Window to be applied to the last change tracking version.
  If `current change tracking version` exceeds `last change tracking + ct_window` value than this value
  will be used instead of `current change tracking version`.
* **disable_platform_ingested_at**`optional` Set `true` if you want to disable autocreation
  column `_platform_ingested_at` on target.
* **log_retention_duration**      `optional` default 7 days,
  viz. https://docs.delta.io/latest/delta-batch.html#data-retention
* **deleted_file_retention_duration**      `optional` default 7 days,
  viz. https://docs.delta.io/latest/delta-batch.html#data-retention

### Reserved words for parameters

Arguments `ct_changes_query_params`, `ct_min_valid_version_params` and `ct_current_version_params` have reserved
parameters which have default behaviour:

- `defaultSQLTable` to generate and to pass mirrored object as `[schema].[table]`
- `queryCTLastVersion` to query CT version from the SQL server (current CT version of the delta table or determined as
  mentioned in the previous part)
- `queryCTCurrentVersion` to query current CT version from the SQL server (determined
  as `CHANGE_TRACKING_CURRENT_VERSION()` or from `ct_current_version_query` if provided)

# Run unit tests

`sbt test`
`sbt compile`
`sbt package`

# Scalastyle usage

`sbt scalastyle`
`sbt headerCreate`
`sbt scalafmt`
