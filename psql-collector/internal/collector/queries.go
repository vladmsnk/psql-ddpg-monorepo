package collector

const (
	SelectTablesInfo = `
SELECT
    relid,
    relname AS tablename,
    n_live_tup,--the number of live tuples
    n_dead_tup,--the number of dead tuples
    seq_scan,
    idx_scan,
    n_tup_ins AS inserts,-- the number of inserts
    n_tup_upd AS updates,-- the number of updates
    n_tup_del AS deletes,-- the number of deletes
    last_vacuum,--the last vacuum times 
    last_autovacuum,
    last_analyze,--the last analyze times 
    last_autoanalyze
FROM
    pg_stat_user_tables
WHERE
    n_live_tup + seq_scan + idx_scan > 0
ORDER BY
    n_live_tup DESC,
    seq_scan + idx_scan DESC;
`

	SelectSharedBufferHitRate = `
SELECT
    sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)) AS ratio
FROM pg_statio_user_tables;
`

	SelectQueryTypesDistribution = `
SELECT
  query_type,
  count(*) AS total
FROM (
  SELECT
    CASE
      WHEN query LIKE 'INSERT%' THEN 'INSERT'
      WHEN query LIKE 'UPDATE%' THEN 'UPDATE'
      WHEN query LIKE 'DELETE%' THEN 'DELETE'
      WHEN query LIKE 'SELECT%' THEN 'SELECT'
      ELSE 'OTHER'
    END AS query_type
  FROM pg_stat_statements
) AS categorized_queries
GROUP BY query_type
ORDER BY total DESC;
`

	SelectLockingInformation = `
SELECT
  bl.pid AS blocked_pid,
  a.usename AS blocked_user,
  ka.query AS blocking_query,
  now() - ka.query_start AS query_time,
  kl.pid AS blocking_pid,
  ka.usename AS blocking_user,
  a.query AS blocked_query
FROM pg_catalog.pg_locks bl
JOIN pg_catalog.pg_stat_activity a ON a.pid = bl.pid
JOIN pg_catalog.pg_locks kl ON kl.transactionid = bl.transactionid AND kl.pid != bl.pid
JOIN pg_catalog.pg_stat_activity ka ON ka.pid = kl.pid
WHERE NOT bl.granted;
`

	SelectWalWriteAndFlushStat = `
SELECT
    checkpoints_timed,
    checkpoints_req,
    checkpoint_write_time,  -- Total time spent in writing data during checkpoints
    checkpoint_sync_time,   -- Total time spent in syncing data during checkpoints
    buffers_checkpoint,     -- Number of buffers written during checkpoints
    buffers_clean,
    maxwritten_clean,
    buffers_backend,        -- Number of buffers written directly by a backend
    buffers_backend_fsync,  -- Number of fsync calls by backends (since PostgreSQL 9.6)
    buffers_alloc           -- Number of buffers allocated
FROM pg_stat_bgwriter;
`

	SelectTablesBloat = `
WITH constants AS (
-- define some constants for sizes of things
-- for reference down the query and easy maintenance

SELECT
	current_setting('block_size')::numeric AS bs,
	23 AS hdr, 
	8 AS ma
),
no_stats AS (
-- screen out table who have attributes
-- which dont have stats, such as JSON

SELECT 
	table_schema,
	table_name,
	n_live_tup::numeric as est_rows,
	pg_table_size(relid)::numeric as table_size
FROM information_schema.columns
	  JOIN pg_stat_user_tables as psut
		   ON table_schema = psut.schemaname
			   AND table_name = psut.relname
	  LEFT OUTER JOIN pg_stats
					  ON table_schema = pg_stats.schemaname
						  AND table_name = pg_stats.tablename
						  AND column_name = attname
WHERE attname IS NULL
AND table_schema NOT IN ('pg_catalog', 'information_schema')
GROUP BY table_schema, table_name, relid, n_live_tup
 ),
null_headers AS (
-- calculate null header sizes
-- omitting tables which dont have complete stats
-- and attributes which aren't visible

SELECT
	 hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
	 SUM((1-null_frac)*avg_width) as datawidth,
	 MAX(null_frac) as maxfracsum,
	 schemaname,
	 tablename,
	 hdr, ma, bs
FROM pg_stats CROSS JOIN constants
		   LEFT OUTER JOIN no_stats
						   ON schemaname = no_stats.table_schema
							   AND tablename = no_stats.table_name
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
AND no_stats.table_name IS NULL
AND EXISTS ( SELECT 1
			FROM information_schema.columns
			WHERE schemaname = columns.table_schema
			  AND tablename = columns.table_name )
GROUP BY schemaname, tablename, hdr, ma, bs
),
data_headers AS (
-- estimate header and row size

SELECT 
	ma, 
	bs, 
	hdr, 
	schemaname, 
	tablename,
	(datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
	(maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
FROM null_headers
),
table_estimates AS (
-- make estimates of how large the table should be
-- based on row and page size

SELECT schemaname, tablename, bs,
	reltuples::numeric as est_rows, relpages * bs as table_bytes,
	CEIL((reltuples*
		  (datahdr + nullhdr2 + 4 + ma -
		   (CASE WHEN datahdr%ma=0
					 THEN ma ELSE datahdr%ma END)
			  )/(bs-20))) * bs AS expected_bytes,
	reltoastrelid
FROM data_headers
	  JOIN pg_class ON tablename = relname
	  JOIN pg_namespace ON relnamespace = pg_namespace.oid
 AND schemaname = nspname
WHERE pg_class.relkind = 'r'
),
estimates_with_toast AS (
-- add in estimated TOAST table sizes
-- estimate based on 4 toast tuples per page because we dont have
-- anything better.  also append the no_data tables

SELECT schemaname, tablename,
	TRUE as can_estimate,
	est_rows,
	table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes,
	expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes
FROM table_estimates LEFT OUTER JOIN pg_class as toast ON table_estimates.reltoastrelid = toast.oid AND toast.relkind = 't'
),

table_estimates_plus AS (
-- add some extra metadata to the table data
-- and calculations to be reused
-- including whether we cant estimate it
-- or whether we think it might be compressed

SELECT current_database() as databasename,
	schemaname, tablename, can_estimate,
	est_rows,
	CASE WHEN table_bytes > 0
			 THEN table_bytes::NUMERIC
		 ELSE NULL::NUMERIC END
					   AS table_bytes,
	CASE WHEN expected_bytes > 0
			 THEN expected_bytes::NUMERIC
		 ELSE NULL::NUMERIC END
					   AS expected_bytes,
	CASE WHEN expected_bytes > 0 AND table_bytes > 0
		AND expected_bytes <= table_bytes
			 THEN (table_bytes - expected_bytes)::NUMERIC
		 ELSE 0::NUMERIC END AS bloat_bytes
FROM estimates_with_toast
UNION ALL
SELECT current_database() as databasename,
	table_schema, table_name, FALSE,
	est_rows, table_size,
	NULL::NUMERIC, NULL::NUMERIC
FROM no_stats
),
bloat_data AS (
-- do final math calculations and formatting

select current_database() as databasename,
	schemaname, tablename, can_estimate,
	table_bytes, round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
	expected_bytes, round(expected_bytes/(1024^2)::NUMERIC,3) as expected_mb,
	round(bloat_bytes*100/table_bytes) as pct_bloat,
	round(bloat_bytes/(1024::NUMERIC^2),2) as mb_bloat,
	table_bytes, expected_bytes, est_rows
FROM table_estimates_plus
)
-- filter output for bloated tables
SELECT 
	tablename,
	est_rows,
	pct_bloat,	--bloat in percent
	mb_bloat, 	--bloat in megabytes
	table_mb    --size of table in megabytes
FROM bloat_data
WHERE can_estimate is true

-- this where clause defines which tables actually appear in the bloat chart
-- example below filters for tables which are either 50%
-- bloated and more than 20mb in size, or more than 25%
-- bloated and more than 4GB in size
-- WHERE ( pct_bloat >= 50 AND mb_bloat >= 10)
--    OR ( pct_bloat >= 25 AND mb_bloat >= 1000)

ORDER BY mb_bloat DESC;
`

	SelectIndexesBloat = `
-- btree index stats query
-- estimates bloat for btree indexes
WITH btree_index_atts AS (

SELECT nspname, 
	indexclass.relname as index_name, 
	indexclass.reltuples, 
	indexclass.relpages, 
	indrelid, indexrelid,
	indexclass.relam,
	tableclass.relname as tablename,
	regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
	indexrelid as index_oid
FROM pg_index
JOIN pg_class AS indexclass ON pg_index.indexrelid = indexclass.oid
JOIN pg_class AS tableclass ON pg_index.indrelid = tableclass.oid
JOIN pg_namespace ON pg_namespace.oid = indexclass.relnamespace
JOIN pg_am ON indexclass.relam = pg_am.oid
WHERE pg_am.amname = 'btree' and indexclass.relpages > 0
	 AND nspname NOT IN ('pg_catalog','information_schema')
), index_item_sizes AS (

SELECT
ind_atts.nspname, ind_atts.index_name, 
ind_atts.reltuples, ind_atts.relpages, ind_atts.relam,
indrelid AS table_oid, index_oid,
current_setting('block_size')::numeric AS bs,
8 AS maxalign,
24 AS pagehdr,
CASE WHEN max(coalesce(pg_stats.null_frac,0)) = 0
	THEN 2
	ELSE 6
END AS index_tuple_hdr,
sum( (1-coalesce(pg_stats.null_frac, 0)) * coalesce(pg_stats.avg_width, 1024) ) AS nulldatawidth
FROM pg_attribute
JOIN btree_index_atts AS ind_atts ON pg_attribute.attrelid = ind_atts.indexrelid AND pg_attribute.attnum = ind_atts.attnum
JOIN pg_stats ON pg_stats.schemaname = ind_atts.nspname
	  -- stats for regular index columns
	  AND ( (pg_stats.tablename = ind_atts.tablename AND pg_stats.attname = pg_catalog.pg_get_indexdef(pg_attribute.attrelid, pg_attribute.attnum, TRUE)) 
	  -- stats for functional indexes
	  OR   (pg_stats.tablename = ind_atts.index_name AND pg_stats.attname = pg_attribute.attname))
WHERE pg_attribute.attnum > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), index_aligned_est AS (

SELECT maxalign, bs, nspname, index_name, reltuples,
	relpages, relam, table_oid, index_oid,
	coalesce (
		ceil (
			reltuples * ( 6 
				+ maxalign 
				- CASE
					WHEN index_tuple_hdr%maxalign = 0 THEN maxalign
					ELSE index_tuple_hdr%maxalign
				  END
				+ nulldatawidth 
				+ maxalign 
				- CASE /* Add padding to the data to align on MAXALIGN */
					WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
					ELSE nulldatawidth::integer%maxalign
				  END
			)::numeric 
		  / ( bs - pagehdr::NUMERIC )
		  +1 )
	 , 0 )
  as expected
FROM index_item_sizes
), raw_bloat AS (

SELECT current_database() as dbname, nspname, pg_class.relname AS table_name, index_name,
	bs*(index_aligned_est.relpages)::bigint AS totalbytes, expected,
	CASE
		WHEN index_aligned_est.relpages <= expected 
			THEN 0
			ELSE bs*(index_aligned_est.relpages-expected)::bigint 
		END AS wastedbytes,
	CASE
		WHEN index_aligned_est.relpages <= expected
			THEN 0 
			ELSE bs*(index_aligned_est.relpages-expected)::bigint * 100 / (bs*(index_aligned_est.relpages)::bigint) 
		END AS realbloat,
	pg_relation_size(index_aligned_est.table_oid) as table_bytes,
	stat.idx_scan as index_scans
FROM index_aligned_est
JOIN pg_class ON pg_class.oid=index_aligned_est.table_oid
JOIN pg_stat_user_indexes AS stat ON index_aligned_est.index_oid = stat.indexrelid
),
format_bloat AS (

SELECT dbname as database_name, nspname as schema_name, table_name, index_name,
        round(realbloat) as bloat_pct, round(wastedbytes/(1024^2)::NUMERIC) as bloat_mb,
        round(totalbytes/(1024^2)::NUMERIC,3) as index_mb,
        round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
        index_scans
FROM raw_bloat
)

-- final query outputting the bloated indexes
-- change the where and order by to change
-- what shows up as bloated

SELECT
	table_name,
	index_name, 
	bloat_pct, 
	bloat_mb, 
	index_mb, 
	table_mb, 
	index_scans
FROM format_bloat
ORDER BY bloat_pct DESC;
`

	SelectDatabaseStat = `
SELECT
     xact_commit --number of transactions that have been committed
     ,xact_rollback
     ,blks_read
     ,blks_hit
     ,tup_returned
     ,tup_fetched
     ,tup_inserted
     ,tup_updated
     ,tup_deleted
     ,conflicts
     ,temp_files
     ,temp_bytes
     ,deadlocks
     ,blk_read_time
     ,blk_write_time
     ,active_time
     ,idle_in_transaction_time
FROM pg_stat_database 
where datname = $1;
`
)
