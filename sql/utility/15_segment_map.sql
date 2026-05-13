-- ============================================================================
-- EPF Data Purge - Tablespace Segment Map (Diagnostic)
-- ============================================================================
-- Shows the complete physical layout of every segment in the DATA tablespace,
-- ordered by position in the datafile (highest block first). This tells you
-- exactly what is pinning the High Water Mark and why RESIZE cannot shrink
-- the datafile.
--
-- Best run AFTER indexes/constraints have been dropped (steps 4-6 of the
-- reclaim workflow) so the output matches what the MOVE phases would see.
-- If run before dropping indexes, index segments will appear too.
--
-- Compatible with both SQL*Plus and SQL Developer (Run as Script / F5).
-- Run as SYS / SYSDBA.
-- ============================================================================

-- ===== 1. Datafile(s) =====
SELECT 'DATAFILES IN ' || default_tablespace AS "=== Section ===" FROM dba_users WHERE username = 'OPPAYMENTS';

SELECT d.file_id,
       d.file_name,
       ROUND(d.bytes / 1024 / 1024 / 1024, 4) AS size_gb,
       ROUND(d.maxbytes / 1024 / 1024 / 1024, 4) AS max_gb,
       d.autoextensible
  FROM dba_data_files d
 WHERE d.tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')
 ORDER BY d.file_id;

-- ===== 2. HWM and free space =====
SELECT 'HWM AND FREE SPACE' AS "=== Section ===" FROM dual;

SELECT hwm.hwm_gb,
       used.total_used_gb,
       fr.free_gb
  FROM (SELECT ROUND(MAX(block_id + blocks) * b.bs / 1024/1024/1024, 6) AS hwm_gb
          FROM dba_extents,
               (SELECT TO_NUMBER(value) AS bs FROM v$parameter WHERE name = 'db_block_size') b
         WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')
         GROUP BY b.bs) hwm,
       (SELECT ROUND(SUM(bytes) / 1024/1024/1024, 4) AS total_used_gb
          FROM dba_extents
         WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')) used,
       (SELECT ROUND(SUM(bytes) / 1024/1024/1024, 4) AS free_gb
          FROM dba_free_space
         WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')) fr;

-- ===== 3. All segments ordered by physical position (highest first) =====
-- Segments near the top of the file are what PIN the HWM.
SELECT 'ALL SEGMENTS BY PHYSICAL POSITION (highest first)' AS "=== Section ===" FROM dual;

WITH params AS (
    SELECT (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS') AS data_ts,
           (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'db_block_size')  AS block_size
      FROM dual
),
file_info AS (
    SELECT MAX(d.bytes) / p.block_size AS file_blocks
      FROM dba_data_files d, params p
     WHERE d.tablespace_name = p.data_ts
     GROUP BY p.block_size
),
seg_extents AS (
    SELECT e.owner,
           e.segment_name,
           e.segment_type,
           e.partition_name,
           COUNT(*)                    AS extent_count,
           SUM(e.bytes)               AS total_bytes,
           MIN(e.block_id)            AS min_block_id,
           MAX(e.block_id + e.blocks) AS max_block_id
      FROM dba_extents e, params p
     WHERE e.tablespace_name = p.data_ts
     GROUP BY e.owner, e.segment_name, e.segment_type, e.partition_name
)
SELECT ROW_NUMBER() OVER (ORDER BY s.max_block_id DESC) AS "#",
       s.owner,
       s.segment_name,
       s.segment_type,
       s.partition_name,
       ROUND(s.total_bytes / 1024 / 1024, 2) AS size_mb,
       s.extent_count AS extents,
       s.min_block_id AS min_block,
       s.max_block_id AS max_block,
       CASE
           WHEN s.max_block_id >= f.file_blocks * 0.95 THEN '** TOP 5% **'
           WHEN s.max_block_id >= f.file_blocks * 0.90 THEN '* TOP 10% *'
           ELSE ''
       END AS hwm_position,
       ROUND(s.max_block_id / NULLIF(f.file_blocks, 0) * 100, 2) AS pct_of_file,
       CASE
           WHEN s.owner IN ('OPPAYMENTS', 'OP') THEN 'YES'
           ELSE 'no'
       END AS is_epf
  FROM seg_extents s, file_info f
 ORDER BY s.max_block_id DESC;

-- ===== 4. Summary by owner =====
SELECT 'SUMMARY BY OWNER' AS "=== Section ===" FROM dual;

SELECT owner,
       COUNT(DISTINCT segment_name || segment_type) AS segments,
       ROUND(SUM(bytes) / 1024 / 1024 / 1024, 4) AS total_gb,
       MAX(block_id + blocks) AS highest_block
  FROM dba_extents
 WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')
 GROUP BY owner
 ORDER BY highest_block DESC;

-- ===== 5. Summary by segment type =====
SELECT 'SUMMARY BY SEGMENT TYPE' AS "=== Section ===" FROM dual;

SELECT segment_type,
       COUNT(DISTINCT owner || '.' || segment_name) AS seg_count,
       ROUND(SUM(bytes) / 1024 / 1024 / 1024, 4) AS total_gb
  FROM dba_extents
 WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')
 GROUP BY segment_type
 ORDER BY total_gb DESC;

-- ===== 6. Top 20 largest segments =====
SELECT 'TOP 20 LARGEST SEGMENTS' AS "=== Section ===" FROM dual;

SELECT owner, segment_name, segment_type, size_mb, max_block
  FROM (
    SELECT owner,
           segment_name,
           segment_type,
           ROUND(SUM(bytes) / 1024 / 1024, 2) AS size_mb,
           MAX(block_id + blocks) AS max_block,
           ROW_NUMBER() OVER (ORDER BY SUM(bytes) DESC) AS rn
      FROM dba_extents
     WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')
     GROUP BY owner, segment_name, segment_type
  )
 WHERE rn <= 20
 ORDER BY rn;

-- ===== 7. Non-EPF segments that could block the resize =====
SELECT 'NON-EPF SEGMENTS (potential resize blockers)' AS "=== Section ===" FROM dual;

SELECT owner,
       segment_name,
       segment_type,
       ROUND(SUM(bytes) / 1024 / 1024, 2) AS size_mb,
       MIN(block_id) AS min_block,
       MAX(block_id + blocks) AS max_block
  FROM dba_extents
 WHERE tablespace_name = (SELECT default_tablespace FROM dba_users WHERE username = 'OPPAYMENTS')
   AND owner NOT IN ('OPPAYMENTS', 'OP')
 GROUP BY owner, segment_name, segment_type
 ORDER BY max_block DESC;
