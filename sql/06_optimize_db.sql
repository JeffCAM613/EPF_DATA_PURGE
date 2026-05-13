-- ============================================================================
-- EPF Data Purge - Pre-Purge Database Optimization
-- ============================================================================
-- Prepares the database for bulk DELETE operations by:
--   1. Enlarging redo logs to 1 GB (eliminates 'log file switch' waits)
--   2. Gathering fresh optimizer statistics on OPPAYMENTS schema
--
-- Must be run AS SYSDBA.  Safe to run multiple times (idempotent).
-- Automatically reverts redo log changes on failure.
--
-- Usage:  @sql/06_optimize_db.sql
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET FEEDBACK OFF
SET VERIFY OFF

DECLARE
    -- Configuration
    c_target_log_size_mb  CONSTANT NUMBER := 1024;   -- 1 GB per redo log
    c_new_group_count     CONSTANT NUMBER := 4;      -- 4 new groups
    c_max_switch_attempts CONSTANT NUMBER := 20;
    c_stats_schema        CONSTANT VARCHAR2(30) := 'OPPAYMENTS';

    -- Working variables
    v_log_dir         VARCHAR2(513);
    v_existing_max_grp NUMBER;
    v_new_grp         NUMBER;
    v_small_count     NUMBER := 0;
    v_created_groups  SYS.ODCINUMBERLIST := SYS.ODCINUMBERLIST();
    v_status          VARCHAR2(20);
    v_current_grp     NUMBER;
    v_attempts        NUMBER;
    v_dropped_count   NUMBER := 0;
    v_msg             VARCHAR2(4000);
    v_old_redo_mb     NUMBER := 0;    -- total size of undersized redo logs

    -- Track orphaned file paths for deletion
    TYPE t_file_list IS TABLE OF VARCHAR2(513);
    v_orphan_files    t_file_list := t_file_list();

    -- For reporting
    TYPE t_log_rec IS RECORD (
        group#  NUMBER,
        size_mb NUMBER,
        status  VARCHAR2(20)
    );
    TYPE t_log_tab IS TABLE OF t_log_rec;
    v_before_logs t_log_tab := t_log_tab();
    v_after_logs  t_log_tab := t_log_tab();

    PROCEDURE log_msg(p_msg VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('[OPTIMIZE] ' || TO_CHAR(SYSDATE, 'HH24:MI:SS') || ' ' || p_msg);
    END;

    PROCEDURE report_redo_state(p_phase VARCHAR2) IS
    BEGIN
        log_msg('--- Redo Log State (' || p_phase || ') ---');
        FOR r IN (SELECT group#, bytes/1024/1024 AS size_mb, status
                  FROM v$log ORDER BY group#)
        LOOP
            log_msg('  Group ' || r.group# || ': ' ||
                    ROUND(r.size_mb) || ' MB  [' || r.status || ']');
        END LOOP;
        log_msg('---');
    END;

BEGIN
    log_msg('============================================================');
    log_msg('  EPF Pre-Purge Database Optimization');
    log_msg('============================================================');

    -- ======================================================================
    -- PHASE 1: REDO LOG ENLARGEMENT
    -- ======================================================================
    log_msg('');
    log_msg('PHASE 1: Redo Log Optimization');

    -- Check how many groups are below target size
    SELECT COUNT(*) INTO v_small_count
    FROM v$log
    WHERE bytes/1024/1024 < c_target_log_size_mb;

    IF v_small_count = 0 THEN
        log_msg('All redo log groups are already >= ' || c_target_log_size_mb || ' MB. Skipping.');
        report_redo_state('CURRENT');
    ELSE
        -- Calculate disk space impact
        SELECT NVL(SUM(bytes/1024/1024), 0) INTO v_old_redo_mb
        FROM v$log WHERE bytes/1024/1024 < c_target_log_size_mb;

        log_msg('Found ' || v_small_count || ' redo log group(s) below ' ||
                c_target_log_size_mb || ' MB. Enlarging...');
        log_msg('');
        log_msg('  Disk space impact:');
        log_msg('    New redo logs:     ' || c_new_group_count || ' x ' || c_target_log_size_mb || ' MB = '
            || (c_new_group_count * c_target_log_size_mb) || ' MB');
        log_msg('    Old redo logs:     ' || ROUND(v_old_redo_mb) || ' MB (will be deleted)');
        log_msg('    Peak extra space:  ~' || ROUND((c_new_group_count * c_target_log_size_mb) / 1024, 1)
            || ' GB (temporary, before old logs are removed)');
        log_msg('    Net change:        +' || ROUND((c_new_group_count * c_target_log_size_mb - v_old_redo_mb) / 1024, 1)
            || ' GB (permanent)');
        log_msg('');

        report_redo_state('BEFORE');

        -- Discover the redo log directory from existing log files
        SELECT SUBSTR(member, 1, INSTR(member, '/', -1))
        INTO v_log_dir
        FROM v$logfile
        WHERE ROWNUM = 1;

        -- If Windows-style path (backslash), try that
        IF v_log_dir IS NULL OR LENGTH(v_log_dir) < 2 THEN
            SELECT SUBSTR(member, 1, INSTR(member, '\', -1))
            INTO v_log_dir
            FROM v$logfile
            WHERE ROWNUM = 1;
        END IF;

        log_msg('Redo log directory: ' || v_log_dir);

        -- Find the highest existing group number
        SELECT MAX(group#) INTO v_existing_max_grp FROM v$log;

        -- Create new larger groups
        FOR i IN 1..c_new_group_count LOOP
            v_new_grp := v_existing_max_grp + i;
            BEGIN
                EXECUTE IMMEDIATE 'ALTER DATABASE ADD LOGFILE GROUP ' || v_new_grp ||
                    ' (''' || v_log_dir || 'G' || v_new_grp || '_1.log'') SIZE ' ||
                    c_target_log_size_mb || 'M';
                v_created_groups.EXTEND;
                v_created_groups(v_created_groups.COUNT) := v_new_grp;
                log_msg('Created redo log group ' || v_new_grp || ' (' ||
                        c_target_log_size_mb || ' MB)');
            EXCEPTION
                WHEN OTHERS THEN
                    log_msg('ERROR creating group ' || v_new_grp || ': ' || SQLERRM);
                    -- Revert: drop any groups we already created
                    log_msg('REVERTING: dropping newly created groups...');
                    FOR j IN 1..v_created_groups.COUNT LOOP
                        BEGIN
                            EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' ||
                                              v_created_groups(j);
                            log_msg('  Dropped group ' || v_created_groups(j));
                        EXCEPTION
                            WHEN OTHERS THEN
                                log_msg('  Could not drop group ' || v_created_groups(j) ||
                                        ': ' || SQLERRM);
                        END;
                    END LOOP;
                    log_msg('Redo log optimization ABORTED. No changes persisted.');
                    report_redo_state('AFTER REVERT');
                    GOTO skip_redo_drop;
            END;
        END LOOP;

        -- Switch log files to rotate into the new groups
        log_msg('Rotating log files into new groups...');
        FOR i IN 1..c_new_group_count + 2 LOOP
            EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
        END LOOP;

        -- Wait for checkpoint
        EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';

        -- Give Oracle a moment (cannot use DBMS_LOCK.SLEEP without grants)
        -- We'll just retry the drops with multiple checkpoint attempts

        -- Drop old small groups (capture member file paths first for cleanup)
        log_msg('Dropping old undersized groups...');
        FOR r IN (SELECT group#, bytes/1024/1024 AS size_mb
                  FROM v$log
                  WHERE bytes/1024/1024 < c_target_log_size_mb
                  ORDER BY group#)
        LOOP
            v_attempts := 0;
            LOOP
                v_attempts := v_attempts + 1;
                BEGIN
                    -- Check status first
                    SELECT status INTO v_status FROM v$log WHERE group# = r.group#;

                    IF v_status = 'CURRENT' THEN
                        EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
                        EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';
                        IF v_attempts >= c_max_switch_attempts THEN
                            log_msg('WARNING: Could not rotate away from group ' ||
                                    r.group# || ' after ' || v_attempts ||
                                    ' attempts. Keeping it.');
                            EXIT;
                        END IF;
                        CONTINUE;
                    END IF;

                    IF v_status = 'ACTIVE' THEN
                        EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';
                        IF v_attempts >= c_max_switch_attempts THEN
                            log_msg('WARNING: Group ' || r.group# ||
                                    ' still ACTIVE after ' || v_attempts ||
                                    ' checkpoints. Keeping it.');
                            EXIT;
                        END IF;
                        CONTINUE;
                    END IF;

                    -- INACTIVE — capture member paths, then drop
                    FOR m IN (SELECT member FROM v$logfile WHERE group# = r.group#) LOOP
                        v_orphan_files.EXTEND;
                        v_orphan_files(v_orphan_files.COUNT) := m.member;
                    END LOOP;
                    EXECUTE IMMEDIATE 'ALTER DATABASE DROP LOGFILE GROUP ' || r.group#;
                    v_dropped_count := v_dropped_count + 1;
                    log_msg('Dropped group ' || r.group# || ' (' ||
                            ROUND(r.size_mb) || ' MB)');
                    EXIT;

                EXCEPTION
                    WHEN OTHERS THEN
                        IF v_attempts >= c_max_switch_attempts THEN
                            log_msg('WARNING: Could not drop group ' || r.group# ||
                                    ': ' || SQLERRM || '. Keeping it.');
                            EXIT;
                        END IF;
                        EXECUTE IMMEDIATE 'ALTER SYSTEM SWITCH LOGFILE';
                        EXECUTE IMMEDIATE 'ALTER SYSTEM CHECKPOINT';
                END;
            END LOOP;
        END LOOP;

        log_msg('Redo optimization complete: created ' || v_created_groups.COUNT ||
                ' new groups, dropped ' || v_dropped_count || ' old groups.');
        report_redo_state('AFTER');

        -- Delete orphaned redo log files from disk
        -- Oracle DROP LOGFILE GROUP only removes the group from the control file
        -- but leaves the physical OS file on disk. We clean them up here.
        IF v_orphan_files.COUNT > 0 THEN
            log_msg('');
            log_msg('Cleaning up ' || v_orphan_files.COUNT || ' orphaned redo log file(s)...');
            BEGIN
                EXECUTE IMMEDIATE
                    'CREATE OR REPLACE DIRECTORY EPF_REDO_CLEANUP AS ''' ||
                    RTRIM(v_log_dir, '/\') || '''';

                FOR i IN 1..v_orphan_files.COUNT LOOP
                    DECLARE
                        v_filename VARCHAR2(513);
                    BEGIN
                        -- Extract just the filename from the full path
                        v_filename := SUBSTR(v_orphan_files(i),
                            GREATEST(NVL(INSTR(v_orphan_files(i), '/', -1), 0),
                                     NVL(INSTR(v_orphan_files(i), '\', -1), 0)) + 1);
                        UTL_FILE.FREMOVE('EPF_REDO_CLEANUP', v_filename);
                        log_msg('  Deleted: ' || v_orphan_files(i));
                    EXCEPTION
                        WHEN OTHERS THEN
                            log_msg('  WARNING: Could not delete ' || v_orphan_files(i)
                                || ': ' || SQLERRM);
                            log_msg('  Delete manually from OS to free disk space.');
                    END;
                END LOOP;

                BEGIN
                    EXECUTE IMMEDIATE 'DROP DIRECTORY EPF_REDO_CLEANUP';
                EXCEPTION
                    WHEN OTHERS THEN NULL;
                END;
            EXCEPTION
                WHEN OTHERS THEN
                    log_msg('WARNING: Could not create cleanup directory: ' || SQLERRM);
                    log_msg('Orphaned files must be deleted manually:');
                    FOR i IN 1..v_orphan_files.COUNT LOOP
                        log_msg('  ' || v_orphan_files(i));
                    END LOOP;
            END;
        END IF;
    END IF;

    <<skip_redo_drop>>

    -- ======================================================================
    -- PHASE 2: GATHER OPTIMIZER STATISTICS
    -- ======================================================================
    log_msg('');
    log_msg('PHASE 2: Gathering optimizer statistics for ' || c_stats_schema);

    BEGIN
        DBMS_STATS.GATHER_SCHEMA_STATS(
            ownname          => c_stats_schema,
            estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
            cascade          => TRUE,
            degree           => 4
        );
        log_msg('Statistics gathered successfully.');
    EXCEPTION
        WHEN OTHERS THEN
            log_msg('WARNING: Could not gather stats: ' || SQLERRM);
            log_msg('Purge will still work, but query plans may be sub-optimal.');
    END;

    -- ======================================================================
    -- DONE
    -- ======================================================================
    log_msg('');
    log_msg('============================================================');
    log_msg('  Database optimization complete');
    log_msg('============================================================');

END;
/
