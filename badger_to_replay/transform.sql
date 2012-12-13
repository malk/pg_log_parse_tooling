COPY badger_log FROM '/tmp/badger.csv' WITH CSV;

UPDATE badger_log SET
  log_time = log_time - CAST(substring(message FROM E'\\d+.\\d* ms') AS interval),
  message = regexp_replace(message, E'^duration: \\d+.\\d* ms  ', '')
WHERE error_severity = 'LOG' AND message ~ E'^duration: \\d+.\\d* ms  ';

COPY (SELECT
        to_char(log_time, 'YYYY-MM-DD HH24:MI:SS.MS TZ'),
        user_name, database_name, process_id, connection_from,
        session_id, session_line_num, command_tag, session_start_time,
        virtual_transaction_id, transaction_id, error_severity,
        sql_state_code, message, detail, hint, internal_query,
        internal_query_pos, context, query, query_pos, location
      FROM badger_log ORDER BY log_time, session_line_num)
  TO '/tmp_replay.csv' WITH CSV;
