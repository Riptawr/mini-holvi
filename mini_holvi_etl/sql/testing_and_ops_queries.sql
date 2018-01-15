--check if procedure exists
SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
FROM information_schema.routines
    LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
WHERE routines.specific_schema='public'
ORDER BY routines.routine_name, parameters.ordinal_position;

SELECT DISTINCT trigger_name, event_object_table
  FROM information_schema.triggers
 WHERE trigger_schema NOT IN
       ('pg_catalog', 'information_schema');

SELECT EXISTS (
   SELECT 1
   FROM   information_schema.tables
   WHERE  table_schema = 'public'
   AND    table_name = 'table_name'
   );

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'table_name'
  AND pid <> pg_backend_pid();