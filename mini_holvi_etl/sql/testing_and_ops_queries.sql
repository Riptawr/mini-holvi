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
