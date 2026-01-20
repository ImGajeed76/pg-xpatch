-- pg_xpatch upgrade script: 0.1.0 -> 0.1.1
-- 
-- This upgrade fixes pg_dump/pg_restore support by ensuring
-- xpatch.table_config data is included in database dumps.

-- Tell pg_dump to include table_config data in dumps
-- This is the critical fix - without this, table configurations
-- are lost after pg_dump/pg_restore
SELECT pg_extension_config_dump('xpatch.table_config', '');
