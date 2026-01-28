-- pg_xpatch upgrade script: 0.2.1 -> 0.3.0
-- 
-- Changes in 0.3.0:
-- - Removed version validation (order_by column no longer enforced to be strictly increasing)
-- - Version column is now treated as user data, _xp_seq handles internal ordering
-- 
-- No SQL schema changes required for this upgrade.

-- This file intentionally left mostly empty as there are no schema changes.
-- The behavioral changes are in the C code.
