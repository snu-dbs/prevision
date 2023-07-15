/* Experiment with index is much faster */
/* Index names are garaunteed by `CREATE TABLE` statement of `pr_setup.sql`. */
\set postfix '_row_id_idx'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v USING HASH (row_num);
\set postfix '_row_id_idx1'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v (row_num);
\set postfix '_col_id_idx'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v (col_num);
\set postfix '_row_id_col_id_idx'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v (row_num, col_num);

EXPLAIN ANALYZE
SELECT madlib.matrix_mult(
	'X', 'row=row_id, col=col_id, val=val', 
	'v', 'row=row_num, col=col_num, val=val',
	'v_new1', 'fmt=sparse');

EXPLAIN ANALYZE
CREATE TABLE v_new2 AS (
	SELECT row_num AS row_num, col_num AS col_num, val * 0.85 AS val
	FROM v_new1
);
/*
SELECT madlib.scalar_mult(
	'v_new1', 'row=row_num, col=col_num, val=val',
	0.85::DOUBLE PRECISION,
	'v_new2', 'fmt=sparse');
*/

EXPLAIN ANALYZE
SELECT madlib.matrix_add(
	'v_new2', 'row=row_num, col=col_num, val=val',
	'with_one', 'row=row_id, col=col_id, val=val',
	'v_new', 'fmt=sparse');

DROP TABLE v;
DROP TABLE v_new1;
DROP TABLE v_new2;
ALTER TABLE v_new RENAME TO v;

