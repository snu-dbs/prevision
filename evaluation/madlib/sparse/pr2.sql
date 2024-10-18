/* Experiment with index is much faster */
/* Index names are garaunteed by `CREATE TABLE` statement of `pr_setup.sql`. */
\set postfix '_row_id_idx'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v USING HASH (row_id);
\set postfix '_row_id_idx1'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v (row_id);
\set postfix '_col_id_idx'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v (col_id);
\set postfix '_row_id_col_id_idx'
CREATE INDEX IF NOT EXISTS :v2:postfix ON v (row_id, col_id);

SELECT madlib.matrix_mult(
	'X', 'row=row_id, col=col_id, val=val', 
	'v', 'row=row_id, col=col_id, val=val',
	'v_new1', 'fmt=sparse');

CREATE TABLE v_new2 AS (
	SELECT row_num AS row_id, col_num AS col_id, val * 0.85 AS val
	FROM v_new1
);

SELECT madlib.matrix_add(
	'v_new2', 'row=row_id, col=col_id, val=val',
	'with_one', 'row=row_id, col=col_id, val=val',
	'v_new', 'row=row_id, col=col_id, val=val');

DROP TABLE v;
DROP TABLE v_new1;
DROP TABLE v_new2;
ALTER TABLE v_new RENAME TO v;

