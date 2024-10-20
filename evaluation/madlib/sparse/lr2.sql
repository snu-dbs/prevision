/* Experiment with index is much faster */
/* Index names are garaunteed by `CREATE TABLE` statement of `lr_setup.sql`. */
CREATE INDEX IF NOT EXISTS w_row_id_idx ON w USING HASH (row_id);
CREATE INDEX IF NOT EXISTS w_row_id_idx1 ON w (row_id);
CREATE INDEX IF NOT EXISTS w_col_id_idx ON w (col_id);
CREATE INDEX IF NOT EXISTS w_row_id_col_id_idx ON w (row_id, col_id);

SELECT madlib.matrix_mult(
	'X', 'row=row_id, col=col_id, val=val', 
	'w', 'row=row_id, col=col_id, val=val',
	'Xw', 'fmt=sparse');

-- populate cells
CREATE UNIQUE INDEX ON Xw(row_num);

INSERT INTO Xw(row_num, col_num, val)
SELECT generate_series(1, 400000000), 1, 0
ON CONFLICT (row_num) DO NOTHING;

CREATE TABLE sigmoid AS (
	SELECT row_num, col_num, (1/(1+EXP(-1*val))) as val
	FROM Xw
);

select madlib.matrix_sub(
	'sigmoid', 'row=row_num, col=col_num, val=val', 
	'y', 'row=row_id, col=col_id, val=val', 
	'ydiff', 'row=row_id, col=col_id, val=val, fmt=sparse');

SELECT madlib.matrix_mult(
	'XT','row=row_id, col=col_id, val=val',
	'ydiff', 'row=row_id, col=col_id, val=val',
	'w_rhs_1', 'fmt=sparse');

CREATE TABLE w_rhs AS (
	SELECT row_num AS row_id, col_num AS col_id, val * 0.0000001::double precision as val
	FROM w_rhs_1
);

SELECT madlib.matrix_sub(
	'w', 'row=row_id, col=col_id, val=val',
	'w_rhs', 'row=row_id, col=col_id, val=val',
	'w_new', 'row=row_id, col=col_id, val=val');

DROP TABLE Xw;
DROP TABLE sigmoid;
DROP TABLE ydiff;
DROP TABLE w_rhs_1;
DROP TABLE w_rhs;

DROP TABLE w;
ALTER TABLE w_new RENAME TO w;

