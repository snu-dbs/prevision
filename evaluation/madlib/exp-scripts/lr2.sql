CREATE TABLE w_new AS (
SELECT 
	madlib.array_sub(
	(SELECT vec FROM w LIMIT 1),
	madlib.array_scalar_mult(
		madlib.matrix_vec_mult('XT','row=row_id, val=row_vec', (
		    SELECT ARRAY_AGG(1/1+EXP(-1*LEAST(
			    madlib.array_dot(X.row_vec, (SELECT vec FROM w LIMIT 1)), 100)) -
			       y.row_vec[1])
			  FROM X
			 INNER JOIN y ON X.row_id = y.row_id
		)), 0.0000001::double precision)) as vec
);

DROP TABLE w;
ALTER TABLE w_new RENAME TO w;
