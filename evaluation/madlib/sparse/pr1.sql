CREATE TABLE with_one AS (
	SELECT generate_series(1, :v3) AS row_id, 1 AS col_id, ((1 - 0.85) / :v3) AS val
);
