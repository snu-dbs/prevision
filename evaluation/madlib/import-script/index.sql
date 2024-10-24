CREATE INDEX ON mat_80Mx100_dense(row_id);
CREATE INDEX ON mat_80Mx10_dense(row_id);
CREATE INDEX ON	mat_80Mx1_dense(row_id);

CREATE INDEX ON mat_80Mx100_dense USING HASH (row_id);
CREATE INDEX ON mat_80Mx10_dense USING HASH (row_id);
CREATE INDEX ON	mat_80Mx1_dense USING HASH (row_id);

CREATE INDEX ON mat_40Mx100_dense(row_id);
CREATE INDEX ON mat_40Mx10_dense(row_id);
CREATE INDEX ON mat_40Mx1_dense(row_id);

CREATE INDEX ON mat_40Mx100_dense USING HASH (row_id);
CREATE INDEX ON mat_40Mx10_dense USING HASH (row_id);
CREATE INDEX ON mat_40Mx1_dense USING HASH (row_id);

CREATE INDEX ON mat_20Mx100_dense(row_id);
CREATE INDEX ON mat_20Mx10_dense(row_id);
CREATE INDEX ON mat_20Mx1_dense(row_id);

CREATE INDEX ON mat_20Mx100_dense USING HASH (row_id);
CREATE INDEX ON mat_20Mx10_dense USING HASH (row_id);
CREATE INDEX ON mat_20Mx1_dense USING HASH (row_id);

CREATE INDEX ON mat_10Mx100_dense(row_id);
CREATE INDEX ON mat_10Mx10_dense(row_id);
CREATE INDEX ON mat_10Mx1_dense(row_id);

CREATE INDEX ON mat_10Mx100_dense USING HASH (row_id);
CREATE INDEX ON mat_10Mx10_dense USING HASH (row_id);
CREATE INDEX ON mat_10Mx1_dense USING HASH (row_id);

CREATE INDEX ON mat_10x100_dense(row_id);
CREATE INDEX ON mat_10x100_dense USING HASH (row_id);
