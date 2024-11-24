SELECT madlib.matrix_mult('X','row=row_id, val=row_vec','H','row=row_id, val=row_vec, trans=True','XHT');
SELECT madlib.matrix_mult('H','row=row_id, val=row_vec','H','row=row_id, val=row_vec, trans=True','HHT');
SELECT madlib.matrix_mult('W','row=row_id, val=row_vec','HHT','row=row_id, val=row_vec','WHHT');
CREATE TABLE WHHT_INV AS (
    SELECT row_id, madlib.array_pow(
	row_vec, -1.0::DOUBLE PRECISION) row_vec FROM WHHT
);
SELECT madlib.matrix_elem_mult('XHT','row=row_id, val=row_vec','WHHT_INV','row=row_id, val=row_vec','XHT_WHHT_INV');
SELECT madlib.matrix_elem_mult('W','row=row_id, val=row_vec','XHT_WHHT_INV','row=row_id, val=row_vec','W_NEW');
DROP TABLE W;
DROP TABLE XHT;
DROP TABLE HHT;
DROP TABLE WHHT;
DROP TABLE WHHT_INV;
DROP TABLE XHT_WHHT_INV;
ALTER TABLE W_NEW RENAME TO W;

SELECT madlib.matrix_mult('W','row=row_id, val=row_vec, trans=True','X','row=row_id, val=row_vec','WTX');
SELECT madlib.matrix_mult('W','row=row_id, val=row_vec, trans=True','W','row=row_id, val=row_vec','WTW');
SELECT madlib.matrix_mult('WTW','row=row_id, val=row_vec','H','row=row_id, val=row_vec','WTWH');
CREATE TABLE WTWH_INV AS (
    SELECT row_id, madlib.array_pow(
	row_vec, -1.0::DOUBLE PRECISION) row_vec FROM WTWH
);
SELECT madlib.matrix_elem_mult('WTX','row=row_id, val=row_vec','WTWH_INV','row=row_id, val=row_vec','WTX_WTWH_INV');
SELECT madlib.matrix_elem_mult('H','row=row_id, val=row_vec','WTX_WTWH_INV','row=row_id, val=row_vec','H_NEW');
DROP TABLE H;
DROP TABLE WTX;
DROP TABLE WTW;
DROP TABLE WTWH;
DROP TABLE WTWH_INV;
DROP TABLE WTX_WTWH_INV;
ALTER TABLE H_NEW RENAME TO H;

