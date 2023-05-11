CREATE TEMP FUNCTION xxdouble_TMP(input INT64) RETURNS INT64 AS ((input + input));
CREATE TEMP FUNCTION half_TMP(input INT64) RETURNS FLOAT64 AS ((xxdouble_TMP(input) / 2));

select half_TMP(1)