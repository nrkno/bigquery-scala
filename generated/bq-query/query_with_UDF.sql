CREATE TEMP FUNCTION half_TMP(input INT64) RETURNS FLOAT64 AS ((double_TMP(input) / 2));

CREATE TEMP FUNCTION double_TMP(input INT64) RETURNS INT64 AS ((input + input));

select half_TMP(1)