SELECT * FROM UNNEST(ARRAY<STRUCT<num STRING, nesteds ARRAY<STRUCT<a STRING, b INT64, c INT64, d FLOAT64>>>>[("a", [("b", 10, 11, 12.5), ("c", 13, 14, 15.5)])])