CREATE TABLE union_a (year STRING, temp INT, quality INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '${WEATHER_DATA}' OVERWRITE INTO TABLE union_a;

CREATE TABLE union_b (year STRING, temp INT, quality INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '${WEATHER_DATA}' OVERWRITE INTO TABLE union_b;

SELECT c.* FROM (
  SELECT a.* FROM union_a a
  UNION ALL
  SELECT b.* FROM union_b b WHERE b.year NOT IN (SELECT year FROM union_a WHERE year = '1949')
) c;
