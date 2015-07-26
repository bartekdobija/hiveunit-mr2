SET hive.support.quoted.identifiers=none;

CREATE TABLE weather_regexp (year STRING, temp INT, quality INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH '${WEATHER_DATA}' OVERWRITE INTO TABLE weather_regexp;
SELECT a.`(year|temp)` FROM weather_regexp a;