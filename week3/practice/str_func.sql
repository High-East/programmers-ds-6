-- SELECT
SELECT ts
FROM raw_data.session_timestamp
LIMIT 5;
/*
2019-05-01 00:49:46.073000
2019-05-01 13:10:56.413000
2019-05-01 14:23:07.660000
2019-05-01 15:13:16.140000
2019-05-01 15:59:57.490000
 */


-- LEFT
SELECT LEFT(ts, 4)
FROM raw_data.session_timestamp
LIMIT 5;
/*
2019
2019
2019
2019
2019
 */


-- REPLACE
SELECT REPLACE(LEFT(ts, 4), '2019', '2020')
FROM raw_data.session_timestamp
LIMIT 5;
/*
2020
2020
2020
2020
2020
 */


-- UPPER
SELECT UPPER('aAbBcC123!@#');
/*
AABBCC123!@#
 */


-- LOWER
SELECT LOWER('aAbBcC123!@#');
/*
aabbcc123!@#
 */


-- LEN
SELECT ts, LEN(ts)
FROM raw_data.session_timestamp
LIMIT 5;
/*
소수점에 있는 0은 길이에 포함하지 않음.
23
23
22
22
22
 */


-- LPAD
SELECT LPAD('abcd', 10, '-');
/*
------abcd
 */


-- RPAD
SELECT RPAD('abcd', 10, '-');
/*
abcd------
 */


-- SUBSTRING
SELECT SUBSTRING('abcdefg', 1, 1); -- a
SELECT SUBSTRING('abcdefg', 1, 5); -- abcde
SELECT SUBSTRING('abcdefg', 3, 100); -- cdefg