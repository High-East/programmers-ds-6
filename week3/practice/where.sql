-- SELECT
SELECT DISTINCT channel
FROM raw_data.user_session_channel;
/*
Organic
Google
Facebook
Naver
Youtube
Instagram
*/


-- WHERE ... IN
SELECT DISTINCT channel
FROM raw_data.user_session_channel
WHERE channel IN ('Google', 'Facebook');
/*
Facebook
Google
*/


-- WHERE ... NOT IN
SELECT DISTINCT channel
FROM raw_data.user_session_channel
WHERE channel NOT IN ('Google', 'Facebook');
/*
Naver
Youtube
Instagram
Organic
*/


-- WHERE ... LIKE
SELECT DISTINCT channel
FROM raw_data.user_session_channel
WHERE channel LIKE '%G%';
/*
G가 포함된 경우만 반환함.
Google
*/


-- WHERE ... ILIKE
SELECT DISTINCT channel
FROM raw_data.user_session_channel
WHERE channel ILIKE '%G%';
/*
g,G가 포함된 경우만 반환함.
----
Instagram
Organic
Google
*/

-- WHERE ... BETWEEN ... and
SELECT LEFT(ts, 10)
FROM raw_data.session_timestamp
WHERE ts BETWEEN '2019-05-01' and '2019-06-01'
LIMIT 10;
