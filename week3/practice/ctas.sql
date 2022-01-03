-- CTAS로 MAU table 만들기
CREATE TABLE monthly_active_user_summary AS
SELECT TO_CHAR(ts, 'YYYY-MM') AS month, COUNT(DISTINCT userid)
FROM raw_data.user_session_channel A
JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid
GROUP BY 1
ORDER BY 1 DESC;


-- SELECT
SELECT * FROM monthly_active_user_summary
LIMIT 10;


-- DROP TABLE
DROP TABLE monthly_active_user_summary;


