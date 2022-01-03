-- session_timestamp 테이블
SELECT *
FROM raw_data.session_timestamp
LIMIT 10;


/*
세션이 가장 많이 생성된 시간대 찾기
EXTRACT 사용
 */
SELECT EXTRACT(HOUR FROM ts), COUNT(1) as session_count
FROM raw_data.session_timestamp
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;


-- user_session_channel 테이블
SELECT *
FROM raw_data.user_session_channel
LIMIT 10;


-- 가장 많이 사용된 채널의 세션 및 유저 개수
SELECT channel, COUNT(1) session_count, COUNT(DISTINCT userid) user_count
FROM raw_data.user_session_channel
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;


-- 가장 많은 세션을 생성한 사용자 ID 찾기
SELECT userid, COUNT(1)
FROM raw_data.user_session_channel
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;


-- 월별 채널별 유니크한 사용자 수 찾기
SELECT
       TO_CHAR(ts.ts, 'YYYY-MM'),
       channel.channel,
       COUNT(DISTINCT channel.userid) MAU
FROM raw_data.user_session_channel channel
JOIN raw_data.session_timestamp ts
    ON channel.sessionid = ts.sessionid
GROUP BY 1, 2
ORDER BY 1 DESC, 3
LIMIT 10;



