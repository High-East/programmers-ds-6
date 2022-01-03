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


-- 요일에 따른 세션의 개수 세기
-- 일요일(0)부터 시작해서 토요일(6)까지 있음.
SELECT EXTRACT(DOW FROM ts), COUNT(1)
FROM raw_data.session_timestamp
GROUP BY 1
ORDER BY 2 DESC
    LIMIT 10;