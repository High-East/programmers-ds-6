-- 월별 거래량 확인
SELECT LEFT(ts, 7), COUNT(1)
FROM raw_data.session_timestamp
GROUP BY 1;


-- 월별 거래량을 10000 기준으로 표시하기
SELECT LEFT(ts, 7), CASE WHEN COUNT(1) >= 10000 THEN 'HIGH' ELSE 'LOW' END
FROM raw_data.session_timestamp
GROUP BY 1;

-- 월별 거래량을 5000, 10000, 15000을 기준으로 1, 2, 3, 4구간으로 나누기
SELECT LEFT(ts, 7),
       CASE
           WHEN COUNT(1) >= 15000 THEN '4'
           WHEN COUNT(1) >= 10000 THEN '3'
           WHEN COUNT(1) >= 5000 THEN '2'
           ELSE '1'
           END
FROM raw_data.session_timestamp
GROUP BY 1
ORDER BY 2;
