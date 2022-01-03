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


-- 월별 채널별 유니크한 사용자 수 찾기 (INNER JOIN 간단한 예시)
SELECT TO_CHAR(ts.ts, 'YYYY-MM'),
       channel.channel,
       COUNT(DISTINCT channel.userid) MAU
FROM raw_data.user_session_channel channel
         JOIN raw_data.session_timestamp ts
              ON channel.sessionid = ts.sessionid
GROUP BY 1, 2
ORDER BY 1 DESC, 3
LIMIT 10;


-- vital 테이블 생성
DROP TABLE IF EXISTS vital;
CREATE TABLE vital
(
    UserID  int,
    VitalID int,
    Date    date,
    Weight  int
);
INSERT INTO vital
VALUES (100, 1, '2020-01-01', 75),
       (100, 3, '2020-01-02', 78),
       (101, 2, '2020-01-01', 90),
       (101, 4, '2020-01-02', 95);


-- alert 테이블 생성
DROP TABLE IF EXISTS alert;
CREATE TABLE alert
(
    AlertID   int,
    VitalID   int,
    AlertType varchar(32),
    Date      date,
    UserID    int
);
INSERT INTO alert
VALUES (1, 4, 'WeightIncrease', '2020-01-01', 101),
       (2, NULL, 'MissingVital', '2020-01-04', 100),
       (3, NULL, 'MissingVital', '2020-01-04', 101);


-- SELECT ALL TABLE
SELECT *
FROM vital;
SELECT *
FROM alert;

-- INNER JOIN
-- 1. 양쪽 테이블에서 매치가 되는 레코드들을 리턴
-- 2. 양쪽 테이블의 모든 필드가 채워진 상태로 리턴
SELECT *
FROM vital v
         JOIN alert a ON v.vitalID = a.vitalID;


-- LEFT JOIN
-- 왼쪽 테이블의 모든 레코드 + 오른쪽 테이블의 매칭되는 레코드 리턴
-- 오른쪽의 레코드 중에 왼쪽가 매칭되지 않는 필드의 경우 NULL로 채워짐
SELECT *
FROM vital v
         LEFT JOIN alert a ON v.vitalID = a.vitalID;


-- FULL JOIN (=OUTER JOIN)
-- 왼쪽, 오른쪽 모든 테이블의 레코드 리턴
-- 매칭되는 레코드의 필드는 모두 채워져 있고, 매칭되지 않은 레코드의 필드는 NULL로 채워짐.
SELECT *
FROM vital v
         FULL JOIN alert a ON v.vitalID = a.vitalID;


-- CROSS JOIN (=CARTESIAN JOIN)
SELECT *
FROM vital v
         CROSS JOIN alert a;


-- CROSS JOIN (=CARTESIAN JOIN)
SELECT *
FROM (
         SELECT vitalid
         FROM vital
     )
         CROSS JOIN (
    SELECT alertid -- 1,2,3
    FROM alert
);

-- SELF JOIN
SELECT *
FROM vital v1
         JOIN vital v2 ON v1.vitalID = v2.vitalID;