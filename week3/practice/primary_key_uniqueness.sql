-- DROP & CREATE TABLE
DROP TABLE IF EXISTS test_channel;
CREATE TABLE test_channel
(
    channel     varchar(16) primary key,
    description varchar(32) default 'test'
);


-- INSERT INTO
INSERT INTO test_channel
VALUES ('FACEBOOK', 'test'),
       ('GOOGLE', 'test');

SELECT *
FROM test_channel;

INSERT INTO test_channel
VALUES ('FACEBOOK'),
       ('GOOGLE');

SELECT *
FROM test_channel;
/*
Redshift는 primary key 보장이 안됨.
----
FACEBOOK,test
GOOGLE,test
FACEBOOK,test
GOOGLE,test
 */


-- 필드 이름 변경하기 (channel에서 new_channel로 변경)
ALTER TABLE test_channel
    RENAME channel to new_channel;


/*
 중복 레코드 확인하기
 아래 두 쿼리의 결과가 다르면 중복 레코드가 존재하는 것.
 */
SELECT COUNT(1)
FROM test_channel;

SELECT COUNT(1) FROM(
    SELECT DISTINCT * FROM test_channel
);


-- primary key uniqueness 확인하기
SELECT new_channel, COUNT(1)
FROM test_channel
GROUP BY 1
ORDER BY 2 DESC
;


-- 타임스탬프 필드를 이용해서 데이터의 업데이트를 주기적으로 확인하는 것은 좋은 습관
SELECT MAX(ts), MIN(ts)
FROM raw_data.session_timestamp;