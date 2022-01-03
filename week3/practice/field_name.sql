-- Invalid operation: syntax error at or near "group"
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    group int primary key,
    "address" varchar(32)
);


-- Success
DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    "group"   int primary key,
    "address" varchar(32)
);

SELECT * FROM test;
