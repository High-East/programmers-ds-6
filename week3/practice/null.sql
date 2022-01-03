-- SELECT ALL
SELECT *
FROM test_table;
/*
1
1
0
0
4
3
 */

-- SELECT NULL
SELECT *
FROM test_table
WHERE value is NULL;


-- CREATE TABLE with NOT NULL field
DROP TABLE IF EXISTS test_null;
CREATE TABLE test_null (
    value int NOT NULL
);


-- Invalid operation: Cannot insert a NULL value into column value
INSERT INTO test_null VALUES (NULL);


-- CREATE TABLE with boolean field
DROP TABLE IF EXISTS test_boolean;
CREATE TABLE test_boolean (
    value boolean
);

INSERT INTO test_boolean VALUES (True), (False), (True), (True), (True), (NULL);
SELECT * FROM test_boolean;
/*
true
false
true
true
true
NULL
 */

SELECT COUNT(1) FROM test_boolean WHERE value = False;  -- 1
SELECT COUNT(1) FROM test_boolean WHERE value is not True; -- 2


-- Boolean 연산
SELECT COUNT(NULL); -- 0
SELECT 1 / NULL; -- NULL
SELECT 0 + NULL; -- NULL


-- COALESCE, NULLIF
SELECT value FROM test_table;
/*
NULL
NULL
1
1
0
0
4
3
 */


-- Invalid operation: Divide by zero;
SELECT 100/value FROM test_table;


-- NULLIF: 두 수가 동일하면 NULL을 반환, 다르면 첫 번째 값을 반환
SELECT value, 100/NULLIF(value, 0)
FROM test_table;


-- COALESCE: NULL이 아닌 첫 번째 값을 반환
SELECT value, COALESCE(value, 1)
FROM test_table;