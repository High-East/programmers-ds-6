-- DROP and CREATE TABLE
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table(
    value int
);

-- INSERT INTO
INSERT INTO test_table
VALUES (NULL), (NULL), (1), (1), (0), (0), (4), (3);


SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table; -- 8
SELECT COUNT(value) FROM test_table; -- 6
SELECT COUNT(DISTINCT value) FROM test_table; -- 4