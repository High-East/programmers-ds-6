-- UNION은 중복을 제거함
SELECT 'donghee' as first_name, 'ko' as last_name
UNION
SELECT 'elon', 'musk'
UNION
SELECT 'donghee', 'ko';
/*
donghee,ko
elon,musk
 */

-- UNION ALL은 중복을 포함함
SELECT 'donghee' as first_name, 'ko' as last_name
UNION ALL
SELECT 'elon', 'musk'
UNION ALL
SELECT 'donghee', 'ko';
/*
donghee,ko
elon,musk
donghee,ko
 */