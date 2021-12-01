-- Count MAU (Monthly Active User)
SELECT to_char(ts.ts, 'YYYY-MM'), COUNT(usc.userID) mau
FROM raw_data.session_timestamp ts
         JOIN raw_data.user_session_channel usc ON ts.sessionID = usc.sessionID
GROUP BY 1
ORDER BY 1
LIMIT 5