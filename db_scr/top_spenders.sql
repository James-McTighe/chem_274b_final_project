SELECT 
    D.account_id, 
    COALESCE(SUM(CASE
                    WHEN T.amount < 0 THEN -T.amount 
                    ELSE 0
                 END), 0) as total_outgoing_transactions
FROM user_data D LEFT JOIN transactions T
ON D.account_id = T.account_id
GROUP BY D.account_id
ORDER BY 
    total_outgoing_transactions DESC, 
    D.account_id ASC
LIMIT ?;