SELECT
    LOWER(purpose) AS loan_purpose,
    COUNT(*) AS total_loans,
    ROUND(AVG(loanAmount), 2) AS avg_loan_amount,
    ROUND(AVG(interestRate), 2) AS avg_interest_rate,
    ROUND(SUM(CASE WHEN LOWER(loanStatus) IN ('charged off', 'default') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS default_rate_pct
FROM LOAN_ANALYTICS.STAGING_LOANS_PROD
GROUP BY LOWER(purpose)
ORDER BY total_loans DESC
