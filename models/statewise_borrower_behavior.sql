SELECT
    UPPER(residentialState) AS state,

    COUNT(*) AS total_borrowers,
    ROUND(AVG(annualIncome), 2) AS avg_income,
    ROUND(AVG(lengthCreditHistory), 2) AS avg_credit_years,
    ROUND(AVG(revolvingUtilizationRate), 2) AS avg_utilization,
    ROUND(AVG(dtiRatio), 2) AS avg_dti,
    ROUND(AVG(numInquiries6Mon), 2) AS avg_recent_inquiries,
    ROUND(SUM(CASE WHEN dtiRatio > 25 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_dti_pct,
    ROUND(SUM(CASE WHEN revolvingUtilizationRate > 70 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS over_utilized_pct,
    ROUND(SUM(CASE WHEN numDerogatoryRec >= 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS derogatory_record_pct

FROM LOAN_ANALYTICS.STAGING_BORROWERS
GROUP BY UPPER(residentialState)
ORDER BY total_borrowers DESC
