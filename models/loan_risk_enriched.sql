SELECT
    l.loanId,
    l.memberId,
    TRY_TO_DATE(l.date) AS loan_date,
    LOWER(l.purpose) AS loan_purpose,
    l.loanAmount,
    l.interestRate,
    l.term,
    l.monthlyPayment,
    l.grade,
    l.loanStatus,

    -- Individual Risk Levels by Grade
    CASE 
        WHEN l.grade = 'A' THEN 'Very Low Risk'
        WHEN l.grade = 'B' THEN 'Low Risk'
        WHEN l.grade = 'C' THEN 'Moderate Risk'
        WHEN l.grade = 'D' THEN 'High Risk'
        WHEN l.grade = 'E' THEN 'Very High Risk'
        WHEN l.grade = 'F' THEN 'Severe Risk'
        WHEN l.grade = 'G' THEN 'Extreme Risk'
        ELSE 'Unknown'
    END AS grade_risk_level,

    -- Numeric Score for Modeling
    CASE 
        WHEN l.grade = 'A' THEN 1.0
        WHEN l.grade = 'B' THEN 2.0
        WHEN l.grade = 'C' THEN 3.0
        WHEN l.grade = 'D' THEN 4.0
        WHEN l.grade = 'E' THEN 5.0
        WHEN l.grade = 'F' THEN 6.0
        WHEN l.grade = 'G' THEN 7.0
        ELSE 8.0
    END AS grade_score,

    -- Income Stress Score
    ROUND((l.monthlyPayment / NULLIF(b.annualIncome, 0)) * 100, 2) AS repayment_stress_pct,

    -- Label for Defaulted
    CASE 
        WHEN LOWER(l.loanStatus) IN ('charged off', 'default') THEN 1 ELSE 0
    END AS is_defaulted

FROM LOAN_DATASET.LOAN_ANALYTICS.STAGING_LOANS_PROD l
JOIN LOAN_DATASET.LOAN_ANALYTICS.STAGING_BORROWERS_PROD b
  ON l.memberId = b.memberId
