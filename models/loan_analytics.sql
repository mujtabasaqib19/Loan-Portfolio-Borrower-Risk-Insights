SELECT
    loanId,
    memberId,
    TRY_TO_DATE(date) AS loan_date,
    LOWER(purpose) AS loan_purpose,
    isJointApplication,
    loanAmount,
    term,
    interestRate,
    monthlyPayment,
    grade,
    loanStatus,

    -- New Calculated Metrics
    ROUND(loanAmount * (interestRate / 100) * term / 12, 2) AS total_interest_paid,
    CASE 
        WHEN interestRate < 10 THEN 'Low Rate'
        WHEN interestRate BETWEEN 10 AND 15 THEN 'Medium Rate'
        ELSE 'High Rate'
    END AS interest_rate_bucket,

    CASE 
        WHEN term <= 36 THEN 'Short Term'
        ELSE 'Long Term'
    END AS term_category,

    CASE 
        WHEN LOWER(loanStatus) IN ('charged off', 'default') THEN 1
        ELSE 0
    END AS is_defaulted,

    CASE 
        WHEN isJointApplication THEN 'Joint'
        ELSE 'Single'
    END AS application_type
FROM LOAN_DATASET.LOAN_ANALYTICS.STAGING_LOANS_PROD
