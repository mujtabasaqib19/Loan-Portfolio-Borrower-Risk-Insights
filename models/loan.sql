SELECT
    loanId,
    memberId,
    TRY_TO_DATE(date) AS loan_date,
    LOWER(purpose) AS purpose,
    isJointApplication,
    loanAmount,
    term,
    interestRate,
    monthlyPayment,
    grade,
    loanStatus
FROM LOAN_DATASET.LOAN_ANALYTICS.STAGING_LOANS_PROD
