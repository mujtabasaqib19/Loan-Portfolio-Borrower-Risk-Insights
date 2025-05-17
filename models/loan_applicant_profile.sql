SELECT
    memberId,
    UPPER(residentialState) AS state,
    yearsEmployment,
    homeOwnership,
    annualIncome,
    incomeVerified,
    dtiRatio,
    lengthCreditHistory,
    revolvingUtilizationRate,
    numInquiries6Mon,

    -- Derived Categories
    CASE 
        WHEN annualIncome < 40000 THEN 'Low Income'
        WHEN annualIncome BETWEEN 40000 AND 100000 THEN 'Middle Income'
        ELSE 'High Income'
    END AS income_bracket,

    CASE 
        WHEN revolvingUtilizationRate < 30 THEN 'Healthy Utilization'
        WHEN revolvingUtilizationRate BETWEEN 30 AND 60 THEN 'Warning Zone'
        ELSE 'Over-utilized'
    END AS credit_utilization_band,

    CASE 
        WHEN yearsEmployment_numeric >= 5 THEN 'Stable Employment'
        WHEN yearsEmployment_numeric BETWEEN 2 AND 5 THEN 'Mid Employment'
        ELSE 'New Employment'
    END AS employment_class

FROM LOAN_DATASET.LOAN_ANALYTICS.STAGING_BORROWERS_PROD
