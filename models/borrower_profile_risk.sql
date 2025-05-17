SELECT
    memberId,
    UPPER(residentialState) AS state,
    yearsEmployment,
    yearsEmployment_numeric,
    homeOwnership,
    annualIncome,
    dtiRatio,
    lengthCreditHistory,
    revolvingUtilizationRate,
    numInquiries6Mon,
    numOpenCreditLines,
    numDerogatoryRec,
    numDelinquency2Years,

    -- Income Segmentation
    CASE 
        WHEN annualIncome < 40000 THEN 'Low Income'
        WHEN annualIncome BETWEEN 40000 AND 100000 THEN 'Middle Income'
        ELSE 'High Income'
    END AS income_group,

    -- Employment Category
    CASE 
        WHEN yearsEmployment_numeric < 1 THEN 'New Employee'
        WHEN yearsEmployment_numeric BETWEEN 1 AND 5 THEN 'Mid-Level'
        ELSE 'Stable Employment'
    END AS employment_status,

    -- DTI Risk Band
    CASE 
        WHEN dtiRatio < 10 THEN 'Low DTI'
        WHEN dtiRatio BETWEEN 10 AND 25 THEN 'Moderate DTI'
        ELSE 'High DTI'
    END AS dti_risk,

    -- Utilization Health
    CASE 
        WHEN revolvingUtilizationRate < 30 THEN 'Healthy Utilization'
        WHEN revolvingUtilizationRate BETWEEN 30 AND 70 THEN 'Warning Zone'
        ELSE 'Over-utilized'
    END AS utilization_category,

    -- Inquiry Level
    CASE 
        WHEN numInquiries6Mon = 0 THEN 'No Recent Inquiries'
        WHEN numInquiries6Mon BETWEEN 1 AND 2 THEN 'Light Activity'
        ELSE 'High Activity'
    END AS inquiry_band,

    -- Credit Experience
    CASE 
        WHEN lengthCreditHistory < 5 THEN 'Thin Credit File'
        WHEN lengthCreditHistory BETWEEN 5 AND 15 THEN 'Moderate History'
        ELSE 'Experienced Borrower'
    END AS credit_history_class,

    -- Composite Risk Profile
    CASE 
        WHEN dtiRatio > 25 OR revolvingUtilizationRate > 70 OR numDerogatoryRec >= 2 THEN 'High Risk'
        WHEN dtiRatio BETWEEN 15 AND 25 AND revolvingUtilizationRate BETWEEN 30 AND 70 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS overall_risk_profile

FROM LOAN_ANALYTICS.STAGING_BORROWERS
