SELECT
    memberId,
    ROUND(
        (
            (dtiRatio * 0.3) +
            (revolvingUtilizationRate * 0.3) +
            (numDerogatoryRec * 10) +
            (numDelinquency2Years * 5) +
            (numInquiries6Mon * 2)
        ), 2
    ) AS fragility_score,
    CASE 
        WHEN (dtiRatio * 0.3 + revolvingUtilizationRate * 0.3 + numDerogatoryRec * 10 + numDelinquency2Years * 5 + numInquiries6Mon * 2) > 100 THEN 'Severe'
        WHEN (dtiRatio * 0.3 + revolvingUtilizationRate * 0.3 + numDerogatoryRec * 10 + numDelinquency2Years * 5 + numInquiries6Mon * 2) BETWEEN 60 AND 100 THEN 'High'
        WHEN (dtiRatio * 0.3 + revolvingUtilizationRate * 0.3 + numDerogatoryRec * 10 + numDelinquency2Years * 5 + numInquiries6Mon * 2) BETWEEN 30 AND 60 THEN 'Moderate'
        ELSE 'Low'
    END AS fragility_level
FROM LOAN_ANALYTICS.STAGING_BORROWERS
