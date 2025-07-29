SELECT DISTINCT
    pat.PAPMI_No AS ipp_ocr,
    pe.PAPER_ResidentNumber AS ipp_chu,
    pe.PAPER_Name AS nom,
    pe.PAPER_Name2 AS prenom,
    pat.PAPMI_DOB AS date_naissance,
    CASE
        WHEN pat.PAPMI_Sex_DR = 2 THEN 'M'
        WHEN pat.PAPMI_Sex_DR = 3 THEN 'F'
        ELSE 'I'
    END AS sexe,
    pat.PAPMI_Deceased_Date AS date_dc,
    'TKC' AS source_deces
FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_Person pe ON pe.PAPER_PAPMI_DR = pat.PAPMI_RowId1
WHERE pat.PAPMI_No IN ({patient_list})
