SELECT
    pat.PAPMI_No AS ipp_ocr,
    med.MED_StartDate AS date_debut_traitement,
    med.MED_EndDate AS date_fin_traitement,
    gen.PHCGE_Code AS dci_code,
    gen.PHCGE_Name AS dci_libelle,
    df.PHCDF_Description AS forme_libelle,
    'TKC' AS source
FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_Adm adm ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
JOIN SQLUser.MR_Adm mradm ON mradm.MRADM_ADM_DR = adm.PAADM_RowID
JOIN SQLUser.MR_Medication med ON med.MED_ParRef = mradm.MRADM_RowId
LEFT JOIN SQLUser.PHC_Generic gen ON gen.PHCGE_RowId = med.MED_Generic_DR
LEFT JOIN SQLUser.PHC_DrgForm df ON df.PHCDF_RowId = med.MED_DrgForm_DR
WHERE pat.PAPMI_No IN ('201903845', '201508702', '202002808')
AND med.MED_StartDate IS NOT NULL
