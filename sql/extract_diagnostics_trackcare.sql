SELECT DISTINCT
    pat.PAPMI_No AS ipp_ocr,
    diag.PROB_OnsetDate AS date_diagnostic,
    MRC.MRCID_Code AS code_cim,
    MRC.MRCID_Desc AS libelle_cim,
    diag.PROB_EntryStatus AS diagnostic_status,
    diag.PROB_Deleted AS diagnostic_deleted_flag,
    diag.PROB_CreateDate AS date_diagnostic_created_at,
    diag.PROB_UpdateDate AS date_diagnostic_updated_at,
    diag.PROB_EndDate AS date_diagnostic_end,
    q.Q01 AS premiere_consultation_flag,
    q.QUESDate AS date_consultation,
    q.QUESCreateDate AS date_consultation_created,
    'TKC' AS source

FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_Person pe ON pe.PAPER_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.PA_Problem diag ON diag.PROB_ParRef = pat.PAPMI_RowId1
LEFT JOIN SQLUser.MRC_ICDDx MRC ON diag.PROB_ICDCode_DR = MRC.MRCID_RowId
LEFT JOIN SQLUser.PA_Adm adm ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN questionnaire.QIUCTEMDSP q ON q.QUESPAAdmDR = adm.PAADM_RowID
WHERE pat.PAPMI_No IN ({patient_list})
AND diag.PROB_OnsetDate IS NOT NULL
