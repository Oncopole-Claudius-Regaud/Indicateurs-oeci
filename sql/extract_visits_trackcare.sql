SELECT
    pat.PAPMI_No AS ipp_ocr,
    adm.PAADM_ADMNo AS visit_episode_id,
    adm.PAADM_AdmDate AS visit_start_date,
    adm.PAADM_DischgDate AS visit_end_date,
    adm.PAADM_VisitStatus AS visit_status,
    loc.CTLOC_Desc AS visit_functional_unit,
    adm.PAADM_Type AS visit_type
FROM SQLUser.PA_PATMAS pat
JOIN SQLUser.PA_ADM adm ON adm.PAADM_PAPMI_DR = pat.PAPMI_RowId1
LEFT JOIN SQLUser.CT_Loc loc ON adm.PAADM_DepCode_DR = loc.CTLOC_RowID
WHERE pat.PAPMI_No IN ({patient_list})
