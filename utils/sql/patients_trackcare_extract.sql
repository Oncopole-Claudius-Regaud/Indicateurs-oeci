SELECT
  p.ipp_chu::text AS "NIP",
  UPPER(TRIM(p.nom))::text AS "SPA_NOM_USUEL",
  UPPER(TRIM(p.nom))::text AS "SPA_NOM_NAISS",
  UPPER(TRIM(p.prenom))::text AS "SPA_PRENOM_USAGE",
  UPPER(TRIM(p.prenom))::text AS "SPA_PRENOM_ETAT_CIVIL",
  p.date_naissance::date AS "SPA_DATE_NAISS",
  ''::text AS "SPA_CP_VILLE_NAISS",
  UPPER(COALESCE(TRIM(p.ville_naissance), ''))::text AS "SPA_VILLE_NAISS",
  ''::text AS "SPA_DEP_NAISS",
  'FRANCE'::text AS "SPA_LIBELLE_PAYS_NAISSANCE",
  CASE
    WHEN UPPER(LEFT(COALESCE(p.sexe, ''), 1)) = 'M' THEN 'M'
    WHEN UPPER(LEFT(COALESCE(p.sexe, ''), 1)) = 'F' THEN 'F'
    ELSE NULL
  END AS "SPA_SEXE",
  ''::text AS "DEP_HABITE",
  NULL::date AS "LAST_VISIT_DATE",
  p.ipp_ocr::text AS "IPP_OCR",
  p.date_dc::date AS "DATE_DC_TRACKCARE",
  p.source_deces::text AS "SOURCE_DECES_TRACKCARE"
FROM oeci.patients_trackcare p
WHERE p.ipp_chu IS NOT NULL
  AND p.nom IS NOT NULL
  AND p.prenom IS NOT NULL
  AND p.date_naissance IS NOT NULL
  AND UPPER(LEFT(COALESCE(p.sexe, ''), 1)) IN ('M', 'F')
  AND p.date_dc IS NULL;
