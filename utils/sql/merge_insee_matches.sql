CREATE SCHEMA IF NOT EXISTS oeci;

CREATE TABLE IF NOT EXISTS oeci.insee_deces_matches (
  ipp_ocr text PRIMARY KEY,
  nip text,
  insee_id text,
  date_deces date,
  probas_rf numeric,
  probas_nn numeric,
  upper integer,
  mean_proba numeric,
  updated_at timestamp without time zone DEFAULT now()
);

WITH ranked AS (
  SELECT
    r.nip,
    r.ipp_ocr,
    r.id AS insee_id,
    NULLIF(r.date_deces, '')::date AS date_deces,
    NULLIF(r.probas_rf, '')::numeric AS probas_rf,
    NULLIF(r.probas_nn, '')::numeric AS probas_nn,
    COALESCE(NULLIF(r.upper, '')::integer, 0) AS upper,
    (
      COALESCE(NULLIF(r.probas_rf, '')::numeric, 0) +
      COALESCE(NULLIF(r.probas_nn, '')::numeric, 0)
    ) / 2.0 AS mean_proba,
    ROW_NUMBER() OVER (
      PARTITION BY r.ipp_ocr
      ORDER BY
        COALESCE(NULLIF(r.upper, '')::integer, 0) DESC,
        (
          COALESCE(NULLIF(r.probas_rf, '')::numeric, 0) +
          COALESCE(NULLIF(r.probas_nn, '')::numeric, 0)
        ) / 2.0 DESC
    ) AS rn
  FROM oeci.insee_deces_matches_raw r
  WHERE r.ipp_ocr IS NOT NULL
    AND r.ipp_ocr <> ''
)
INSERT INTO oeci.insee_deces_matches (
  ipp_ocr, nip, insee_id, date_deces, probas_rf, probas_nn, upper, mean_proba, updated_at
)
SELECT
  ipp_ocr, nip, insee_id, date_deces, probas_rf, probas_nn, upper, mean_proba, now()
FROM ranked
WHERE rn = 1
ON CONFLICT (ipp_ocr) DO UPDATE
SET
  nip = EXCLUDED.nip,
  insee_id = EXCLUDED.insee_id,
  date_deces = EXCLUDED.date_deces,
  probas_rf = EXCLUDED.probas_rf,
  probas_nn = EXCLUDED.probas_nn,
  upper = EXCLUDED.upper,
  mean_proba = EXCLUDED.mean_proba,
  updated_at = now();

UPDATE oeci.patients_trackcare p
SET
  date_dc = m.date_deces,
  source_deces = 'INSEE_ML'
FROM oeci.insee_deces_matches m
WHERE p.ipp_ocr = m.ipp_ocr
  AND m.date_deces IS NOT NULL
  AND p.date_dc IS NULL;

