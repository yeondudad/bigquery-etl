-- Bug 1572873: Daily User Statistics to power AMO stats pages.
WITH cd AS (
  SELECT
    submission_date,
    client_id,
    active_addons,
    app_version,
    country,
    locale,
    os
  FROM
    telemetry.clients_daily
  WHERE
    array_length(active_addons) > 0
    AND submission_date = @submission_date
),
unnested AS (
  SELECT
    cd.* EXCEPT (active_addons),
    addon.*
  FROM
    cd CROSS JOIN UNNEST(active_addons) AS addon
)

SELECT
  submission_date,
  addon_id,
  version AS addon_version,
  app_version,
  locale,
  country,
  os AS app_os,
  count(DISTINCT client_id) AS dau
FROM
  unnested
GROUP BY
  1, 2, 3, 4, 5, 6, 7
