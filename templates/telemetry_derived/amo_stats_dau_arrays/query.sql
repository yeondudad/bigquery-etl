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
    * EXCEPT (active_addons, version, os),
    version AS addon_version,
    os AS app_os,
  FROM
    cd CROSS JOIN UNNEST(active_addons) AS addon
),
--
per_addon_version AS (
  select submission_date, addon_id, array_agg(struct(key, value) order by value desc) AS dau_by_addon_version
  from (select submission_date, addon_id, addon_version AS key, count(distinct client_id) AS value from unnested 
        group by 1, 2, 3)
  group by 1, 2 ),
per_app_version AS (
  select submission_date, addon_id, array_agg(struct(key, value) order by value desc) AS dau_by_app_version
  from (select submission_date, addon_id, app_version AS key, count(distinct client_id) AS value from unnested
        group by 1, 2, 3)
  group by 1, 2 ),
per_locale AS (
  select submission_date, addon_id, array_agg(struct(key, value) order by value desc) AS dau_by_locale
  from (select submission_date, addon_id, locale AS key, count(distinct client_id) AS value from unnested 
        group by 1, 2, 3)
  group by 1, 2 ),
per_country AS (
  select submission_date, addon_id, array_agg(struct(key, value) order by value desc) AS dau_by_country
  from (select submission_date, addon_id, country AS key, count(distinct client_id) AS value from unnested 
        group by 1, 2, 3)
  group by 1, 2
),
per_app_os AS (
  select submission_date, addon_id, array_agg(struct(key, value) order by value desc) AS dau_by_app_os
  from (select submission_date, addon_id, app_os AS key, count(distinct client_id) AS value from unnested 
        group by 1, 2, 3)
  group by 1, 2
),
--
total_dau AS (
  select submission_date, addon_id, count(distinct client_id) AS dau from unnested 
  group by 1, 2
)
--
SELECT *
FROM total_dau
JOIN per_addon_version USING (submission_date, addon_id)
JOIN per_app_version USING (submission_date, addon_id)
JOIN per_locale USING (submission_date, addon_id)
JOIN per_country USING (submission_date, addon_id)
JOIN per_app_os USING (submission_date, addon_id)
