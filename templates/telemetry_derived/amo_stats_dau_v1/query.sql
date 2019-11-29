-- Bug 1572873
--[keep] Daily users.
--[keep] Average Daily Installs (average of past two weeks).
--[keep] Daily users by add-on version.
--[keep] Daily users by application version.
--[keep] Daily users by application language.
--[add] Daily users by country.
--[keep] Daily users by platform.
--[drop?] Daily users by add-on status. This may be more useful as a stat for admins, to check on disable and blocking rates.

DECLARE target_date DATE DEFAULT '2019-11-21';

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
    AND submission_date = target_date
    AND sample_id = 42
  --LIMIT 20
), unnested AS (
  SELECT
    cd.* EXCEPT (active_addons),
    addon.*
  FROM
    cd CROSS JOIN UNNEST(active_addons) AS addon
), addons_dau AS (
  SELECT
    submission_date,
    addon_id,
    version as addon_version,
    app_version,
    locale,
    country,
    os AS app_os,
    count(distinct client_id) as dau
  FROM
    unnested
  GROUP BY
    1, 2, 3, 4, 5, 6, 7
  ORDER BY
    submission_date,
    addon_id,
    version,
    dau DESC
), installs AS (
  SELECT
    target_date AS install_date,
    addon.addon_id,
    -- and all the other junk above
    count(distinct client_id) as installs
  FROM
    telemetry.clients_daily
    CROSS JOIN UNNEST(active_addons) AS addon
  WHERE
    -- Wait two days for install data for a given day
    submission_date >= target_date
    AND submission_date < DATE_ADD(target_date, INTERVAL 2 DAY)
    -- Filter out bogus install dates
    AND install_day > 10000 -- before Firefox was a thing
    AND install_day < 25000 -- the distant future
    AND DATE_ADD('1970-01-01', INTERVAL install_day DAY) = target_date
  GROUP BY
    addon_id
), overall_dau AS (
SELECT
submission_date,
addon_id,
sum(dau) as dau
FROM addons_dau
GROUP BY
1, 2
)

select
  overall_dau.*,
  installs.installs
FROM
  overall_dau
  LEFT JOIN installs ON (
    overall_dau.addon_id = installs.addon_id
    AND overall_dau.submission_date = installs.install_date
  )
ORDER BY
dau DESC
