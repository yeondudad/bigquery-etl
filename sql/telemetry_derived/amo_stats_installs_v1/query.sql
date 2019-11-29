-- Bug 1572873: Daily Install Statistics to power AMO stats pages.
-- This query looks forward in time by two days to capture
-- installs reported after some delay. It should therefore
-- be run after at least that much time has passed.
DECLARE latency INTEGER DEFAULT 2;

SELECT
  @submission_date AS install_date,
  addon.addon_id,
  count(distinct client_id) as installs
FROM
  telemetry.clients_daily
  CROSS JOIN UNNEST(active_addons) AS addon
WHERE
  -- Wait `latency` days for install data for a given day
  submission_date >= @submission_date
  AND submission_date < DATE_ADD(@submission_date, INTERVAL latency DAY)
  -- Filter out bogus install dates
  AND install_day > 10000 -- before Firefox was a thing
  AND install_day < 25000 -- the distant future
  AND DATE_ADD('1970-01-01', INTERVAL install_day DAY) = @submission_date
GROUP BY
  addon_id
