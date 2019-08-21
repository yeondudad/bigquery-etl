import requests
from datetime import datetime, timedelta
from dateutil.parser import parse
import argparse
from dateutil.tz import tzutc
import sys


args = argparse.ArgumentParser()
args.add_argument("submission_date", type=parse, help="Submission date to process")


TEMPLATE = """
WITH
  appearances AS (
  SELECT
    client_id,
    e.event_string_value AS experiment_id,
    udf.get_key(e.event_map_values,
      'branch') AS experiment_branch,
    SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%E3SZ',
      creation_date) AS client_time,
    submission_timestamp AS server_time,
    'normandy_enrollment' AS first_appearance_type,
    date(submission_timestamp) AS submission_date
  FROM
    `moz-fx-data-shar-nonprod-efed.telemetry_stable.event_v4`
  CROSS JOIN
    UNNEST(`moz-fx-data-derived-datasets`.udf_js.json_extract_events(json_EXTRACT(additional_properties,
          '$.payload.events'))) AS e
  WHERE
    e.event_category='normandy'
    AND DATE(submission_timestamp) = @submission_date
    AND e.event_string_value IN {active_experiments}
  UNION ALL
  SELECT
    client_id,
    experiment.key AS experiment_id,
    experiment.value.branch AS branch,
    SAFE.PARSE_DATETIME('%Y-%m-%dT%H:%M:%E3SZ',
      creation_date) AS client_time,
    submission_timestamp AS server_time,
    'main_ping' AS first_appearance_type,
    date(submission_timestamp) AS submission_date
  FROM
    `moz-fx-data-shar-nonprod-efed.telemetry_stable.main_v4`
  CROSS JOIN
    UNNEST(environment.experiments) AS experiment
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND experiment.key IN {active_experiments} )
SELECT
  * EXCEPT (RowNum)
FROM (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY server_time) AS RowNum,
    *
  FROM
    appearances ) sorted
WHERE
  RowNum = 1
  AND NOT EXISTS(
  SELECT
    NULL
  FROM
    telemetry_events_dev.experiments_first_seen
  WHERE
    experiments_first_seen.client_id = sorted.client_id
    AND experiments_first_seen.experiment_id = sorted.experiment_id)
"""


def get_enabled_recipes(begin, end):
    """Scans the normandy api for recipes enabled in the specified period"""
    pref_flips = "https://normandy.services.mozilla.com/api/v3/recipe/?latest_revision__action=3"
    opt_out = "https://normandy.services.mozilla.com/api/v3/recipe/?latest_revision__action=4"
    return _get_enabled_recipes(pref_flips, 'slug', begin, end) + _get_enabled_recipes(opt_out, 'name', begin, end)


def _get_enabled_recipes(url, slug_field, begin, end):
    response = requests.get(url).json()
    recipes = response['results']
    while (response['next'] is not None):
        response = requests.get(response['next']).json()
        recipes += response['results']

    return [
        recipe['latest_revision']['arguments'][slug_field] for recipe in recipes
        if (was_enabled(recipe['latest_revision'].get('enabled_states', []), begin, end)
            and not recipe['latest_revision'].get('arguments', {}).get('isHighVolume', False))
    ]


def was_enabled(enabled_states, begin, end):
    # Assumes enabled_states is in reverse chronological order
    try:
        latest = enabled_states[0]
        # special handling if the last state is enabled and true
        if (latest['enabled'] == True):
            if (parse(latest['created']) < end):
                return True
            else:
                return _was_enabled(enabled_states[1], enabled_states[2], begin, end, enabled_states[3::])
        else:
            return _was_enabled(enabled_states[0], enabled_states[1], begin, end, enabled_states[2::])
    except IndexError:
        return False


def _was_enabled(disable, enable, begin, end, rest):
    if (disable['enabled'] is not False or enable['enabled'] is not True):
        raise Exception("Unexpected enabled state sequence")

    # get the later of the two beginnings of the period, and the earlier of the two endings.
    # If the latest of the beginnings is less than the earliest of the endings, the periods overlap
    latest_begin = max(begin, parse(enable['created']))
    earliest_end = min(end, parse(disable['created']))
    if (latest_begin < earliest_end):
        return True
    else:
        return _was_enabled(rest[0], rest[1], begin, end, rest[2::])


def recipes_to_string(recipe_ids):
    stringified = [f"'{recipe}'" for recipe in recipe_ids]
    return f"({','.join(stringified)})"


def generate_sql(opts):
    begin = opts["submission_date"].replace(tzinfo=tzutc())
    end = begin + timedelta(1)
    active_experiments = recipes_to_string(get_enabled_recipes(begin, end))
    return TEMPLATE.format(**locals())


def main(argv, out=print):
    """Print a particular day's experiment first appearance query to stdout."""
    opts = args.parse_args(argv[1:])
    out(generate_sql(vars(opts)))


if __name__ == "__main__":
    main(sys.argv)
