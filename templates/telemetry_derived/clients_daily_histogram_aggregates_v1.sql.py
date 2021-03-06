#!/usr/bin/env python3
"""clients_daily_histogram_aggregates query generator."""
import sys
import json
import argparse
import textwrap
import subprocess
import urllib.request


PROBE_INFO_SERVICE = (
    "https://probeinfo.telemetry.mozilla.org/firefox/all/main/all_probes"
)

p = argparse.ArgumentParser()
p.add_argument(
    "--agg-type",
    type=str,
    help="One of histograms/keyed_histograms",
    required=True,
)


def generate_sql(opts, additional_queries, windowed_clause, select_clause):
    """Create a SQL query for the clients_daily_histogram_aggregates dataset."""
    get_keyval_pairs = """
        CREATE TEMPORARY FUNCTION get_keyval_pairs(y STRING)
        RETURNS ARRAY<STRING>
        LANGUAGE js AS
        '''
          var z = new Array();
          node = JSON.parse(y);
          for (let [key, value] of Object.entries(node)) {
            z.push(`${key}:${value}`);
          }
          return z;
        ''';
    """

    string_to_arr = """
        -- convert a string like '[5, 6, 7]' to an array struct
        CREATE TEMPORARY FUNCTION string_to_arr(y STRING)
        RETURNS ARRAY<STRING>
        LANGUAGE js AS
        '''
          return JSON.parse(y);
        ''';
    """

    """Create a SQL query for the clients_daily_histogram_aggregates dataset."""
    return textwrap.dedent(
        f"""-- Query generated by: templates/clients_daily_histogram_aggregates.sql.py
        {get_keyval_pairs}

        {string_to_arr}

        CREATE TEMP FUNCTION udf_get_bucket_range(histograms ARRAY<STRING>) AS ((
          WITH buckets AS (
            SELECT
              string_to_arr(JSON_EXTRACT(histogram, "$.range")) AS bucket_range,
              SAFE_CAST(JSON_EXTRACT(histogram, "$.bucket_count") AS INT64) AS num_buckets
            FROM UNNEST(histograms) AS histogram
            WHERE histogram IS NOT NULL
              AND JSON_EXTRACT(histogram, "$.range") IS NOT NULL
              AND JSON_EXTRACT(histogram, "$.bucket_count") IS NOT NULL
            LIMIT 1
          )

          SELECT AS STRUCT
            SAFE_CAST(bucket_range[OFFSET(0)] AS INT64) AS first_bucket,
            SAFE_CAST(bucket_range[OFFSET(1)] AS INT64) AS last_bucket,
            num_buckets
          FROM
            buckets));


        CREATE TEMP FUNCTION udf_get_histogram_type(histograms ARRAY<STRING>) AS ((
            SELECT
              CASE SAFE_CAST(JSON_EXTRACT(histogram, "$.histogram_type") AS INT64)
                WHEN 0 THEN 'histogram-exponential'
                WHEN 1 THEN 'histogram-linear'
                WHEN 2 THEN 'histogram-boolean'
                WHEN 3 THEN 'histogram-flag'
                WHEN 4 THEN 'histogram-count'
                WHEN 5 THEN 'histogram-categorical'
              END AS histogram_type
            FROM UNNEST(histograms) AS histogram
            WHERE histogram IS NOT NULL
              AND JSON_EXTRACT(histogram, "$.histogram_type") IS NOT NULL
            LIMIT 1
        ));

        CREATE TEMP FUNCTION
          udf_aggregate_json_sum(histograms ARRAY<STRING>) AS (ARRAY(
              SELECT
                AS STRUCT SPLIT(keyval, ':')[OFFSET(0)] AS key,
                SUM(SAFE_CAST(SPLIT(keyval, ':')[OFFSET(1)] AS INT64)) AS value
              FROM
                UNNEST(histograms) AS histogram,
                UNNEST(get_keyval_pairs(JSON_EXTRACT(histogram, "$.values"))) AS keyval
              WHERE histogram IS NOT NULL
                AND JSON_EXTRACT(histogram, "$.values") IS NOT NULL
              GROUP BY key));

        WITH filtered AS (
            SELECT
                *,
                SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
                DATE(submission_timestamp) as submission_date,
                normalized_os as os,
                application.build_id AS app_build_id,
                normalized_channel AS channel
            FROM `moz-fx-data-shared-prod.telemetry_stable.main_v4`
            WHERE DATE(submission_timestamp) = @submission_date
                AND normalized_channel in (
                  "release", "beta", "nightly"
                )
                AND client_id IS NOT NULL),

        {additional_queries}

        aggregated AS (
            {windowed_clause}
        )
        {select_clause}
        """
    )


def _get_keyed_histogram_sql(probes):
    probes_struct = []
    for probe, processes in probes.items():
        for process in processes:
            probe_location = (f"payload.keyed_histograms.{probe}"
                if process == 'parent'
                else f"payload.processes.{process}.keyed_histograms.{probe}"
            )
            probes_struct.append((
                f"('{probe}', '{process}', {probe_location})"
            ))

    probes_struct.sort()
    probes_arr = ",\n\t\t\t".join(probes_struct)

    probes_string = """
        metric,
        key,
        ARRAY_AGG(value) as bucket_range,
        ARRAY_AGG(value) as value
    """

    additional_queries = f"""
        grouped_metrics AS
          (select
            submission_timestamp,
            DATE(submission_timestamp) as submission_date,
            client_id,
            normalized_os as os,
            SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
            application.build_id AS app_build_id,
            normalized_channel AS channel,
            ARRAY<STRUCT<
                name STRING,
                process STRING,
                value ARRAY<STRUCT<key STRING, value STRING>>
            >>[
              {probes_arr}
            ] as metrics
          FROM filtered),

          flattened_metrics AS
            (SELECT
              submission_timestamp,
              submission_date,
              client_id,
              os,
              app_version,
              app_build_id,
              channel,
              process,
              metrics.name AS metric,
              value.key AS key,
              value.value AS value
            FROM grouped_metrics
            CROSS JOIN UNNEST(metrics) AS metrics
            CROSS JOIN unnest(metrics.value) AS value),
    """

    windowed_clause = f"""
        SELECT
            submission_date,
            client_id,
            os,
            app_version,
            app_build_id,
            channel,
            process,
            {probes_string}
            FROM flattened_metrics
            GROUP BY
                client_id,
                submission_date,
                os,
                app_version,
                app_build_id,
                channel,
                process,
                metric,
                key
    """

    select_clause = """
        SELECT
            client_id,
            submission_date,
            os,
            app_version,
            app_build_id,
            channel,
            ARRAY_AGG(STRUCT<
                metric STRING,
                metric_type STRING,
                key STRING,
                process STRING,
                agg_type STRING,
                bucket_range STRUCT<
                    first_bucket INT64,
                    last_bucket INT64,
                    num_buckets INT64
                >,
                value ARRAY<STRUCT<key STRING, value INT64>>
            >(
                metric,
                udf_get_histogram_type(bucket_range),
                key,
                process,
                '',
                udf_get_bucket_range(bucket_range),
                udf_aggregate_json_sum(value)
            )) AS histogram_aggregates
        FROM aggregated
        GROUP BY
            client_id,
            submission_date,
            os,
            app_version,
            app_build_id,
            channel
    """

    return {
        "additional_queries": additional_queries,
        "select_clause": select_clause,
        "windowed_clause": windowed_clause,
    }


def get_histogram_probes_sql_strings(probes, histogram_type):
    """Put together the subsets of SQL required to query histograms."""
    sql_strings = {}
    if histogram_type == "keyed_histograms":
        return _get_keyed_histogram_sql(probes)

    probe_structs = []
    for probe, processes in probes.items():
        for process in processes:
            probe_location = (f"payload.histograms.{probe}"
                if process == 'parent'
                else f"payload.processes.{process}.histograms.{probe}"
            )
            probe_structs.append((
                f"('{probe}', '{process}', {probe_location})"
            ))

    probe_structs.sort()
    probes_arr = ",\n\t\t\t".join(probe_structs)
    probes_string = f"""
            ARRAY<STRUCT<
                metric STRING,
                process STRING,
                value STRING
            >> [
            {probes_arr}
        ] AS histogram_aggregates
    """

    sql_strings[
        "select_clause"
    ] = f"""
        SELECT
          client_id,
          submission_date,
          os,
          app_version,
          app_build_id,
          channel,
          ARRAY_AGG(STRUCT<
            metric STRING,
            metric_type STRING,
            key STRING,
            process STRING,
            agg_type STRING,
            bucket_range STRUCT<first_bucket INT64, last_bucket INT64, num_buckets INT64>,
            value ARRAY<STRUCT<key STRING, value INT64>>
          > (metric,
            udf_get_histogram_type(value),
            '',
            process,
            'summed_histogram',
            udf_get_bucket_range(value),
            udf_aggregate_json_sum(value))) AS histogram_aggregates
        FROM aggregated
        GROUP BY
          1, 2, 3, 4, 5, 6

    """

    sql_strings["additional_queries"] = f"""
        histograms AS (
            SELECT
                client_id,
                submission_date,
                os,
                app_version,
                app_build_id,
                channel,
                {probes_string}
            FROM filtered),

        filtered_aggregates AS (
          SELECT
            submission_date,
            client_id,
            os,
            app_version,
            app_build_id,
            channel,
            metric,
            process,
            value
          FROM histograms
          CROSS JOIN
            UNNEST(histogram_aggregates)
          WHERE value IS NOT NULL
        ),
    """

    sql_strings[
        "windowed_clause"

    ] = f"""
      SELECT
        client_id,
        submission_date,
        os,
        app_version,
        app_build_id,
        channel,
        metric,
        process,
        ARRAY_AGG(value) AS value
      FROM filtered_aggregates
      GROUP BY
        1, 2, 3, 4, 5, 6, 7, 8
    """

    return sql_strings


def get_histogram_probes(histogram_type):
    """Return relevant histogram probes."""
    project = "moz-fx-data-shared-prod"
    main_summary_histograms = {}
    process = subprocess.Popen(
        [
            "bq",
            "show",
            "--schema",
            "--format=json",
            f"{project}:telemetry_stable.main_v4",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if process.returncode > 0:
        raise Exception(
            f"Call to bq exited non-zero: {process.returncode}", stdout, stderr
        )
    main_summary_schema = json.loads(stdout)

    # Fetch the histograms field
    histograms_field = []
    for field in main_summary_schema:
        if field["name"] != "payload":
            continue

        for payload_field in field["fields"]:
            if payload_field["name"] == histogram_type:
                histograms_field.append({"histograms": payload_field, "process": 'parent'})
                continue

            if payload_field["name"] == "processes":
                for processes_field in payload_field["fields"]:
                    if processes_field["name"] == "content":
                        process_field = processes_field["name"]
                        for type_field in processes_field["fields"]:
                            if type_field["name"] == histogram_type:
                                histograms_field.append({"histograms": type_field, "process": process_field})
                                break

    if len(histograms_field) == 0:
        return

    for histograms_and_process in histograms_field:
        for histogram in histograms_and_process["histograms"].get("fields", {}):
            histograms_dict = None
            if "name" not in histogram:
                continue

            processes = main_summary_histograms.setdefault(histogram["name"], set())
            processes.add(histograms_and_process["process"])
            main_summary_histograms[histogram["name"]] = processes

    with urllib.request.urlopen(PROBE_INFO_SERVICE) as url:
        data = json.loads(url.read().decode())
        histogram_probes = {
            x.replace("histogram/", "").replace(".", "_").lower()
            for x in data.keys()
            if x.startswith("histogram/")
        }
        relevant_probes = {
            histogram: process for histogram, process in main_summary_histograms.items() if histogram in histogram_probes
        }
        return relevant_probes


def main(argv, out=print):
    """Print a clients_daily_histogram_aggregates query to stdout."""
    opts = vars(p.parse_args(argv[1:]))
    sql_string = ""

    if opts["agg_type"] in ("histograms", "keyed_histograms"):
        histogram_probes = get_histogram_probes(opts["agg_type"])
        sql_string = get_histogram_probes_sql_strings(
            histogram_probes, opts["agg_type"]
        )
    else:
        raise ValueError("agg-type must be one of histograms, keyed_histograms")

    out(
        generate_sql(
            opts,
            sql_string.get("additional_queries", ""),
            sql_string["windowed_clause"],
            sql_string["select_clause"],
        )
    )


if __name__ == "__main__":
    main(sys.argv)
