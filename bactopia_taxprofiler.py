"""CAPE workflow for Bactopia v4.1.0 and taxprofiler Kraken2.

Bactopia runs first as a non-blocking AWS Batch parent. The DAG copies the
Bactopia input to a taxprofiler-compatible `.fastq.gz` key, creates a samplesheet,
and submits standalone nf-core/taxprofiler Kraken2 processing without waiting for
Bactopia QC. The Bactopia report branch
waits for the full Bactopia run and remains independent of taxprofiler.

The workflow is triggered through the CAPE `/workflows/trigger` endpoint with
structured `pipelineConfigs` options. The DAG converts those option dictionaries
to the Nextflow parent `NF_OPTS` string. Taxprofiler input and database paths are
DAG-owned for this initial implementation; its output path remains caller-owned.
"""

import csv
import io
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional, Set, Tuple

import boto3  # pyright: ignore[reportMissingImports]
from airflow.providers.amazon.aws.operators.batch import (  # pyright: ignore[reportMissingImports]
    BatchOperator,
)
from airflow.providers.amazon.aws.sensors.batch import (  # pyright: ignore[reportMissingImports]
    BatchSensor,
)
from airflow.sdk import (  # pyright: ignore[reportMissingImports]
    chain,
    dag,
    task,
)
from botocore.exceptions import (  # pyright: ignore[reportMissingImports]
    ClientError,
)

log = logging.getLogger(__name__)

DAG_ID = "bactopia_taxprofiler"
DAG_DISPLAY_NAME = "Bactopia v4.1.0 and taxprofiler Kraken2"
DAG_DESCRIPTION = (
    "Run Bactopia v4.1.0 and standalone nf-core/taxprofiler "
    "Kraken2 in AWS Batch with CAPE report publishing for submitted "
    "samples."
)
# Keep this value stable. A dynamic start date changes the serialized DAG hash
# on every parse and creates a new Airflow DAG version every refresh.
DAG_START_DATE = datetime(2026, 1, 1)

BACTOPIA_PROJ = "bactopia/bactopia"
BACTOPIA_VERSION = "v4.1.0"
TAXPROFILER_PROJ = "nf-core/taxprofiler"
TAXPROFILER_VERSION = "2.0.1"

# TODO: source these deployment values from CAPE configuration rather than
# keeping the current development values in the workflow.
WORKFLOW_QUEUE_NAME = "ccd-pvsl-workflows-btch-jobq-e326d2f"
NEXTFLOW_JOB_DEFINITION = "ccd-pvsl-nextflow-jobdef"
JOB_QUEUE_NAME = "ccd-pvsl-analysis-btch-jobq-0a107a5"

# This is the immutable database-sheet publication selected for the initial
# workflow.
#
# TODO(#2): Remove this DAG-owned database sheet when the caller supplies
# `--databases` together with `--input`. At that point, require both values and
# remove the DAG-created samplesheet as well.
TAXPROFILER_DATABASE_SHEET = (
    "s3://ccd-meta-assets-vbkt-s3-8b7134e/pipelines/nf-core/taxprofiler/"
    "2.0.1/database-sheets/standard-8-2026-06-26.csv"
)
# TODO: Replace this DAG-owned policy with the trusted profile policy supplied
# by the shared CAPE submission path. Keep this in sync with the current
# taxprofiler profile until that runtime wiring is available.
TAXPROFILER_PROCESS_OVERRIDES = {
    "kraken2": {
        "selector": ".*KRAKEN2_KRAKEN2.*",
        "cpus": 2,
        "memory": "9.GB",
        "time": "4.h",
    }
}
TAXPROFILER_SAMPLESHEET_PREFIX = "batch_job_scratch/taxprofiler"

# Bactopia and taxprofiler reports use the existing CAPE crawler/report path.
# Cape Cod owns the report data functions and catalog contract.
AWS_REGION = "us-east-2"
INPUT_CLEAN_CRAWLER_NAME = (
    "ccd-dlh-T-seqauto-input-clean-vbkt-crwl-gcrwl-be77632"
)
RESULT_CLEAN_CRAWLER_NAME = (
    "ccd-dlh-T-seqauto-result-clean-vbkt-crwl-gcrwl-1ceb6f5"
)
SEQAUTO_CRAWLER_NAMES = (
    INPUT_CLEAN_CRAWLER_NAME,
    RESULT_CLEAN_CRAWLER_NAME,
)
REPORT_LAMBDA_ARN = (
    "arn:aws:lambda:us-east-2:767397883306:function:"
    "ccd-pvsl-capi-api-getcannedreport-lmbdfn-b295d26"
)
REPORT_ID = "bactopia-single-sample-analysis"
TAXPROFILER_REPORT_ID = "taxprofiler-kraken2"
REPORT_OUTPUT_BUCKET = "ccd-dlh-t-seqauto-artifacts-vbkt-s3-d2421eb"
REPORT_OUTPUT_PREFIX = "reports"
REPORT_OUTPUT_FILENAME = "bactopia.html"
TAXPROFILER_REPORT_OUTPUT_FILENAME = "taxprofiler-kraken2.html"
REPORT_MAX_ATTEMPTS = 30
REPORT_ATTEMPT_SLEEP_SECONDS = 60
CRAWLER_POLL_INTERVAL_SECONDS = 15
CRAWLER_WAIT_TIMEOUT_SECONDS = 900

EXPECTED_PIPELINES = {
    "bactopia": {
        "pipeline_ids": ["bactopia-ont-v4.1.0"],
        "required_fields": [
            "-profile",
            "--ont",
            "--sample",
            "--outdir",
        ],
    },
    "taxprofiler": {
        "pipeline_ids": ["taxprofiler-kraken2-2.0.1"],
        "required_fields": [
            "-profile",
            "--outdir",
        ],
        "dag_owned_fields": ["--input", "--databases"],
    },
}


def nextflow_options_to_cli_string(
    options_dict: dict,
    excluded_keys: Optional[Set[str]] = None,
    omit_false_keys: Optional[Set[str]] = None,
) -> str:
    """Convert a Nextflow option dictionary into a CLI string.

    Empty optional values are omitted. Boolean values are rendered in lowercase
    because trigger options arrive as JSON values while Nextflow receives a
    shell command string. Some false-valued options must be omitted instead of
    rendered as the string ``false`` because the receiving pipeline treats that
    non-empty string as truthy.
    """
    excluded_keys = excluded_keys or set()
    omit_false_keys = omit_false_keys or set()
    parts = []
    for key, value in options_dict.items():
        if key in excluded_keys:
            continue
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            if not value and key in omit_false_keys:
                continue
            value = "true" if value else "false"
        parts.append(f"{key} {value}")
    return " ".join(parts)


def extract_s3_bucket_name(s3_path: str) -> str:
    """Extract the bucket name from an `s3://` URI."""
    if not isinstance(s3_path, str) or not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path (missing s3://): {s3_path}")

    path_without_scheme = s3_path[5:]
    bucket_name = path_without_scheme.split("/", 1)[0]
    if not bucket_name:
        raise ValueError(f"Invalid S3 path (no bucket name): {s3_path}")
    return bucket_name


def extract_s3_key(s3_path: str) -> str:
    """Extract the object key from an `s3://` URI."""
    if not isinstance(s3_path, str) or not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path (missing s3://): {s3_path}")

    path_without_scheme = s3_path[5:]
    _, separator, key = path_without_scheme.partition("/")
    if not separator or not key:
        raise ValueError(f"Invalid S3 path (no object key): {s3_path}")
    return key.strip("/")


def extract_s3_leaf_name(s3_path: str) -> str:
    """Extract the final non-empty object-key component from an S3 URI."""
    key = extract_s3_key(s3_path)
    return key.rsplit("/", 1)[-1]


def taxprofiler_fastq_uri(source_uri: str) -> str:
    """Return a taxprofiler-compatible FASTQ URI for a gzip object."""
    if source_uri.endswith((".fastq.gz", ".fq.gz")):
        return source_uri
    if not source_uri.endswith(".gz"):
        raise ValueError(
            f"Taxprofiler input must be gzip-compressed FASTQ: {source_uri}"
        )
    return f"{source_uri[:-3]}.fastq.gz"


def safe_s3_component(value: str) -> str:
    """Make an Airflow run identifier safe for an S3 key component."""
    component = re.sub(r"[^A-Za-z0-9._=-]+", "_", value).strip("_")
    return component or "run"


def _require_non_empty(options: dict, key: str, pipeline_key: str) -> None:
    value = options.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Missing required string field '{key}' for pipeline "
            f"'{pipeline_key}'"
        )


def _require_positive_integer(
    options: dict, key: str, pipeline_key: str
) -> None:
    value = options.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(
            f"Field '{key}' for pipeline '{pipeline_key}' must be a "
            "positive integer"
        )


def _require_memory_limit(options: dict, key: str, pipeline_key: str) -> None:
    value = options.get(key)
    if not isinstance(value, str) or not re.fullmatch(
        r"\d+(?:\.\d+)?\.?\s*(?:[KMGTP]?B)", value, re.IGNORECASE
    ):
        raise ValueError(
            f"Field '{key}' for pipeline '{pipeline_key}' must be a "
            "memory value such as 24.GB"
        )


def _require_boolean(
    options: dict,
    key: str,
    pipeline_key: str,
    expected: Optional[bool] = None,
) -> None:
    if key not in options:
        raise ValueError(
            f"Missing required field '{key}' for pipeline '{pipeline_key}'"
        )
    value = options[key]
    if not isinstance(value, bool):
        raise ValueError(
            f"Field '{key}' for pipeline '{pipeline_key}' must be a boolean"
        )
    if expected is not None and value is not expected:
        expected_text = "true" if expected else "false"
        raise ValueError(
            f"Field '{key}' for pipeline '{pipeline_key}' must be "
            f"{expected_text}"
        )


def _validate_profile_option(options: dict, pipeline_key: str) -> None:
    _require_non_empty(options, "-profile", pipeline_key)
    if options["-profile"] != "docker":
        raise ValueError(
            f"Field '-profile' for pipeline '{pipeline_key}' must be docker"
        )


def _validate_pipeline_options(pipeline_key: str, options: object) -> dict:
    """Validate one pipeline's independent Nextflow options."""
    if not isinstance(options, dict):
        raise ValueError(
            f"'nextflowOptions' for pipeline '{pipeline_key}' must be an object"
        )

    if any(not isinstance(key, str) for key in options):
        raise ValueError(
            f"All option names for pipeline '{pipeline_key}' must be strings"
        )

    spec = EXPECTED_PIPELINES[pipeline_key]
    for field in spec["required_fields"]:
        _require_non_empty(options, field, pipeline_key)

    _validate_profile_option(options, pipeline_key)

    if pipeline_key == "bactopia":
        output_key = extract_s3_key(options["--outdir"])
        if output_key != "pipeline-output":
            raise ValueError(
                "Field '--outdir' for pipeline 'bactopia' must use the "
                "top-level S3 key 'pipeline-output' so the Bactopia ETL "
                "can discover 'bactopia-runs/<run>' outputs"
            )
        _require_positive_integer(options, "--max_cpus", pipeline_key)
        _require_memory_limit(options, "--max_memory", pipeline_key)
        _require_boolean(options, "--skip_qc_plots", pipeline_key)
        return dict(options)

    _require_boolean(options, "--run_kraken2", pipeline_key, expected=True)
    _require_boolean(
        options,
        "--kraken2_save_minimizers",
        pipeline_key,
        expected=False,
    )
    if "--skip_preprocessing_qc" in options:
        _require_boolean(
            options,
            "--skip_preprocessing_qc",
            pipeline_key,
            expected=True,
        )

    # TODO(#2): This transitional mode owns both values. Reject non-empty
    # caller values rather than silently ignoring them. The future mode will
    # require both options and remove this task and constant.
    dag_owned_fields = set(spec["dag_owned_fields"])
    for field in dag_owned_fields:
        if options.get(field) not in (None, ""):
            raise ValueError(
                f"Field '{field}' for pipeline '{pipeline_key}' is DAG-owned"
            )

    options = dict(options)
    options.setdefault("--skip_preprocessing_qc", True)
    return options


def _extract_lambda_payload(response: dict) -> Optional[str]:
    """Read the response payload returned by `lambda.invoke`."""
    payload = response.get("Payload")
    if payload is None:
        return None
    return payload.read().decode("utf-8")


def start_crawler_if_idle(glue_client, crawler_name: str) -> None:
    """Start a Glue crawler unless it is already running."""
    try:
        glue_client.start_crawler(Name=crawler_name)
        log.info("Started Glue crawler '%s'", crawler_name)
    except ClientError as err:
        if err.response["Error"]["Code"] != "CrawlerRunningException":
            raise
        log.info(
            "Glue crawler '%s' already running; will wait for it to finish",
            crawler_name,
        )


def run_crawlers_and_wait(glue_client, crawler_names) -> None:
    """Start the crawlers concurrently and wait for all of them to finish."""
    for crawler_name in crawler_names:
        start_crawler_if_idle(glue_client, crawler_name)

    deadline = time.monotonic() + CRAWLER_WAIT_TIMEOUT_SECONDS
    time.sleep(CRAWLER_POLL_INTERVAL_SECONDS)
    pending = list(crawler_names)
    while pending:
        still_running = []
        for crawler_name in pending:
            state = glue_client.get_crawler(Name=crawler_name)["Crawler"][
                "State"
            ]
            if state == "READY":
                log.info("Glue crawler '%s' finished", crawler_name)
            else:
                still_running.append(crawler_name)

        if not still_running:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"Glue crawlers {still_running} did not finish within "
                f"{CRAWLER_WAIT_TIMEOUT_SECONDS}s."
            )
        pending = still_running
        time.sleep(CRAWLER_POLL_INTERVAL_SECONDS)


def invoke_report_lambda(
    lambda_client, sample_id: str, report_id: str = REPORT_ID
) -> Tuple[int, Optional[str]]:
    """Invoke the deployed Bactopia canned-report Lambda."""
    event = {
        "queryStringParameters": {
            "reportId": report_id,
            "sampleId": sample_id,
            "format": "html",
        }
    }
    response = lambda_client.invoke(
        FunctionName=REPORT_LAMBDA_ARN,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode("utf-8"),
    )

    if response.get("FunctionError"):
        error_payload = _extract_lambda_payload(response)
        log.warning("Report Lambda returned FunctionError: %s", error_payload)
        return 500, None

    payload_text = _extract_lambda_payload(response)
    if payload_text is None:
        return 500, None
    try:
        payload = json.loads(payload_text)
    except (ValueError, KeyError) as err:
        log.warning("Could not parse report Lambda response: %s", err)
        return 500, None

    status_code = payload.get("statusCode", 500)
    if status_code != 200:
        log.warning(
            "Report Lambda handler returned status %s: %s",
            status_code,
            payload.get("body"),
        )
        return status_code, None
    return 200, payload.get("body")


def _generate_and_store_report(
    sample_id: str, report_id: str, output_filename: str
) -> str:
    """Crawl CAPE data and store the rendered Bactopia report."""
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    lambda_client = boto3.client("lambda", region_name=AWS_REGION)
    s3_client = boto3.client("s3", region_name=AWS_REGION)

    report_html = None
    for attempt in range(1, REPORT_MAX_ATTEMPTS + 1):
        log.info(
            "Report '%s' attempt %s/%s for sample '%s'",
            report_id,
            attempt,
            REPORT_MAX_ATTEMPTS,
            sample_id,
        )
        run_crawlers_and_wait(glue_client, SEQAUTO_CRAWLER_NAMES)
        status_code, body = invoke_report_lambda(
            lambda_client, sample_id, report_id
        )
        if status_code == 200 and body:
            report_html = body
            break

        if attempt < REPORT_MAX_ATTEMPTS:
            time.sleep(REPORT_ATTEMPT_SLEEP_SECONDS)

    if report_html is None:
        raise RuntimeError(
            f"Report '{report_id}' for sample '{sample_id}' was not ready "
            f"after {REPORT_MAX_ATTEMPTS} attempts."
        )

    s3_key = f"{REPORT_OUTPUT_PREFIX}/{sample_id}/{output_filename}"
    s3_client.put_object(
        Bucket=REPORT_OUTPUT_BUCKET,
        Key=s3_key,
        Body=report_html.encode("utf-8"),
        ContentType="text/html",
    )
    s3_uri = f"s3://{REPORT_OUTPUT_BUCKET}/{s3_key}"
    log.info("Wrote Bactopia report to %s", s3_uri)
    return s3_uri


@task
def generate_and_store_report(**context) -> str:
    """Generate the Bactopia report after the full Bactopia run completes."""
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    sample_id = configs["bactopia"]["nextflowOptions"]["--sample"]
    return _generate_and_store_report(
        sample_id, REPORT_ID, REPORT_OUTPUT_FILENAME
    )


@task
def generate_and_store_taxprofiler_report(**context) -> str:
    """Generate the taxprofiler Kraken2 report after profiling completes."""
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    # TODO(#2): Replace this DAG-generated sample with the authoritative
    # sample ID parsed from the caller-provided external samplesheet.
    sample_id = configs["bactopia"]["nextflowOptions"]["--sample"]
    return _generate_and_store_report(
        sample_id,
        TAXPROFILER_REPORT_ID,
        TAXPROFILER_REPORT_OUTPUT_FILENAME,
    )


@task
def copy_taxprofiler_input_to_fastq_gz(**context) -> str:
    """Copy the Bactopia input to a taxprofiler-compatible S3 key."""
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    source_uri = configs["bactopia"]["nextflowOptions"]["--ont"]
    target_uri = taxprofiler_fastq_uri(source_uri)
    if target_uri == source_uri:
        return source_uri

    # TODO(#2): Remove this copy when input-clean ETL writes `.fastq.gz`
    # keys directly. The current source object is gzip FASTQ content whose key
    # ends in `.gz`, while taxprofiler validates the filename suffix.
    source_bucket = extract_s3_bucket_name(source_uri)
    source_key = extract_s3_key(source_uri)
    target_bucket = extract_s3_bucket_name(target_uri)
    target_key = extract_s3_key(target_uri)
    boto3.client("s3", region_name=AWS_REGION).copy_object(
        Bucket=target_bucket,
        Key=target_key,
        CopySource={"Bucket": source_bucket, "Key": source_key},
        ContentType="application/gzip",
        MetadataDirective="REPLACE",
    )
    log.info("Copied taxprofiler input to %s", target_uri)
    return target_uri


def render_taxprofiler_samplesheet(
    sample: str, run_accession: str, fastq_uri: str
) -> str:
    """Render one ONT sample row in the taxprofiler CSV format."""
    sheet = io.StringIO()
    writer = csv.writer(sheet, lineterminator="\n")
    writer.writerow(
        [
            "sample",
            "run_accession",
            "instrument_platform",
            "fastq_1",
            "fastq_2",
            "fasta",
        ]
    )
    writer.writerow(
        [sample, run_accession, "OXFORD_NANOPORE", fastq_uri, "", ""]
    )
    return sheet.getvalue()


@task
def create_taxprofiler_samplesheet(**context) -> str:
    """Create the temporary taxprofiler samplesheet in the result bucket."""
    configs = context["ti"].xcom_pull(
        task_ids="validate_and_extract_nextflow_configs"
    )
    bactopia_options = configs["bactopia"]["nextflowOptions"]
    taxprofiler_options = configs["taxprofiler"]["nextflowOptions"]

    taxprofiler_input_uri = context["ti"].xcom_pull(
        task_ids="copy_taxprofiler_input_to_fastq_gz"
    )
    output_bucket = extract_s3_bucket_name(taxprofiler_options["--outdir"])
    run_component = safe_s3_component(context["dag_run"].run_id)
    output_key = (
        f"{TAXPROFILER_SAMPLESHEET_PREFIX}/{run_component}/samplesheet.csv"
    )
    # TODO(#2): Move samplesheet creation to the external caller with the
    # database sheet. Until then, one Bactopia sample represents one run.
    bactopia_sample = bactopia_options["--sample"]
    sheet = render_taxprofiler_samplesheet(
        bactopia_sample,
        bactopia_sample,
        taxprofiler_input_uri,
    )

    boto3.client("s3", region_name=AWS_REGION).put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=sheet.encode("utf-8"),
        ContentType="text/csv",
    )
    output_uri = f"s3://{output_bucket}/{output_key}"
    log.info("Wrote taxprofiler samplesheet to %s", output_uri)
    return output_uri


@task
def validate_and_extract_nextflow_configs(
    fail_on_any_error: bool = True, **context
) -> dict:
    """Validate independent pipeline configs and render their CLI options."""
    conf = context["dag_run"].conf
    if conf is None:
        raise ValueError(
            "No configuration provided. DAG must be triggered with 'conf'."
        )
    pipeline_configs = conf.get("pipelineConfigs")
    if not isinstance(pipeline_configs, list):
        raise ValueError("'pipelineConfigs' must be a list")

    received_ids = []
    entries_by_id = {}
    expected_ids = {
        pipeline_id
        for spec in EXPECTED_PIPELINES.values()
        for pipeline_id in spec["pipeline_ids"]
    }
    for item in pipeline_configs:
        if not isinstance(item, dict):
            raise ValueError("Each pipeline configuration must be an object")
        pipeline_id = item.get("pipelineId")
        if not isinstance(pipeline_id, str) or not pipeline_id:
            raise ValueError("Each pipeline configuration needs a pipelineId")
        if pipeline_id not in expected_ids:
            raise ValueError(f"Unknown pipelineId: {pipeline_id}")
        if pipeline_id in entries_by_id:
            raise ValueError(f"Duplicate pipelineId: {pipeline_id}")
        received_ids.append(pipeline_id)
        entries_by_id[pipeline_id] = item

    result = {}
    for pipeline_key, pipeline_spec in EXPECTED_PIPELINES.items():
        expected_pipeline_id = pipeline_spec["pipeline_ids"][0]
        matching_config = entries_by_id.get(expected_pipeline_id)
        if matching_config is None:
            error_msg = (
                f"No config found for pipeline '{pipeline_key}'. Expected "
                f"one of: {pipeline_spec['pipeline_ids']}. Received "
                f"pipelineIds: {received_ids}"
            )
            if fail_on_any_error:
                raise ValueError(error_msg)
            log.warning(error_msg)
            continue

        options = _validate_pipeline_options(
            pipeline_key, matching_config.get("nextflowOptions")
        )
        excluded_keys = set(pipeline_spec.get("dag_owned_fields", []))
        omit_false_keys = (
            {"--kraken2_save_minimizers"}
            if pipeline_key == "taxprofiler"
            else set()
        )
        result[pipeline_key] = {
            "pipelineId": matching_config["pipelineId"],
            "nextflowOptions": options,
            "nextflowOptionsCli": nextflow_options_to_cli_string(
                options,
                excluded_keys=excluded_keys,
                omit_false_keys=omit_false_keys,
            ),
        }

    if "bactopia" in result and "taxprofiler" in result:
        bactopia_sample = result["bactopia"]["nextflowOptions"]["--sample"]
        taxprofiler_output_sample = extract_s3_leaf_name(
            result["taxprofiler"]["nextflowOptions"]["--outdir"]
        )
        if taxprofiler_output_sample != bactopia_sample:
            raise ValueError(
                "Taxprofiler '--outdir' leaf must match the generated "
                "samplesheet sample ID until external samplesheet support "
                "is enabled. Expected "
                f"'{bactopia_sample}', received "
                f"'{taxprofiler_output_sample}'"
            )

    return result


@dag(
    dag_id=DAG_ID,
    dag_display_name=DAG_DISPLAY_NAME,
    description=DAG_DESCRIPTION,
    schedule=None,
    start_date=DAG_START_DATE,
    catchup=False,
    user_defined_filters={
        "extract_s3_bucket": extract_s3_bucket_name,
    },
)
def bactopia_taxprofiler():
    """Define the Bactopia v4.1.0 and taxprofiler DAG."""
    configs = validate_and_extract_nextflow_configs()

    submit_bactopia_job = BatchOperator(
        task_id="submit_bactopia_batch_job",
        job_name="{{ dag_run.dag_id }}-bactopia-v4-job",
        job_queue=WORKFLOW_QUEUE_NAME,
        job_definition=NEXTFLOW_JOB_DEFINITION,
        wait_for_completion=False,
        container_overrides={
            "environment": [
                {"name": "PIPELINE", "value": BACTOPIA_PROJ},
                {"name": "PIPELINE_VERSION", "value": BACTOPIA_VERSION},
                {"name": "PIPELINE_QUEUE", "value": JOB_QUEUE_NAME},
                {
                    "name": "NF_OPTS",
                    "value": "{{ ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')['bactopia']['nextflowOptionsCli'] }}",
                },
            ]
        },
    )

    copy_taxprofiler_input = copy_taxprofiler_input_to_fastq_gz()
    create_samplesheet = create_taxprofiler_samplesheet()
    taxprofiler_options_xcom = (
        "ti.xcom_pull(task_ids='validate_and_extract_nextflow_configs')"
        "['taxprofiler']['nextflowOptionsCli']"
    )
    submit_taxprofiler_job = BatchOperator(
        task_id="submit_taxprofiler_batch_job",
        job_name="{{ dag_run.dag_id }}-taxprofiler-kraken2-job",
        job_queue=WORKFLOW_QUEUE_NAME,
        job_definition=NEXTFLOW_JOB_DEFINITION,
        wait_for_completion=True,
        container_overrides={
            "environment": [
                {"name": "PIPELINE", "value": TAXPROFILER_PROJ},
                {"name": "PIPELINE_VERSION", "value": TAXPROFILER_VERSION},
                {"name": "PIPELINE_QUEUE", "value": JOB_QUEUE_NAME},
                {
                    "name": "NEXTFLOW_PROCESS_OVERRIDES",
                    "value": json.dumps(
                        TAXPROFILER_PROCESS_OVERRIDES,
                        separators=(",", ":"),
                    ),
                },
                {
                    "name": "NF_OPTS",
                    "value": (
                        f"{{{{ {taxprofiler_options_xcom} }}}} "
                        "--input {{ ti.xcom_pull(task_ids='create_taxprofiler_samplesheet') }} "
                        f"--databases {TAXPROFILER_DATABASE_SHEET}"
                    ),
                },
            ]
        },
    )

    wait_for_bactopia_complete = BatchSensor(
        task_id="wait_for_bactopia_complete",
        job_id="{{ ti.xcom_pull(task_ids='submit_bactopia_batch_job') }}",
        poke_interval=30,
        mode="reschedule",
    )
    generate_report = generate_and_store_report()
    generate_taxprofiler_report = generate_and_store_taxprofiler_report()

    chain(
        configs,
        submit_bactopia_job,
        copy_taxprofiler_input,
        create_samplesheet,
        submit_taxprofiler_job,
    )
    chain(submit_bactopia_job, wait_for_bactopia_complete)
    chain(submit_taxprofiler_job, generate_taxprofiler_report)
    chain(wait_for_bactopia_complete, generate_report)


bactopia_taxprofiler()
