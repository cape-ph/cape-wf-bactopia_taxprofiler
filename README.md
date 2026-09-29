# CAPE Workflow: Bactopia v4.1.0 + taxprofiler Kraken2

[![CI/CD](https://github.com/cape-ph/cape-wf-bactopia_taxprofiler/actions/workflows/cape.yml/badge.svg)](https://github.com/cape-ph/cape-wf-bactopia_taxprofiler/actions/workflows/cape.yml)

Airflow workflow that runs Bactopia v4.1.0 and standalone `nf-core/taxprofiler` v2.0.1 Kraken2 processing in AWS Batch.

## Overview

The DAG runs these stages:

1. Bactopia v4.1.0 processes the ONT input under the ETL-compatible `pipeline-output` root.
2. The DAG copies the ONT archive supplied to Bactopia to a key ending in `.fastq.gz`.
3. The DAG creates a taxprofiler samplesheet from the Bactopia sample and copied input.
4. Taxprofiler runs the Kraken2-only profile.
5. The Bactopia report branch waits for full Bactopia completion and uses the existing CAPE crawler/report path.

Taxprofiler native output is written below the caller-provided taxprofiler `--outdir`. The database sheet is temporarily fixed to the CAPE Standard-8 publication. Database selection is a later contract change.

Because taxprofiler requires an input samplesheet, the initial DAG creates a run-specific samplesheet after Bactopia submission. The DAG temporarily uses the Bactopia sample for both the samplesheet `sample` and `run_accession` fields. It copies the Bactopia `--ont` object to a key ending in `.fastq.gz` and uses that copy as `fastq_1` so taxprofiler's filename validation succeeds.

TODO: When the caller can provide both taxprofiler `--input` and `--databases`, remove DAG-owned samplesheet creation, the hard-coded database sheet, and this temporary sample reuse.

## Trigger contract

The workflow is triggered through `POST /workflows/trigger?dagId=bactopia_taxprofiler`. The request body is forwarded to Airflow as `dag_run.conf`:

```json
{
  "pipelineConfigs": [
    {
      "pipelineId": "bactopia-ont-v4.1.0",
      "nextflowOptions": {
        "-profile": "docker",
        "--max_cpus": 8,
        "--max_memory": "24.GB",
        "--ont": "s3://input/sample.fastq.gz",
        "--sample": "bactopia-sample",
        "--outdir": "s3://result/pipeline-output",
        "--skip_qc_plots": true
      }
    },
    {
      "pipelineId": "taxprofiler-kraken2-2.0.1",
      "nextflowOptions": {
        "-profile": "docker",
        "--outdir": "s3://result/taxprofiler-output/bactopia-sample",
        "--run_kraken2": true,
        "--kraken2_save_minimizers": false
      }
    }
  ]
}
```

The trigger contract carries structured `nextflowOptions` dictionaries. The DAG matches entries by `pipelineId` and keeps pipeline options independent.

Bactopia options:

- `--max_cpus` and `--max_memory` are required resource limiters. The deployment defaults are `8` and `24.GB`; callers must provide them explicitly. Values may be adjusted when the deployment supports different limits.
- `--ont`, `--sample`, and `--outdir` are required.
- Bactopia `--outdir` must be the top-level `pipeline-output` S3 key. Bactopia creates its internal `bactopia-runs/<run>` partition below that root, which is the path consumed by the existing ETL.
- `--skip_qc_plots` must be present as a boolean. If it is absent, DAG validation rejects the configuration. If it is `true`, QC plots are skipped. If it is `false`, the full QC plot path is preserved.
- `-profile docker` selects the container engine. AWS Batch execution comes from the CAPE-generated Nextflow configuration.

Taxprofiler options:

- `--outdir` is caller-owned and is passed through unchanged.
- The initial samplesheet uses the Bactopia `--sample` for both `sample` and `run_accession`.
- The DAG creates and supplies `--input`.
- The DAG supplies the immutable database sheet through `--databases`.
- The initial policy requires `--run_kraken2 true`, `--skip_preprocessing_qc true`, and `--kraken2_save_minimizers false`. The DAG validates the minimizer setting as a boolean but omits the false-valued option from the Nextflow command because the parent receives a shell string and taxprofiler treats the literal string `false` as truthy.
- The taxprofiler `--outdir` final component must match the generated samplesheet sample ID because the current taxprofiler ETL partitions its cleaned data by that value. The report task uses the generated samplesheet sample ID and validates this invariant.

## Output and dependencies

Bactopia v4 QC is still produced at:

```text
<outdir>/bactopia-runs/<bactopia-run>/<sample>/main/qc/<sample>_ONT.fastq.gz
```

The taxprofiler samplesheet has this shape:

```csv
sample,run_accession,instrument_platform,fastq_1,fastq_2,fasta
bactopia-sample,bactopia-sample,OXFORD_NANOPORE,s3://.../sample_ONT.fastq.gz,,
```

The `fastq_1` value is the run-specific `.fastq.gz` copy of the Bactopia `--ont` archive. The sample and run accession temporarily both use the Bactopia sample. Taxprofiler does not wait for the Bactopia QC object. The report task uses the generated samplesheet sample ID; the output leaf is validated to match it for the current ETL contract.

The taxprofiler input copy is written beside the source under the crawler-excluded `sequencing-reads/` prefix, with the same partition and a `.fastq.gz` suffix. The samplesheet is written to a run-specific temporary S3 key under `batch_job_scratch/taxprofiler/`. Both tasks are intentionally isolated so ownership can move to the caller or another orchestration stage later. Bactopia output is intentionally rooted at `pipeline-output/` so the existing ETL can discover its internal `bactopia-runs/<run>` output.

Taxprofiler native output remains in S3. After taxprofiler completes, the DAG requests the `taxprofiler-kraken2` report through the existing CAPE crawler/report path. CAPE Cod owns the report data function and catalog contract.

## Requirements

- CAPE infrastructure with Airflow 3.0.6 and the Amazon provider.
- AWS Batch workflow and analysis queues.
- The deployed CAPE Nextflow kickstart runtime.
- S3 input and output access.
- The immutable CAPE taxprofiler database-sheet publication.

## Development

Use Python 3.10 through 3.12 and install the project dependencies with Poetry. Then run:

```bash
poetry install
poetry run black bactopia_taxprofiler.py tests/test_bactopia_taxprofiler_contract.py
poetry run isort bactopia_taxprofiler.py tests/test_bactopia_taxprofiler_contract.py
poetry run pyright
poetry run python -m unittest discover -s tests -v
poetry run typos
```

## Release

Releases use conventional commits and the shared CAPE semantic-release workflow. The release archive contains exactly:

- `bactopia_taxprofiler.py`
- `meta.json`

The archive is consumed by `cape-cod-env`. Initial deployment is manual and requires owner review.

## Related projects

- [cape-cod](https://github.com/cape-ph/cape-cod) - CAPE infrastructure and pipeline profiles
- [cape-cod-env](https://github.com/cape-ph/cape-cod-env) - Airflow workflow archive deployment
