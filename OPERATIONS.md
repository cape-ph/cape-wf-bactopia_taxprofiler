# Operations Guide

Operational procedures for the CAPE Bactopia v4.1.0 and taxprofiler workflow.

## Release management

Releases use conventional commits:

- `feat:` - new feature
- `fix:` - bug fix
- `feat!:` or `BREAKING CHANGE:` - breaking change

Normal flow:

1. Create a branch from main.
2. Make and locally validate changes.
3. Open a pull request.
4. Merge after review and CI success.
5. The shared release workflow creates the version, tag, GitHub release, and workflow archive.

The archive must contain exactly these two flat files:

```text
bactopia_taxprofiler.py
meta.json
```

Expected archive naming:

```text
cape-wf-bactopia_taxprofiler-v<version>.zip
```

The `cape-cod-env` workflow deployment role downloads the archive, uploads the DAG to the Airflow S3 prefix, and writes the `meta.json` identity to the workflow registry. Updating the development release URL is a separate manually reviewed change.

## Local validation

Use Python 3.10 through 3.12 and install the project dependencies with Poetry:

```bash
poetry install
poetry run black bactopia_taxprofiler.py tests/test_bactopia_taxprofiler_contract.py
poetry run isort bactopia_taxprofiler.py tests/test_bactopia_taxprofiler_contract.py
poetry run pyright
poetry run python -m unittest discover -s tests -v
poetry run typos
```

Validate the release shape without publishing it:

```bash
zip -j /tmp/cape-wf-bactopia_taxprofiler.zip bactopia_taxprofiler.py meta.json
unzip -l /tmp/cape-wf-bactopia_taxprofiler.zip
```

The archive must contain only one DAG file and `meta.json`.

## Manual workflow testing

Before testing, confirm that the caller sends:

- `dagId=bactopia_taxprofiler`.
- Both expected pipeline IDs.
- Bactopia `--max_cpus: 8` and `--max_memory: 24.GB` are present as resource limiters. Callers must provide these values explicitly. The DAG rejects missing or incorrectly typed values, but allows deployment-supported values.
- Bactopia `--outdir` rooted at the top-level `pipeline-output` S3 key. Bactopia creates `bactopia-runs/<run>` below that root for the existing ETL contract.
- Bactopia `--skip_qc_plots` as a boolean. If absent, DAG validation rejects the configuration. `true` skips QC plots; `false` preserves the full QC plot path.
- Taxprofiler `--run_kraken2: true` and `--kraken2_save_minimizers: false` as JSON booleans. The DAG validates the false setting and omits that option from `NF_OPTS`; do not send the value as a string.
- Bactopia `--sample`, which temporarily populates both taxprofiler `sample` and `run_accession` in the generated samplesheet.
- A taxprofiler `--outdir` whose final component matches the generated samplesheet sample ID. Use a unique sample ID for each retained run; the current ETL uses this component as its sample partition.

The transitional DAG owns the samplesheet and database sheet. It rejects non-empty `--input` or `--databases` values until the external-caller cutover is implemented.

Do not run `pulumi up`, deploy, destroy, or delete resources from this repository. Keep all commits, pushes, GitHub repository creation, and release publication behind explicit owner approval.

## Reporting boundary

The current DAG preserves the Bactopia crawler/report branch and requests the `taxprofiler-kraken2` report after taxprofiler completes. Taxprofiler native S3 output remains available alongside the report. Cape Cod owns the report data function and catalog contract.
