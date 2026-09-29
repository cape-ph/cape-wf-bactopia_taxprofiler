# AGENTS.md

## Repository structure

CAPE workflow for Bactopia v4.1.0 and standalone taxprofiler Kraken2.

- `bactopia_taxprofiler.py` - Airflow DAG workflow definition
- `meta.json` - workflow metadata used by CAPE workflow deployment
- `tests/test_bactopia_taxprofiler_contract.py` - focused contract tests
- `README.md` - user-facing workflow documentation
- `pyproject.toml` - tool configuration and CI dependencies
- `.github/workflows/` - CI/CD automation

## Development environment

Use Python 3.10 through 3.12 and install project dependencies with Poetry:

```bash
poetry install
```

Install pre-commit separately if you want local commit hooks:

```bash
pre-commit install
```

## Quality checks

Run locally before committing:

```bash
poetry run black bactopia_taxprofiler.py tests/test_bactopia_taxprofiler_contract.py
poetry run isort bactopia_taxprofiler.py tests/test_bactopia_taxprofiler_contract.py
poetry run pyright
poetry run python -m unittest discover -s tests -v
poetry run typos
```

Pre-commit hooks run these checks automatically on git commit.

## CI/CD automation

### Pull request checks

Pull requests trigger:

- Pyright type checking (basic mode, ignores missing stubs)
- Black formatting validation (80 character lines)
- isort import sorting validation (Black-compatible profile)
- typos spell checking
- Conventional commit PR title validation

All checks must pass before merge.

### Release process

Automated via release-please:

1. Use conventional commits:
   - `feat:` - new feature (minor version bump)
   - `fix:` - bug fix (patch version bump)
   - `feat!:` or `BREAKING CHANGE:` - breaking change (major version bump)
2. Merge to main triggers semantic version calculation, changelog update, tag creation, and GitHub release.
3. The release workflow attaches a flat archive containing:
   - `bactopia_taxprofiler.py`
   - `meta.json`

The archive is the artifact consumed by `cape-cod-env` workflow deployment.
See [OPERATIONS.md](OPERATIONS.md) for release procedures.

## Workflow conventions

- The workflow is an Airflow DAG using the TaskFlow API.
- It requires Airflow 3.0.6 with the Amazon provider in the pre-built CAPE environment.
- `meta.json` defines the DAG and pipeline IDs visible to CAPE.
- The trigger contract uses `pipelineConfigs` with independent structured `nextflowOptions` dictionaries.
- The DAG converts options to the Nextflow parent `NF_OPTS` string and normalizes booleans to lowercase. It omits the false-valued taxprofiler `--kraken2_save_minimizers` option because `NF_OPTS` is a shell string and taxprofiler treats literal `false` as truthy.
- The DAG requires Bactopia `--max_cpus` and `--max_memory` resource limiters. The deployment defaults are 8 and 24.GB; callers must provide them explicitly. The DAG validates their types without imposing deployment-independent upper bounds.
- The DAG requires Bactopia `--skip_qc_plots` to be present, but preserves an explicit false value.
- Bactopia `--outdir` must use the top-level `pipeline-output` S3 key so the existing ETL can discover `bactopia-runs/<run>` output.
- The DAG copies the Bactopia input to a `.fastq.gz` key and creates the taxprofiler samplesheet for the initial implementation.
- The taxprofiler output path is caller-owned, and its final component must match the generated samplesheet sample ID until external samplesheet support is enabled. The database sheet is hard-coded to the current immutable CAPE asset for this initial implementation.
- The DAG requests the `taxprofiler-kraken2` report; Cape Cod owns the report data function and catalog contract.

## Dependencies

Dependencies in `pyproject.toml` are used for local type checking and contract tests only. They are not bundled or distributed. The workflow runs in a pre-built CAPE environment.

## Notes

- This workflow is not published to PyPI. Releases are GitHub archives.
- Do not commit or push changes without explicit owner review and approval.
- Do not deploy AWS or run `pulumi up` from this repository.

## Project Wiki

This project keeps durable knowledge in `.llm-wiki/` (an Obsidian-compatible LLM
wiki). Treat it as the source of truth for decisions, architecture, and
hard-won findings.

- At task start, read relevant pages under `.llm-wiki/wiki/`.
- At task end, record durable decisions and findings as pages under `.llm-wiki/wiki/`: one page per thing, kebab-case filenames, cross-link with `[[folder/page]]`, and cite sources.
- Never edit `.llm-wiki/raw/**` (immutable) or `.llm-wiki/meta/**` (generated index). `meta/` is gitignored and rebuilt locally.
- Commit authored `.llm-wiki/wiki/**` changes in the same commit as the code they describe. Keep generated and immutable layers out of commits.
- With the `@zosmaai/pi-llm-wiki` extension, prefer its wiki tools because they maintain metadata automatically.
