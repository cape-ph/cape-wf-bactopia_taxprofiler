"""Focused contract tests for the Bactopia/taxprofiler DAG."""

import csv
import importlib.util
import io
import json
import unittest
from pathlib import Path

DAG_PATH = Path(__file__).parents[1] / "bactopia_taxprofiler.py"


def load_dag_module():
    spec = importlib.util.spec_from_file_location(
        "bactopia_taxprofiler_contract", DAG_PATH
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load DAG module from {DAG_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def bactopia_options(**overrides):
    options = {
        "-profile": "docker",
        "--max_cpus": 8,
        "--max_memory": "24.GB",
        "--ont": "s3://input/sample.fastq.gz",
        "--sample": "bactopia-sample",
        "--outdir": "s3://result/pipeline-output",
        "--skip_qc_plots": True,
    }
    options.update(overrides)
    return options


def taxprofiler_options(**overrides):
    options = {
        "-profile": "docker",
        "--outdir": "s3://result/taxprofiler-output/bactopia-sample",
        "--run_kraken2": True,
        "--kraken2_save_minimizers": False,
    }
    options.update(overrides)
    return options


class BactopiaTaxprofilerContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dag_module = load_dag_module()

    def test_cli_string_renders_boolean_values_as_lowercase(self):
        result = self.dag_module.nextflow_options_to_cli_string(
            {"--skip_qc_plots": True, "--other": False, "--empty": ""}
        )

        self.assertEqual(result, "--skip_qc_plots true --other false")

    def test_invoke_report_lambda_reads_payload(self):
        class FakeLambdaClient:
            def invoke(self, **kwargs):
                return {
                    "Payload": io.BytesIO(
                        json.dumps(
                            {"statusCode": 200, "body": "<html>report</html>"}
                        ).encode("utf-8")
                    )
                }

        result = self.dag_module.invoke_report_lambda(
            FakeLambdaClient(), "bactopia-sample"
        )

        self.assertEqual(result, (200, "<html>report</html>"))

    def test_bactopia_requires_skip_qc_plots_but_preserves_false(self):
        with self.assertRaisesRegex(ValueError, "skip_qc_plots"):
            self.dag_module._validate_pipeline_options(
                "bactopia", bactopia_options(**{"--skip_qc_plots": None})
            )

        options = self.dag_module._validate_pipeline_options(
            "bactopia", bactopia_options(**{"--skip_qc_plots": False})
        )

        self.assertIs(options["--skip_qc_plots"], False)

    def test_bactopia_requires_resource_limiters(self):
        for key in ("--max_cpus", "--max_memory"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                options = bactopia_options()
                options.pop(key)
                self.dag_module._validate_pipeline_options("bactopia", options)

        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.dag_module._validate_pipeline_options(
                "bactopia", bactopia_options(**{"--max_cpus": "8"})
            )

        with self.assertRaisesRegex(ValueError, "memory value"):
            self.dag_module._validate_pipeline_options(
                "bactopia", bactopia_options(**{"--max_memory": "24"})
            )

    def test_bactopia_requires_etl_output_root(self):
        with self.assertRaisesRegex(ValueError, "pipeline-output"):
            self.dag_module._validate_pipeline_options(
                "bactopia",
                bactopia_options(
                    **{"--outdir": "s3://result/pipeline-output/run-001"}
                ),
            )

    def test_taxprofiler_owns_samplesheet_input_and_database(self):
        options = self.dag_module._validate_pipeline_options(
            "taxprofiler",
            taxprofiler_options(**{"--input": "", "--databases": ""}),
        )
        cli = self.dag_module.nextflow_options_to_cli_string(
            options,
            excluded_keys={"--input", "--databases"},
            omit_false_keys={"--kraken2_save_minimizers"},
        )

        self.assertIs(options["--skip_preprocessing_qc"], True)
        self.assertNotIn("--input", cli)
        self.assertNotIn("--databases", cli)
        self.assertNotIn("--kraken2_save_minimizers", cli)

    def test_taxprofiler_rejects_external_inputs_during_transition(self):
        with self.assertRaisesRegex(ValueError, "--input.*DAG-owned"):
            self.dag_module._validate_pipeline_options(
                "taxprofiler",
                taxprofiler_options(**{"--input": "s3://input/sheet.csv"}),
            )

        with self.assertRaisesRegex(ValueError, "--databases.*DAG-owned"):
            self.dag_module._validate_pipeline_options(
                "taxprofiler",
                taxprofiler_options(
                    **{"--databases": "s3://meta/database.csv"}
                ),
            )

    def test_taxprofiler_rejects_disabled_kraken2(self):
        with self.assertRaisesRegex(ValueError, "run_kraken2.*true"):
            self.dag_module._validate_pipeline_options(
                "taxprofiler",
                taxprofiler_options(**{"--run_kraken2": False}),
            )

    def test_taxprofiler_samplesheet_reuses_bactopia_sample_temporarily(self):
        content = self.dag_module.render_taxprofiler_samplesheet(
            "bactopia-sample",
            "bactopia-sample",
            "s3://input/sequencing-reads.gz",
        )
        rows = list(csv.reader(content.splitlines()))

        self.assertEqual(
            rows,
            [
                [
                    "sample",
                    "run_accession",
                    "instrument_platform",
                    "fastq_1",
                    "fastq_2",
                    "fasta",
                ],
                [
                    "bactopia-sample",
                    "bactopia-sample",
                    "OXFORD_NANOPORE",
                    "s3://input/sequencing-reads.gz",
                    "",
                    "",
                ],
            ],
        )

    def test_s3_helper_preserves_caller_output_bucket(self):
        output_path = "s3://result/taxprofiler-output/run-001"

        self.assertEqual(
            self.dag_module.extract_s3_bucket_name(output_path), "result"
        )
        self.assertEqual(
            self.dag_module.extract_s3_leaf_name(output_path), "run-001"
        )

    def test_taxprofiler_fastq_uri_adds_required_suffix(self):
        source_uri = "s3://input/sequencing-reads.gz"

        self.assertEqual(
            self.dag_module.taxprofiler_fastq_uri(source_uri),
            "s3://input/sequencing-reads.fastq.gz",
        )
        self.assertEqual(
            self.dag_module.taxprofiler_fastq_uri(
                "s3://input/sequencing-reads.fastq.gz"
            ),
            "s3://input/sequencing-reads.fastq.gz",
        )

    def test_full_config_validation_renders_both_stages(self):
        config = {
            "pipelineConfigs": [
                {
                    "pipelineId": "bactopia-ont-v4.1.0",
                    "nextflowOptions": bactopia_options(),
                },
                {
                    "pipelineId": "taxprofiler-kraken2-2.0.1",
                    "nextflowOptions": taxprofiler_options(),
                },
            ]
        }
        result = self.dag_module.validate_and_extract_nextflow_configs.function(
            dag_run=type("DagRun", (), {"conf": config})()
        )

        self.assertIn(
            "--skip_qc_plots true", result["bactopia"]["nextflowOptionsCli"]
        )
        self.assertIn(
            "--skip_preprocessing_qc true",
            result["taxprofiler"]["nextflowOptionsCli"],
        )
        self.assertNotIn(
            "--kraken2_save_minimizers",
            result["taxprofiler"]["nextflowOptionsCli"],
        )
        self.assertEqual(
            set(result["taxprofiler"]),
            {"pipelineId", "nextflowOptions", "nextflowOptionsCli"},
        )

    def test_full_config_validation_requires_matching_sample_and_output_leaf(
        self,
    ):
        config = {
            "pipelineConfigs": [
                {
                    "pipelineId": "bactopia-ont-v4.1.0",
                    "nextflowOptions": bactopia_options(),
                },
                {
                    "pipelineId": "taxprofiler-kraken2-2.0.1",
                    "nextflowOptions": taxprofiler_options(
                        **{"--outdir": "s3://result/taxprofiler-output/run-001"}
                    ),
                },
            ]
        }

        with self.assertRaisesRegex(ValueError, "must match.*samplesheet"):
            self.dag_module.validate_and_extract_nextflow_configs.function(
                dag_run=type("DagRun", (), {"conf": config})()
            )

    def test_taxprofiler_resource_policy_is_submitted_separately(self):
        self.assertEqual(
            self.dag_module.TAXPROFILER_REPORT_ID, "taxprofiler-kraken2"
        )
        self.assertEqual(
            self.dag_module.REPORT_OUTPUT_BUCKET,
            "ccd-dlh-t-seqauto-artifacts-vbkt-s3-d2421eb",
        )
        dag = self.dag_module.bactopia_taxprofiler()
        task = dag.get_task("submit_taxprofiler_batch_job")
        environment = {
            item["name"]: item["value"]
            for item in task.container_overrides["environment"]
        }

        self.assertEqual(
            json.loads(environment["NEXTFLOW_PROCESS_OVERRIDES"]),
            self.dag_module.TAXPROFILER_PROCESS_OVERRIDES,
        )
        self.assertNotIn("cpus", environment["NF_OPTS"])
        self.assertNotIn("memory", environment["NF_OPTS"])

    def test_dag_uses_fastq_alias_and_samplesheet_dependencies(
        self,
    ):
        dag = self.dag_module.bactopia_taxprofiler()

        self.assertNotIn("create_k2_include", dag.task_ids)
        self.assertNotIn("wait_for_kraken_2_include_file", dag.task_ids)
        self.assertNotIn("wait_for_bactopia_qc_output", dag.task_ids)
        self.assertIn("copy_taxprofiler_input_to_fastq_gz", dag.task_ids)
        self.assertIn("create_taxprofiler_samplesheet", dag.task_ids)
        self.assertIn("submit_taxprofiler_batch_job", dag.task_ids)
        self.assertIn("generate_and_store_taxprofiler_report", dag.task_ids)

        bactopia_task = dag.get_task("submit_bactopia_batch_job")
        copy_task = dag.get_task("copy_taxprofiler_input_to_fastq_gz")
        samplesheet_task = dag.get_task("create_taxprofiler_samplesheet")
        self.assertIn(
            "copy_taxprofiler_input_to_fastq_gz",
            bactopia_task.downstream_task_ids,
        )
        self.assertIn(
            "create_taxprofiler_samplesheet",
            copy_task.downstream_task_ids,
        )
        self.assertIn(
            "submit_taxprofiler_batch_job",
            samplesheet_task.downstream_task_ids,
        )
        taxprofiler_task = dag.get_task("submit_taxprofiler_batch_job")
        self.assertIn(
            "generate_and_store_taxprofiler_report",
            taxprofiler_task.downstream_task_ids,
        )


if __name__ == "__main__":
    unittest.main()
