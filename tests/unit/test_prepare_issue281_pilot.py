from __future__ import annotations

import ast
import inspect
import json
import unittest

from app.lab.data_pipelines import prepare_issue281_pilot as prep

SOURCE_SHA = "c76d7671a26c07145e863ef6969b21a32ec28781"
GENERATION_TIME = "2026-10-20T12:00:00+00:00"
RUN_ID = "issue281-nonlive-pilot-preparation-20261020-v1"

FORBIDDEN_IMPORT_ROOTS = {
    "subprocess",
    "socket",
    "urllib",
    "http",
    "requests",
    "httpx",
    "boto3",
    "botocore",
    "modal",
    "paramiko",
    "websocket",
    "ftplib",
    "smtplib",
}


def _build_plan() -> dict:
    return prep.build_pilot_preparation_plan(
        source_sha=SOURCE_SHA,
        expected_source_sha=SOURCE_SHA,
        proposed_run_id=RUN_ID,
        proposed_generation_time=GENERATION_TIME,
    )


def _complete_export(plan: dict) -> dict:
    """Build a complete synthetic export fixture matching the given plan."""
    tickers_block = {}
    for ticker in plan["tickers"]:
        tickers_block[ticker] = {
            "chunks": [
                {
                    "text": "Synthetic chunk text for machine-completeness validation only.",
                    "relevance_decision": "accepted",
                    "reason_codes": ["direct_target_event"],
                    "included_in_signal": True,
                    "effective_contribution": 0.5,
                }
            ],
            "article_context": {
                "synthetic-article-1": {
                    "headline": "Synthetic headline; not real evidence.",
                    "source": "synthetic",
                }
            },
        }
    return {
        "export_identity": {
            "run_id": plan["proposed_run_id"],
            "source_sha": plan["source_sha"],
            "window": {
                "start": plan["window"]["start"],
                "end": plan["window"]["end"],
            },
            "tickers": list(plan["tickers"]),
        },
        "cached_fallback_used": False,
        "tickers": tickers_block,
    }


class SourceShaValidationTests(unittest.TestCase):
    def test_plan_rejects_mismatched_source_sha(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha="0" * 40,
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
            )

    def test_plan_rejects_uppercase_source_sha(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA.upper(),
                expected_source_sha=SOURCE_SHA.upper(),
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
            )

    def test_plan_rejects_non_hex_source_sha(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha="not-a-sha",
                expected_source_sha="not-a-sha",
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
            )


class PlanValidationTests(unittest.TestCase):
    def test_plan_rejects_rejected_packet_run_id(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=prep.REJECTED_PACKET_ID,
                proposed_generation_time=GENERATION_TIME,
            )

    def test_plan_rejects_missing_ticker(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
                tickers=("AAPL", "AMD", "NVDA"),
            )

    def test_plan_rejects_extra_ticker(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
                tickers=("AAPL", "AMD", "MSFT", "NVDA", "SPY"),
            )

    def test_plan_rejects_wrong_window(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
                window_start="2026-10-06",
                window_end="2026-10-09",
            )

    def test_plan_rejects_generation_time_not_after_cutoff(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time="2026-10-09T00:00:00+00:00",
            )


class PlanContentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = _build_plan()

    def test_plan_labels_input_cutoff_vs_generation_time(self) -> None:
        timing = self.plan["timing"]
        self.assertEqual(timing["input_cutoff_date"], "2026-10-09")
        self.assertEqual(timing["proposed_generation_time"], GENERATION_TIME)
        self.assertFalse(timing["live_freshness_proven"])

    def test_plan_marks_historical_observations_unverified(self) -> None:
        observations = self.plan["historical_input_observations"]
        self.assertEqual(len(observations), 3)
        for obs in observations:
            self.assertEqual(obs["status"], "unverified_observation")
            self.assertTrue(obs["revalidation_required"])
        keys = {(obs["observation"], obs["date"]) for obs in observations}
        self.assertIn(("missing_raw_news", "2026-10-12"), keys)
        self.assertIn(("missing_universe", "2026-10-12"), keys)
        self.assertIn(("missing_macro", "2026-10-09"), keys)

    def test_plan_artifacts_stay_in_stage_namespace(self) -> None:
        artifacts = self.plan["planned_artifacts"]
        self.assertTrue(artifacts)
        for artifact in artifacts:
            path = artifact["path"]
            self.assertTrue(path.startswith(f"{prep.STAGE_NAMESPACE}/{RUN_ID}/"))
            self.assertNotIn("..", path)
            self.assertFalse(path.startswith("features/"))
        kinds = {artifact["kind"] for artifact in artifacts}
        self.assertIn("nlp_diagnostics", kinds)
        self.assertIn("hmm_diagnostics", kinds)
        self.assertIn("export_plan", kinds)
        dates = {artifact.get("date") for artifact in artifacts if artifact.get("date")}
        self.assertEqual(dates, set(prep.CANDIDATE_WINDOW_DATES))

    def test_plan_prohibits_canonical_and_full_universe_outputs(self) -> None:
        prohibited = self.plan["prohibited"]
        self.assertEqual(
            prohibited["canonical_feature_parquet_pattern"],
            "features/{date}/{ticker}.parquet",
        )
        self.assertTrue(prohibited["full_universe_output"])
        self.assertTrue(prohibited["namespace_escape"])
        self.assertTrue(prohibited["path_traversal"])

    def test_plan_resources_are_proposal_only(self) -> None:
        resources = self.plan["resource_proposal"]
        self.assertTrue(resources["proposal_only"])
        self.assertFalse(resources["reservation"])
        self.assertFalse(resources["launch"])

    def test_plan_grants_no_authority(self) -> None:
        authority = self.plan["authority"]
        for key in (
            "execution_authorized",
            "launch_authorized",
            "semantic_acceptance",
            "pilot_readiness",
            "live_freshness",
        ):
            self.assertFalse(authority[key], key)

    def test_plan_is_new_replacement_not_resumed_bytes(self) -> None:
        self.assertEqual(self.plan["artifact_lineage"], "new_replacement")

    def test_plan_is_json_serializable(self) -> None:
        encoded = json.dumps(self.plan, sort_keys=True)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["proposed_run_id"], RUN_ID)


class StagePathValidationTests(unittest.TestCase):
    def test_path_validator_rejects_traversal(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.validate_stage_artifact_path(
                f"{prep.STAGE_NAMESPACE}/{RUN_ID}/../escape.json", run_id=RUN_ID
            )

    def test_path_validator_rejects_absolute_path(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.validate_stage_artifact_path(
                f"/{prep.STAGE_NAMESPACE}/{RUN_ID}/x.json", run_id=RUN_ID
            )

    def test_path_validator_rejects_namespace_escape(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.validate_stage_artifact_path("other-namespace/x.json", run_id=RUN_ID)

    def test_path_validator_rejects_canonical_feature_path(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.validate_stage_artifact_path("features/2026-10-09/AAPL.parquet", run_id=RUN_ID)

    def test_path_validator_accepts_stage_owned_path(self) -> None:
        path = f"{prep.STAGE_NAMESPACE}/{RUN_ID}/nlp_diagnostics/2026-10-05.json"
        self.assertEqual(prep.validate_stage_artifact_path(path, run_id=RUN_ID), path)


class ExportAssessmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = _build_plan()
        self.export = _complete_export(self.plan)

    def _rejections(self, export: dict) -> list[str]:
        return prep.assess_export_evidence(export, plan=self.plan)["rejections"]

    def test_complete_fixture_is_machine_complete_but_grants_nothing(self) -> None:
        result = prep.assess_export_evidence(self.export, plan=self.plan)
        self.assertTrue(result["machine_complete"])
        self.assertEqual(result["rejections"], [])
        for key in (
            "semantic_acceptance",
            "live_freshness",
            "execution_authority",
            "pilot_readiness",
        ):
            self.assertFalse(result["grants"][key], key)

    def test_assessment_rejects_historical_packet_substitution(self) -> None:
        self.export["export_identity"]["run_id"] = prep.REJECTED_PACKET_ID
        rejections = self._rejections(self.export)
        self.assertIn("historical_packet_substitution", rejections)
        self.assertIn("mismatched_run_id", rejections)

    def test_assessment_rejects_missing_chunk_text(self) -> None:
        self.export["tickers"]["AAPL"]["chunks"][0]["text"] = ""
        rejections = self._rejections(self.export)
        self.assertTrue(any(r.startswith("missing_chunk_text:AAPL") for r in rejections))

    def test_assessment_rejects_missing_relevance_fields(self) -> None:
        chunk = self.export["tickers"]["NVDA"]["chunks"][0]
        del chunk["relevance_decision"]
        del chunk["reason_codes"]
        del chunk["included_in_signal"]
        del chunk["effective_contribution"]
        rejections = self._rejections(self.export)
        self.assertTrue(any(r.startswith("missing_relevance_decision:NVDA") for r in rejections))
        self.assertTrue(any(r.startswith("missing_reason_codes:NVDA") for r in rejections))
        self.assertTrue(any(r.startswith("missing_inclusion_flag:NVDA") for r in rejections))
        self.assertTrue(
            any(r.startswith("missing_effective_contribution:NVDA") for r in rejections)
        )

    def test_assessment_rejects_missing_article_context(self) -> None:
        self.export["tickers"]["MSFT"]["article_context"] = {}
        rejections = self._rejections(self.export)
        self.assertIn("missing_article_context:MSFT", rejections)

    def test_assessment_rejects_cached_fallback_as_ready(self) -> None:
        self.export["cached_fallback_used"] = True
        self.assertIn("cached_fallback_used", self._rejections(self.export))

    def test_assessment_rejects_incomplete_ticker_coverage(self) -> None:
        self.export["export_identity"]["tickers"] = ["AAPL", "AMD", "NVDA"]
        self.assertIn("incomplete_ticker_coverage", self._rejections(self.export))

    def test_assessment_rejects_missing_ticker_export(self) -> None:
        del self.export["tickers"]["AMD"]
        rejections = self._rejections(self.export)
        self.assertIn("missing_ticker_export:AMD", rejections)

    def test_assessment_rejects_incomplete_export_identity(self) -> None:
        del self.export["export_identity"]["source_sha"]
        self.assertIn("incomplete_export_identity", self._rejections(self.export))

    def test_assessment_rejects_mismatched_source_run_window(self) -> None:
        for key, code in (
            ("run_id", "mismatched_run_id"),
            ("source_sha", "mismatched_source_sha"),
        ):
            export = _complete_export(self.plan)
            export["export_identity"][key] = "bogus"
            self.assertIn(code, self._rejections(export))
        export = _complete_export(self.plan)
        export["export_identity"]["window"] = {"start": "2026-10-06", "end": "2026-10-09"}
        self.assertIn("mismatched_window", self._rejections(export))

    def test_assessment_is_json_serializable(self) -> None:
        result = prep.assess_export_evidence(self.export, plan=self.plan)
        self.assertTrue(json.dumps(result))


class ModuleSafetyTests(unittest.TestCase):
    def test_module_has_no_network_subprocess_or_launch_surface(self) -> None:
        tree = ast.parse(inspect.getsource(prep))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertNotIn(alias.name.split(".")[0], FORBIDDEN_IMPORT_ROOTS)
            elif isinstance(node, ast.ImportFrom):
                self.assertIsNotNone(node.module)
                self.assertNotIn(node.module.split(".")[0], FORBIDDEN_IMPORT_ROOTS)
            elif isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    self.assertNotIn(func.id, {"eval", "exec", "print"})
                elif isinstance(func, ast.Attribute):
                    self.assertNotIn(func.attr, {"system", "popen", "run", "Popen"})


if __name__ == "__main__":
    unittest.main()
