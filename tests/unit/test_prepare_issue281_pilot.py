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

    def test_plan_rejects_malformed_ticker_arguments_deliberately(self) -> None:
        for value in (None, 1, "AAPL,AMD,MSFT,NVDA", ("AAPL", 1, "MSFT", "NVDA")):
            with self.subTest(tickers=value), self.assertRaises(prep.PilotPreparationError):
                prep.build_pilot_preparation_plan(
                    source_sha=SOURCE_SHA,
                    expected_source_sha=SOURCE_SHA,
                    proposed_run_id=RUN_ID,
                    proposed_generation_time=GENERATION_TIME,
                    tickers=value,  # type: ignore[arg-type]
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

    def test_path_validator_rejects_ambiguous_path_characters(self) -> None:
        for path in (
            f"{prep.STAGE_NAMESPACE}/{RUN_ID}/bad\\name.json",
            f"{prep.STAGE_NAMESPACE}/{RUN_ID}/bad\x00name.json",
            f"{prep.STAGE_NAMESPACE}/{RUN_ID}/ ",
            f"{prep.STAGE_NAMESPACE}/{RUN_ID}/.",
            f"{prep.STAGE_NAMESPACE}/{RUN_ID}/",
        ):
            with self.subTest(path=path), self.assertRaises(prep.PilotPreparationError):
                prep.validate_stage_artifact_path(path, run_id=RUN_ID)


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


class RunIdValidationTests(unittest.TestCase):
    """Regression tests for reviewer finding 1: unsafe run-id plan paths."""

    def _assert_run_id_rejected(self, run_id: object) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=run_id,  # type: ignore[arg-type]
                proposed_generation_time=GENERATION_TIME,
            )

    def test_plan_rejects_traversal_run_id(self) -> None:
        self._assert_run_id_rejected("../../../features")

    def test_plan_rejects_nested_run_id(self) -> None:
        self._assert_run_id_rejected("nested/run")

    def test_plan_rejects_backslash_run_id(self) -> None:
        self._assert_run_id_rejected("nested\\run")

    def test_plan_rejects_whitespace_only_run_id(self) -> None:
        self._assert_run_id_rejected("   ")

    def test_plan_rejects_run_id_with_inner_whitespace(self) -> None:
        self._assert_run_id_rejected("run id")

    def test_plan_rejects_dot_run_ids(self) -> None:
        self._assert_run_id_rejected(".")
        self._assert_run_id_rejected("..")
        self._assert_run_id_rejected("v1.2")

    def test_plan_rejects_non_string_run_id(self) -> None:
        self._assert_run_id_rejected(None)
        self._assert_run_id_rejected(123)

    def test_plan_validates_every_generated_artifact_path(self) -> None:
        plan = _build_plan()
        artifacts = plan["planned_artifacts"]
        self.assertTrue(artifacts)
        for artifact in artifacts:
            self.assertEqual(
                prep.validate_stage_artifact_path(artifact["path"], run_id=RUN_ID),
                artifact["path"],
            )

    def test_path_validator_rejects_unsafe_run_id(self) -> None:
        for bad in ("../escape", "nested/run", " ", ".", ".."):
            with self.assertRaises(prep.PilotPreparationError, msg=bad):
                prep.validate_stage_artifact_path(
                    f"{prep.STAGE_NAMESPACE}/{bad}/x.json", run_id=bad
                )


class ExactShaFormatTests(unittest.TestCase):
    """Regression tests for reviewer finding 4: exact full-string SHA check."""

    def test_plan_rejects_trailing_newline_source_sha(self) -> None:
        bad = SOURCE_SHA + "\n"
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=bad,
                expected_source_sha=bad,
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
            )

    def test_plan_rejects_non_string_source_sha(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=None,  # type: ignore[arg-type]
                expected_source_sha=None,  # type: ignore[arg-type]
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
            )

    def test_plan_rejects_non_string_expected_source_sha(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=123,  # type: ignore[arg-type]
                proposed_run_id=RUN_ID,
                proposed_generation_time=GENERATION_TIME,
            )


class GenerationTimeValidationTests(unittest.TestCase):
    """Timezone-consistent generation-vs-cutoff regression tests."""

    def test_plan_rejects_pre_cutoff_utc_despite_later_local_date(self) -> None:
        # 2026-10-10T01:00:00+02:00 == 2026-10-09T23:00:00Z, which is not
        # strictly after the 2026-10-09 input cutoff date in UTC.
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time="2026-10-10T01:00:00+02:00",
            )

    def test_plan_accepts_post_cutoff_utc_with_negative_offset(self) -> None:
        # 2026-10-09T23:30:00-05:00 == 2026-10-10T04:30:00Z, strictly after
        # the cutoff date when compared timezone-consistently in UTC.
        plan = prep.build_pilot_preparation_plan(
            source_sha=SOURCE_SHA,
            expected_source_sha=SOURCE_SHA,
            proposed_run_id=RUN_ID,
            proposed_generation_time="2026-10-09T23:30:00-05:00",
        )
        self.assertEqual(
            plan["timing"]["proposed_generation_time"], "2026-10-09T23:30:00-05:00"
        )

    def test_plan_rejects_naive_generation_time(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time="2026-10-20T12:00:00",
            )

    def test_plan_rejects_non_string_generation_time(self) -> None:
        with self.assertRaises(prep.PilotPreparationError):
            prep.build_pilot_preparation_plan(
                source_sha=SOURCE_SHA,
                expected_source_sha=SOURCE_SHA,
                proposed_run_id=RUN_ID,
                proposed_generation_time=None,  # type: ignore[arg-type]
            )


class ExportEvidenceStrictnessTests(unittest.TestCase):
    """Regression tests for reviewer finding 2: no false machine completeness."""

    def setUp(self) -> None:
        self.plan = _build_plan()

    def _rejections(self, export: dict) -> list[str]:
        return prep.assess_export_evidence(export, plan=self.plan)["rejections"]

    def test_assessment_rejects_missing_or_ambiguous_cached_fallback(self) -> None:
        export = _complete_export(self.plan)
        del export["cached_fallback_used"]
        self.assertIn("missing_cached_fallback_proof", self._rejections(export))
        for value in (None, 0, ""):
            export = _complete_export(self.plan)
            export["cached_fallback_used"] = value
            self.assertIn(
                "missing_cached_fallback_proof", self._rejections(export), value
            )

    def test_assessment_rejects_invalid_effective_contribution(self) -> None:
        for value in (None, "bogus", float("nan"), float("inf"), True, 10**10000):
            export = _complete_export(self.plan)
            export["tickers"]["AAPL"]["chunks"][0]["effective_contribution"] = value
            self.assertIn(
                "invalid_effective_contribution:AAPL", self._rejections(export), value
            )

    def test_assessment_rejects_non_boolean_inclusion_flag(self) -> None:
        for value in ("false", 1, None):
            export = _complete_export(self.plan)
            export["tickers"]["AMD"]["chunks"][0]["included_in_signal"] = value
            self.assertIn(
                "invalid_inclusion_flag:AMD", self._rejections(export), value
            )

    def test_assessment_rejects_whitespace_only_chunk_text(self) -> None:
        export = _complete_export(self.plan)
        export["tickers"]["AAPL"]["chunks"][0]["text"] = "   \n  "
        self.assertIn("missing_chunk_text:AAPL", self._rejections(export))

    def test_assessment_rejects_non_string_chunk_text(self) -> None:
        export = _complete_export(self.plan)
        export["tickers"]["AAPL"]["chunks"][0]["text"] = 42
        self.assertIn("missing_chunk_text:AAPL", self._rejections(export))

    def test_assessment_rejects_unsupported_relevance_decision(self) -> None:
        for value in (None, "bogus", 1, [], {}):
            export = _complete_export(self.plan)
            export["tickers"]["NVDA"]["chunks"][0]["relevance_decision"] = value
            self.assertIn(
                "invalid_relevance_decision:NVDA", self._rejections(export), value
            )

    def test_assessment_accepts_supported_relevance_decisions(self) -> None:
        for value in sorted(prep.SUPPORTED_RELEVANCE_DECISIONS):
            export = _complete_export(self.plan)
            export["tickers"]["NVDA"]["chunks"][0]["relevance_decision"] = value
            self.assertNotIn(
                "invalid_relevance_decision:NVDA", self._rejections(export), value
            )

    def test_assessment_rejects_invalid_reason_codes(self) -> None:
        for value in (" ", "direct_target_event", [], ["direct", " "], [1]):
            export = _complete_export(self.plan)
            export["tickers"]["AMD"]["chunks"][0]["reason_codes"] = value
            self.assertIn("invalid_reason_codes:AMD", self._rejections(export), value)


class ExportContainerValidationTests(unittest.TestCase):
    """Regression tests for reviewer finding 3: safe malformed containers."""

    def setUp(self) -> None:
        self.plan = _build_plan()

    def _rejections(self, export: object) -> list[str]:
        return prep.assess_export_evidence(export, plan=self.plan)["rejections"]  # type: ignore[arg-type]

    def test_assessment_rejects_non_dict_export_safely(self) -> None:
        for bad in (None, [], "export", 1):
            result = prep.assess_export_evidence(bad, plan=self.plan)  # type: ignore[arg-type]
            self.assertFalse(result["machine_complete"], bad)
            self.assertIn("malformed_export_container", result["rejections"], bad)
            for key in (
                "semantic_acceptance",
                "live_freshness",
                "execution_authority",
                "pilot_readiness",
            ):
                self.assertFalse(result["grants"][key], key)

    def test_assessment_rejects_malformed_plan_safely(self) -> None:
        export = _complete_export(_build_plan())
        for plan in (None, {}, {"tickers": 1}, {"tickers": ["AAPL"]}):
            result = prep.assess_export_evidence(export, plan=plan)  # type: ignore[arg-type]
            self.assertFalse(result["machine_complete"], plan)
            self.assertIn("malformed_assessment_plan", result["rejections"], plan)

    def test_assessment_rejects_forged_or_shallow_valid_plans(self) -> None:
        valid_plan = _build_plan()
        cases = (
            ("empty_tickers", {**valid_plan, "tickers": []}, {"tickers": {}}),
            ("aapl_only", {**valid_plan, "tickers": ["AAPL"]}, None),
            (
                "traversal_run_id",
                {**valid_plan, "proposed_run_id": "../../../features"},
                None,
            ),
            ("bogus_source_sha", {**valid_plan, "source_sha": "bogus"}, None),
            (
                "wrong_window",
                {**valid_plan, "window": {"start": "wrong", "end": "wrong"}},
                None,
            ),
            ("missing_window_keys", {**valid_plan, "window": {}}, None),
            (
                "malformed_nested_types",
                {**valid_plan, "window": {"start": [], "end": "2026-10-09"}},
                None,
            ),
            (
                "minimal_empty_plan",
                {
                    "tickers": [],
                    "proposed_run_id": "",
                    "source_sha": "",
                    "window": {"start": None, "end": None},
                },
                {"tickers": {}},
            ),
        )
        for label, plan, export_override in cases:
            with self.subTest(plan=label):
                export = export_override or _complete_export(valid_plan)
                if export_override is None:
                    identity = export["export_identity"]
                    identity["run_id"] = plan["proposed_run_id"]
                    identity["source_sha"] = plan["source_sha"]
                    identity["window"] = plan["window"]
                    identity["tickers"] = plan["tickers"]
                result = prep.assess_export_evidence(export, plan=plan)
                self.assertFalse(result["machine_complete"])
                self.assertEqual(result["rejections"], ["malformed_assessment_plan"])
                self.assertTrue(all(value is False for value in result["grants"].values()))

    def test_assessment_rejects_malformed_identity_tickers(self) -> None:
        for value in (None, 1, "AAPL", [["AAPL"], ["AMD"], ["MSFT"], ["NVDA"]]):
            export = _complete_export(self.plan)
            export["export_identity"]["tickers"] = value
            self.assertIn("malformed_identity_tickers", self._rejections(export), value)

    def test_assessment_rejects_dict_identity_tickers(self) -> None:
        export = _complete_export(self.plan)
        export["export_identity"]["tickers"] = {
            "AAPL": 1,
            "AMD": 1,
            "MSFT": 1,
            "NVDA": 1,
        }
        self.assertIn("malformed_identity_tickers", self._rejections(export))

    def test_assessment_rejects_duplicate_identity_tickers(self) -> None:
        export = _complete_export(self.plan)
        export["export_identity"]["tickers"] = [
            "AAPL",
            "AAPL",
            "AMD",
            "MSFT",
            "NVDA",
        ]
        self.assertIn("duplicate_ticker_entries", self._rejections(export))

    def test_assessment_rejects_malformed_chunks_container(self) -> None:
        for value in (1, "chunks", {"text": "x"}):
            export = _complete_export(self.plan)
            export["tickers"]["AAPL"]["chunks"] = value
            self.assertIn("malformed_chunks:AAPL", self._rejections(export), value)

    def test_assessment_rejects_malformed_article_context(self) -> None:
        for value in (" ", 1, ["not context"]):
            export = _complete_export(self.plan)
            export["tickers"]["MSFT"]["article_context"] = value
            self.assertIn(
                "malformed_article_context:MSFT", self._rejections(export), value
            )

    def test_assessment_rejects_invalid_article_context_entry(self) -> None:
        for value in (
            {"x": None},
            {"x": {"headline": " ", "source": "s"}},
            {"x": {"headline": "h"}},
            {" ": {"headline": "h", "source": "s"}},
        ):
            export = _complete_export(self.plan)
            export["tickers"]["MSFT"]["article_context"] = value
            self.assertIn(
                "invalid_article_context_entry:MSFT", self._rejections(export), value
            )

    def test_assessment_rejects_malformed_top_level_tickers_block(self) -> None:
        for value in (None, [], "tickers", 1):
            export = _complete_export(self.plan)
            export["tickers"] = value
            rejections = self._rejections(export)
            self.assertIn("malformed_tickers_block", rejections, value)
            for ticker in self.plan["tickers"]:
                self.assertIn(f"missing_ticker_export:{ticker}", rejections, value)


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
