from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime, timedelta

import pytest

from quant_data_kit.financial import (
    AdmissionBoundaryV2,
    AvailabilityDecisionV2,
    DowngradeReasonV2,
    EvidenceRevisionV2,
    EvidenceTimingBuildResultV2,
    EvidenceTimingV2,
    NominalPublicationIntervalV2,
    PhysicalClockAccuracyV2,
    PublicationDisplayRuleV2,
    PublicationZoneEvidenceV2,
    SourceMaterialReferenceV2,
    SourcePublicationV2,
    build_evidence_timing_v2,
)


def material(
    material_id: str,
    *,
    acquired_at: str = "2026-10-04T00:00:00Z",
    kind: str = "official_document",
) -> SourceMaterialReferenceV2:
    return SourceMaterialReferenceV2(
        material_id=material_id,
        kind=kind,
        archive_version="frozen-v1",
        sha256=("a" if material_id == "zone" else "b") * 64,
        locator=f"local:{material_id}:page-1",
        review_citation=f"{material_id} section 1",
        acquired_at=acquired_at,
    )


def minute_result(
    local_minute: str,
    *,
    display_rule: str | None = "truncated_minute",
    parity_reference: str | None = None,
    zone_name: str | None = None,
    offset: str | None = "+00:00",
    fold: int | None = None,
    captured_at: str = "2026-10-04T00:00:00Z",
    source_materials: tuple[SourceMaterialReferenceV2, ...] | None = None,
    acquired_at: str | None = None,
    study_mode: str = "retrospective",
    trust_model: str = "trust_source_declared_time",
) -> EvidenceTimingBuildResultV2:
    materials = source_materials or (material("zone"), material("display"))
    return build_evidence_timing_v2(
        effective_at="2026-12-01T00:00:00Z",
        captured_at=captured_at,
        raw_text=local_minute,
        precision="minute",
        revision_id="revision-1",
        source_materials=materials,
        local_minute=local_minute,
        zone_status="evidenced_iana" if zone_name else "evidenced_offset",
        iana_name=zone_name,
        utc_offset=offset,
        fold=fold,
        zone_material_ids=("zone",),
        display_rule=display_rule,
        parity_reference=parity_reference,
        display_material_ids=("display",) if display_rule else (),
        acquired_at=acquired_at,
        study_mode=study_mode,
        trust_model=trust_model,
    )


def reason_codes(result: EvidenceTimingBuildResultV2) -> set[str]:
    return {item.code for item in result.downgrade_reasons}


def test_a3_unverified_minute_is_captured_only_without_invented_offset():
    result = build_evidence_timing_v2(
        effective_at="2026-07-02T01:30:00Z",
        captured_at="2026-10-04T06:30:00Z",
        raw_text="13/07/2026 17:59",
        precision="minute",
        revision_id="hkex-capture",
        local_minute="2026-07-13T17:59",
    )

    assert result.timing.availability.mode == "captured_only"
    assert result.timing.source_publication.zone.status == "unverified"
    assert result.timing.source_publication.nominal_interval is None
    assert "ZONE_UNVERIFIED" in reason_codes(result)
    assert result.timing.is_admitted(
        "2026-10-04T06:30:00Z",
        study_mode="natural_forward",
        trust_model="capture_receipt_only",
    )


def test_a4_evidenced_zone_without_display_rule_stays_captured_only():
    result = minute_result("2026-09-01T12:00", display_rule=None)

    assert result.timing.availability.mode == "captured_only"
    assert "DISPLAY_RULE_UNVERIFIED" in reason_codes(result)


def test_a5_truncation_uses_nominal_boundary_with_unknown_physical_clock():
    result = minute_result("2026-09-01T12:00")
    timing = result.timing
    interval = timing.source_publication.nominal_interval

    assert result.downgrade_reasons == ()
    assert interval.earliest == "2026-09-01T12:00:00Z"
    assert interval.latest == "2026-09-01T12:01:00Z"
    assert interval.latest_inclusive is False
    assert timing.availability.admission_relation == "at_or_after"
    assert timing.availability.physical_clock_accuracy == "unknown"
    assert "physical clock accuracy is unknown" in timing.availability.risk_note
    assert timing.is_admitted(
        interval.latest,
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
    )
    assert (
        timing.admission_boundary(
            study_mode="natural_forward", trust_model="capture_receipt_only"
        ).boundary_at
        == "2026-10-04T00:00:00Z"
    )


def test_e1_e2_closed_upper_requires_strictly_after_without_epsilon():
    result = minute_result("2026-09-01T12:00", display_rule="rounded_half_down")
    timing = result.timing
    interval = timing.source_publication.nominal_interval

    assert interval.latest_inclusive is True
    assert timing.availability.admission_relation == "strictly_after"
    assert not timing.is_admitted(
        interval.latest,
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
    )
    later = datetime.fromisoformat(interval.latest.replace("Z", "+00:00")) + timedelta(seconds=1)
    assert timing.is_admitted(
        later,
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
    )
    assert "epsilon" not in timing.to_json()


@pytest.mark.parametrize(
    ("minute", "inclusive"),
    [("2026-09-01T12:02", True), ("2026-09-01T12:01", False)],
)
def test_e3_half_even_records_both_endpoint_flags(minute, inclusive):
    result = minute_result(
        minute,
        display_rule="rounded_half_even",
        parity_reference="minute_of_hour",
    )
    interval = result.timing.source_publication.nominal_interval

    assert interval.earliest_inclusive is inclusive
    assert interval.latest_inclusive is inclusive
    assert result.timing.availability.admission_relation == (
        "strictly_after" if inclusive else "at_or_after"
    )


def test_d3_half_even_without_parity_reference_downgrades():
    result = minute_result("2026-09-01T12:02", display_rule="rounded_half_even")

    assert result.timing.availability.mode == "captured_only"
    assert "ROUNDING_REFERENCE_UNVERIFIED" in reason_codes(result)


def test_a6_date_without_publication_semantics_is_captured_only():
    materials = (material("zone"),)
    result = build_evidence_timing_v2(
        effective_at="2026-12-01T00:00:00Z",
        captured_at="2026-10-04T00:00:00Z",
        raw_text="2026-03-08",
        precision="date",
        revision_id="date-no-semantics",
        source_materials=materials,
        local_date="2026-03-08",
        zone_status="evidenced_iana",
        iana_name="America/New_York",
        zone_material_ids=("zone",),
    )

    assert result.timing.availability.mode == "captured_only"
    assert "PUBLICATION_DATE_SEMANTICS_UNVERIFIED" in reason_codes(result)


@pytest.mark.parametrize(
    ("local_date", "duration_hours"),
    [("2026-03-08", 23), ("2026-11-01", 25)],
)
def test_a7_date_interval_uses_adjacent_local_midnights(local_date, duration_hours):
    materials = (material("zone"), material("display"))
    result = build_evidence_timing_v2(
        effective_at="2026-12-01T00:00:00Z",
        captured_at="2026-12-02T00:00:00Z",
        raw_text=local_date,
        precision="date",
        revision_id=f"date-{local_date}",
        source_materials=materials,
        local_date=local_date,
        zone_status="evidenced_iana",
        iana_name="America/New_York",
        zone_material_ids=("zone",),
        display_rule="publication_date",
        display_material_ids=("display",),
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
    )
    interval = result.timing.source_publication.nominal_interval
    earliest = datetime.fromisoformat(interval.earliest.replace("Z", "+00:00"))
    latest = datetime.fromisoformat(interval.latest.replace("Z", "+00:00"))

    assert latest - earliest == timedelta(hours=duration_hours)
    assert interval.latest_inclusive is False


@pytest.mark.parametrize(
    ("local_date", "zone", "code"),
    [
        ("2020-11-01", "America/Havana", "DATE_BOUNDARY_AMBIGUOUS"),
        ("2011-12-29", "Pacific/Apia", "DATE_BOUNDARY_NONEXISTENT"),
    ],
)
def test_e6_ambiguous_or_nonexistent_midnight_downgrades(local_date, zone, code):
    materials = (material("zone"), material("display"))
    result = build_evidence_timing_v2(
        effective_at="2026-12-01T00:00:00Z",
        captured_at="2026-12-02T00:00:00Z",
        raw_text=local_date,
        precision="date",
        revision_id=f"date-boundary-{code}",
        source_materials=materials,
        local_date=local_date,
        zone_status="evidenced_iana",
        iana_name=zone,
        zone_material_ids=("zone",),
        display_rule="publication_date",
        display_material_ids=("display",),
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
    )

    assert result.timing.availability.mode == "captured_only"
    assert code in reason_codes(result)


def test_a8_explicit_interval_preserves_inclusive_upper_relation():
    material_ref = material("display")
    interval = NominalPublicationIntervalV2(
        "2026-07-01T00:00:00Z",
        True,
        "2026-07-01T00:01:00Z",
        True,
        "explicit_interval",
        ("display",),
    )
    result = build_evidence_timing_v2(
        effective_at="2026-07-02T00:00:00Z",
        captured_at="2026-07-01T01:00:00Z",
        raw_text="archive interval",
        precision="interval",
        revision_id="interval",
        source_materials=(material_ref,),
        explicit_interval=interval,
        display_rule="explicit_interval",
        display_material_ids=("display",),
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
    )

    assert result.timing.availability.boundary_at == interval.latest
    assert result.timing.availability.admission_relation == "strictly_after"


def test_a8_interval_rejects_empty_or_reversed_bounds():
    with pytest.raises(ValueError, match="positive duration"):
        NominalPublicationIntervalV2(
            "2026-07-01T00:00:00Z",
            True,
            "2026-07-01T00:00:00Z",
            False,
            "explicit_interval",
            ("display",),
        )


def test_a9_e4_dst_fold_is_resolved_before_absolute_minute_arithmetic():
    result = minute_result(
        "2026-11-01T01:59",
        zone_name="America/New_York",
        offset=None,
        fold=0,
        captured_at="2026-11-02T00:00:00Z",
    )
    interval = result.timing.source_publication.nominal_interval

    assert interval.earliest == "2026-11-01T05:59:00Z"
    assert interval.latest == "2026-11-01T06:00:00Z"
    assert (
        datetime.fromisoformat(interval.latest.replace("Z", "+00:00"))
        .astimezone(__import__("zoneinfo").ZoneInfo("America/New_York"))
        .strftime("%H:%M %z")
        == "01:00 -0500"
    )


def test_a9_ambiguous_and_nonexistent_minutes_downgrade_with_specific_reasons():
    ambiguous = minute_result(
        "2026-11-01T01:30",
        zone_name="America/New_York",
        offset=None,
        captured_at="2026-11-02T00:00:00Z",
    )
    nonexistent = minute_result(
        "2026-03-08T02:30",
        zone_name="America/New_York",
        offset=None,
        captured_at="2026-11-02T00:00:00Z",
    )

    assert "DST_LOCAL_TIME_AMBIGUOUS" in reason_codes(ambiguous)
    assert "DST_LOCAL_TIME_NONEXISTENT" in reason_codes(nonexistent)


def test_a9_conflicting_iana_offset_downgrades_as_material_conflict():
    result = minute_result(
        "2026-07-01T12:00",
        zone_name="America/New_York",
        offset="+09:00",
        captured_at="2026-11-02T00:00:00Z",
    )

    assert "MATERIAL_REFERENCE_CONFLICT" in reason_codes(result)


def test_a11_e8_later_material_revision_never_backfills_natural_forward():
    materials = (
        material("zone", acquired_at="2026-10-05T00:00:00Z"),
        material("display", acquired_at="2026-10-06T00:00:00Z"),
    )
    result = minute_result(
        "2026-07-01T12:00",
        captured_at="2026-10-04T00:00:00Z",
        source_materials=materials,
        acquired_at="2026-10-05T12:00:00Z",
    )
    timing = result.timing

    assert timing.availability.usage_scope == "retrospective_only"
    assert timing.revision.acquired_at == "2026-10-06T00:00:00Z"
    assert (
        timing.admission_boundary(
            study_mode="natural_forward", trust_model="capture_receipt_only"
        ).boundary_at
        == "2026-10-06T00:00:00Z"
    )
    assert (
        timing.admission_boundary(
            study_mode="retrospective", trust_model="trust_source_declared_time"
        ).boundary_at
        == "2026-07-01T12:01:00Z"
    )


def test_a13_nominal_boundary_after_capture_builder_downgrades_and_parser_rejects():
    result = minute_result(
        "2026-09-01T12:00",
        captured_at="2026-09-01T12:00:30Z",
    )
    assert result.timing.availability.mode == "captured_only"
    assert "NOMINAL_BOUND_AFTER_CAPTURE" in reason_codes(result)

    valid = minute_result("2026-09-01T12:00").timing.to_dict()
    valid["captured_at"] = "2026-09-01T12:00:30Z"
    with pytest.raises(ValueError, match="cannot follow captured_at"):
        EvidenceTimingV2.from_dict(valid)


def test_a14_e9_strict_parser_rejects_incomplete_evidenced_claim_builder_records_reason():
    built = build_evidence_timing_v2(
        effective_at="2026-12-01T00:00:00Z",
        captured_at="2026-10-04T00:00:00Z",
        raw_text="2026-09-01T12:00",
        precision="minute",
        revision_id="incomplete",
        local_minute="2026-09-01T12:00",
        zone_status="evidenced_offset",
        utc_offset="+08:00",
        zone_material_ids=("missing",),
    )
    assert "MATERIAL_REFERENCE_INCOMPLETE" in reason_codes(built)

    payload = built.timing.to_dict()
    payload["source_publication"]["zone"] = {
        "status": "evidenced_offset",
        "iana_name": None,
        "utc_offset": "+08:00",
        "fold": None,
        "material_ids": ["missing"],
    }
    with pytest.raises(ValueError, match="dangling"):
        EvidenceTimingV2.from_dict(payload)


def test_a14_closed_world_parsers_and_material_fields_fail_closed():
    result = minute_result("2026-09-01T12:00")
    payload = result.timing.to_dict()
    payload["unknown"] = True
    with pytest.raises(ValueError, match="unsupported fields"):
        EvidenceTimingV2.from_dict(payload)
    with pytest.raises(ValueError, match="source material kind"):
        SourceMaterialReferenceV2("x", "web", "v", "0" * 64, "l", "c", "2026-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="64 lowercase"):
        SourceMaterialReferenceV2(
            "x", "official_document", "v", "A" * 64, "l", "c", "2026-01-01T00:00:00Z"
        )


def test_round_trip_fingerprint_and_admission_input_validation():
    timing = minute_result("2026-09-01T12:00").timing
    restored = EvidenceTimingV2.from_json(timing.to_json())

    assert restored == timing
    assert restored.fingerprint() == timing.fingerprint()
    with pytest.raises(ValueError, match="study_mode"):
        timing.admission_boundary(study_mode="live", trust_model="capture_receipt_only")
    with pytest.raises(ValueError, match="trust_model"):
        timing.admission_boundary(study_mode="retrospective", trust_model="implicit")
    with pytest.raises(ValueError, match="requires trust_source"):
        timing.admission_boundary(study_mode="retrospective", trust_model="capture_receipt_only")


def test_value_object_negative_branches_are_closed():
    with pytest.raises(ValueError, match="positive duration"):
        NominalPublicationIntervalV2(
            "2026-01-02T00:00:00Z", True, "2026-01-01T00:00:00Z", False, "publication_date", ("x",)
        )
    with pytest.raises(ValueError, match="fold"):
        PublicationZoneEvidenceV2("evidenced_iana", "UTC", None, 2, ("x",))
    with pytest.raises(ValueError, match="half-even"):
        PublicationDisplayRuleV2("evidenced", "rounded_half_even", None, ("x",))
    with pytest.raises(ValueError, match="nonnegative bound"):
        PhysicalClockAccuracyV2("evidenced", None, ("x",))
    with pytest.raises(ValueError, match="reason code"):
        DowngradeReasonV2("OTHER", "field", (), "detail")
    with pytest.raises(ValueError, match="relation"):
        AdmissionBoundaryV2("2026-01-01T00:00:00Z", "equal")


def test_manual_captured_only_invariants_and_build_result_binding():
    reason = DowngradeReasonV2("PRECISION_UNKNOWN", "precision", (), "unknown")
    with pytest.raises(ValueError, match="captured_only"):
        AvailabilityDecisionV2(
            "2026-01-01T00:00:00Z",
            "strictly_after",
            "captured_only",
            "local_capture",
            "capture_receipt_only",
            "forward_and_retrospective",
            "unknown",
            None,
            "1",
            (),
            (reason,),
        )
    timing = build_evidence_timing_v2(
        effective_at="2026-01-01T00:00:00Z",
        captured_at="2026-01-02T00:00:00Z",
        raw_text="",
        precision="unknown",
        revision_id="unknown",
    ).timing
    with pytest.raises(ValueError, match="must match"):
        EvidenceTimingBuildResultV2(timing, ())


def test_revision_rejects_invalid_supersedes_and_material_after_revision():
    with pytest.raises(ValueError, match="lowercase SHA"):
        EvidenceRevisionV2("r", "X" * 64, "2026-01-01T00:00:00Z", "retrospective_only")
    timing = minute_result("2026-09-01T12:00").timing.to_dict()
    timing["revision"]["acquired_at"] = "2026-01-01T00:00:00Z"
    with pytest.raises(ValueError, match="cannot precede captured_at"):
        EvidenceTimingV2.from_dict(timing)


def test_parser_rejects_mutated_interval_relation_and_unknown_precision_shape():
    payload = minute_result("2026-09-01T12:00").timing.to_dict()
    payload["availability"]["admission_relation"] = "strictly_after"
    with pytest.raises(ValueError, match="inconsistent upper boundary"):
        EvidenceTimingV2.from_dict(payload)

    unknown = build_evidence_timing_v2(
        effective_at="2026-01-01T00:00:00Z",
        captured_at="2026-01-02T00:00:00Z",
        raw_text="",
        precision="unknown",
        revision_id="u",
    ).timing.to_dict()
    unknown["source_publication"]["local_date"] = "2026-01-01"
    with pytest.raises(ValueError, match="unknown precision"):
        EvidenceTimingV2.from_dict(unknown)


def test_captured_payload_with_complete_evidence_can_choose_capture_policy():
    result = minute_result(
        "2026-09-01T12:00",
        study_mode="natural_forward",
        trust_model="capture_receipt_only",
    )
    assert result.timing.source_publication.nominal_interval is not None
    assert result.timing.availability.mode == "captured_only"
    assert reason_codes(result) == {"POLICY_CAPTURE_ONLY"}
    assert EvidenceTimingV2.from_dict(copy.deepcopy(result.timing.to_dict())) == result.timing


def test_leaf_parsers_reject_wrong_container_missing_fields_and_bad_scalars():
    with pytest.raises(TypeError, match="must be an object"):
        SourceMaterialReferenceV2.from_dict([])
    with pytest.raises(ValueError, match="missing fields"):
        SourceMaterialReferenceV2.from_dict({})
    with pytest.raises(TypeError, match="nonempty string"):
        SourceMaterialReferenceV2(
            "", "official_document", "v", "a" * 64, "l", "c", "2026-01-01T00:00:00Z"
        )
    with pytest.raises(TypeError, match="timestamp string"):
        SourceMaterialReferenceV2("x", "official_document", "v", "a" * 64, "l", "c", 1)
    with pytest.raises(ValueError, match="YYYY-MM-DDTHH:MM"):
        build_evidence_timing_v2(
            effective_at="2026-01-01T00:00:00Z",
            captured_at="2026-01-02T00:00:00Z",
            raw_text="bad",
            precision="minute",
            revision_id="r",
            local_minute="bad",
        )
    with pytest.raises(ValueError, match="plain ISO"):
        build_evidence_timing_v2(
            effective_at="2026-01-01T00:00:00Z",
            captured_at="2026-01-02T00:00:00Z",
            raw_text="bad",
            precision="date",
            revision_id="r",
            local_date="bad",
        )
    with pytest.raises(ValueError, match="plain ISO"):
        build_evidence_timing_v2(
            effective_at="2026-01-01T00:00:00Z",
            captured_at="2026-01-02T00:00:00Z",
            raw_text="20260101",
            precision="date",
            revision_id="r",
            local_date="20260101",
        )


def test_zone_display_clock_and_interval_value_objects_fail_closed():
    with pytest.raises(ValueError, match="zone status"):
        PublicationZoneEvidenceV2("claimed")
    with pytest.raises(TypeError, match="array of strings"):
        PublicationZoneEvidenceV2("unverified", material_ids="x")
    with pytest.raises(ValueError, match="unique"):
        PublicationZoneEvidenceV2("unverified", material_ids=("x", "x"))
    with pytest.raises(ValueError, match="canonical signed"):
        PublicationZoneEvidenceV2("evidenced_offset", utc_offset="UTC", material_ids=("x",))
    with pytest.raises(ValueError, match="supported UTC offset"):
        PublicationZoneEvidenceV2("evidenced_offset", utc_offset="+24:00", material_ids=("x",))
    negative = PublicationZoneEvidenceV2(
        "evidenced_offset", utc_offset="-05:00", material_ids=("x",)
    )
    assert negative.utc_offset == "-05:00"
    with pytest.raises(ValueError, match="requires a name"):
        PublicationZoneEvidenceV2("evidenced_iana", material_ids=("x",))
    with pytest.raises(ValueError, match="installed IANA"):
        PublicationZoneEvidenceV2("evidenced_iana", "Mars/Olympus", material_ids=("x",))
    with pytest.raises(ValueError, match="requires only an offset"):
        PublicationZoneEvidenceV2("evidenced_offset", "UTC", "+00:00", None, ("x",))
    with pytest.raises(ValueError, match="cannot carry zone claims"):
        PublicationZoneEvidenceV2("unverified", utc_offset="+00:00")

    with pytest.raises(ValueError, match="display rule status"):
        PublicationDisplayRuleV2("claimed")
    with pytest.raises(ValueError, match="supported value"):
        PublicationDisplayRuleV2("evidenced", "invented", None, ("x",))
    with pytest.raises(ValueError, match="only valid for half-even"):
        PublicationDisplayRuleV2("evidenced", "exact", "minute_of_hour", ("x",))
    with pytest.raises(ValueError, match="cannot carry"):
        PublicationDisplayRuleV2("unknown", "exact", None, ())

    with pytest.raises(ValueError, match="clock accuracy status"):
        PhysicalClockAccuracyV2("claimed")
    with pytest.raises(ValueError, match="cannot claim"):
        PhysicalClockAccuracyV2("unknown", 0, ())
    for invalid_bound in (True, -1, 1.5):
        with pytest.raises(ValueError, match="nonnegative bound"):
            PhysicalClockAccuracyV2("evidenced", invalid_bound, ("x",))
    with pytest.raises(ValueError, match="nonnegative bound"):
        PhysicalClockAccuracyV2("source_asserted", 1, ())

    with pytest.raises(TypeError, match="endpoint flags"):
        NominalPublicationIntervalV2(
            "2026-01-01T00:00:00Z", 1, "2026-01-01T00:01:00Z", False, "explicit_interval", ("x",)
        )
    with pytest.raises(ValueError, match="derivation rule"):
        NominalPublicationIntervalV2(
            "2026-01-01T00:00:00Z", True, "2026-01-01T00:01:00Z", False, "invented", ("x",)
        )
    with pytest.raises(ValueError, match="requires material IDs"):
        NominalPublicationIntervalV2(
            "2026-01-01T00:00:00Z", True, "2026-01-01T00:01:00Z", False, "explicit_interval", ()
        )


def test_source_publication_shape_and_type_invariants():
    zone = PublicationZoneEvidenceV2("unverified")
    display = PublicationDisplayRuleV2("unknown")
    clock = PhysicalClockAccuracyV2("unknown")
    unknown = SourcePublicationV2("", "unknown", None, None, None, None, zone, display, clock, None)
    assert unknown.material_ids() == ()
    with pytest.raises(TypeError, match="raw_text"):
        replace(unknown, raw_text=1)
    with pytest.raises(ValueError, match="empty only"):
        replace(unknown, raw_text="", precision="minute", local_minute="2026-01-01T00:00")
    with pytest.raises(ValueError, match="unsupported source publication precision"):
        replace(unknown, raw_text="x", precision="second")
    for field_name, bad_value, message in (
        ("zone", object(), "zone must"),
        ("display_rule", object(), "display_rule must"),
        ("clock_accuracy", object(), "clock_accuracy must"),
        ("explicit_interval", object(), "explicit_interval must"),
        ("nominal_interval", object(), "nominal_interval must"),
    ):
        with pytest.raises(TypeError, match=message):
            replace(unknown, **{field_name: bad_value})
    with pytest.raises(ValueError, match="exact precision"):
        replace(unknown, raw_text="x", precision="exact_timestamp")
    with pytest.raises(ValueError, match="minute precision"):
        replace(unknown, raw_text="x", precision="minute", exact_at="2026-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="date precision"):
        replace(unknown, raw_text="x", precision="date", exact_at="2026-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="interval precision"):
        replace(unknown, raw_text="x", precision="interval")
    interval = NominalPublicationIntervalV2(
        "2026-01-01T00:00:00Z", True, "2026-01-01T00:01:00Z", False, "explicit_interval", ("x",)
    )
    other = replace(interval, latest="2026-01-01T00:02:00Z")
    with pytest.raises(ValueError, match="must be the nominal interval"):
        SourcePublicationV2(
            "x", "interval", None, None, None, interval, zone, display, clock, other
        )
    with pytest.raises(ValueError, match="unknown precision"):
        replace(unknown, raw_text="", exact_at="2026-01-01T00:00:00Z")


def test_availability_value_object_rejects_inconsistent_nominal_and_legacy_claims():
    nominal = minute_result("2026-09-01T12:00").timing.availability
    for field_name, bad_value, message in (
        ("admission_relation", "equal", "admission relation"),
        ("mode", "claimed", "availability mode"),
        ("basis", "claimed", "availability basis"),
        ("trust_model", "claimed", "trust model"),
        ("usage_scope", "live", "usage scope"),
        ("physical_clock_accuracy", "precise", "physical clock"),
    ):
        with pytest.raises(ValueError, match=message):
            replace(nominal, **{field_name: bad_value})
    with pytest.raises(TypeError, match="DowngradeReasonV2"):
        replace(nominal, downgrade_reasons=(object(),))
    with pytest.raises(ValueError, match="retrospective source trust"):
        replace(nominal, usage_scope="forward_and_retrospective")
    with pytest.raises(ValueError, match="materials and no legacy pointer"):
        replace(nominal, material_ids=())
    with pytest.raises(ValueError, match="risk note"):
        replace(nominal, risk_note=None)

    legacy = AvailabilityDecisionV2(
        "2026-01-01T00:00:00Z",
        "at_or_after",
        "source_declared_nominal",
        "legacy_available_at",
        "trust_source_declared_time",
        "retrospective_only",
        "unknown",
        "legacy timing semantics",
        "1",
        (),
        (),
        "/timing/available_at",
    )
    assert legacy.legacy_timing_pointer == "/timing/available_at"
    with pytest.raises(ValueError, match="requires only"):
        replace(legacy, legacy_timing_pointer=None)
    bad = nominal.to_dict()
    bad["downgrade_reasons"] = {}
    with pytest.raises(TypeError, match="must be an array"):
        AvailabilityDecisionV2.from_dict(bad)


def test_evidence_timing_rejects_cross_object_conflicts_and_bad_json():
    timing = minute_result("2026-09-01T12:00").timing
    with pytest.raises(ValueError, match="schema"):
        replace(timing, schema="puresaber.evidence-timing/3")
    with pytest.raises(TypeError, match="source_materials"):
        replace(timing, source_materials=(object(),))
    with pytest.raises(ValueError, match="unique"):
        replace(timing, source_materials=(material("zone"), material("zone")))
    for field_name, message in (
        ("source_publication", "source_publication must"),
        ("availability", "availability must"),
        ("revision", "revision must"),
    ):
        with pytest.raises(TypeError, match=message):
            replace(timing, **{field_name: object()})
    payload = timing.to_dict()
    payload["source_materials"][0]["acquired_at"] = "2026-10-05T00:00:00Z"
    with pytest.raises(ValueError, match="after the revision"):
        EvidenceTimingV2.from_dict(payload)

    captured = build_evidence_timing_v2(
        effective_at="2026-01-01T00:00:00Z",
        captured_at="2026-01-02T00:00:00Z",
        raw_text="",
        precision="unknown",
        revision_id="u",
    ).timing
    with pytest.raises(ValueError, match="boundary must equal"):
        replace(
            captured,
            availability=replace(captured.availability, boundary_at="2026-01-01T00:00:00Z"),
        )
    with pytest.raises(ValueError, match="clock accuracy conflicts"):
        replace(
            timing,
            availability=replace(timing.availability, physical_clock_accuracy="source_asserted"),
        )
    with pytest.raises(ValueError, match="usage scopes"):
        replace(timing, revision=replace(timing.revision, usage_scope="forward_and_retrospective"))
    rewritten = timing.to_dict()
    rewritten["source_publication"]["nominal_interval"]["latest"] = "2026-09-01T12:02:00Z"
    rewritten["availability"]["boundary_at"] = "2026-09-01T12:02:00Z"
    with pytest.raises(ValueError, match="does not match"):
        EvidenceTimingV2.from_dict(rewritten)
    malformed = timing.to_dict()
    malformed["source_materials"] = {}
    with pytest.raises(TypeError, match="must be an array"):
        EvidenceTimingV2.from_dict(malformed)
    with pytest.raises(TypeError, match="must be str or bytes"):
        EvidenceTimingV2.from_json({})
    with pytest.raises(ValueError, match="invalid evidence timing JSON"):
        EvidenceTimingV2.from_json(b"{")
    assert (
        captured.admission_boundary(
            study_mode="retrospective", trust_model="capture_receipt_only"
        ).boundary_at
        == captured.captured_at
    )
    with pytest.raises(TypeError, match="timing must"):
        EvidenceTimingBuildResultV2(object(), ())


def test_builder_rejects_invalid_shapes_and_records_material_conflicts():
    with pytest.raises(TypeError, match="source_materials"):
        build_evidence_timing_v2(
            effective_at="2026-01-01T00:00:00Z",
            captured_at="2026-01-02T00:00:00Z",
            raw_text="",
            precision="unknown",
            revision_id="u",
            source_materials=(object(),),
        )
    duplicate = material("zone")
    with pytest.raises(ValueError, match="unique"):
        build_evidence_timing_v2(
            effective_at="2026-01-01T00:00:00Z",
            captured_at="2026-01-02T00:00:00Z",
            raw_text="",
            precision="unknown",
            revision_id="u",
            source_materials=(duplicate, duplicate),
        )
    zone_conflict = build_evidence_timing_v2(
        effective_at="2026-01-01T00:00:00Z",
        captured_at="2026-01-02T00:00:00Z",
        raw_text="2026-01-01T00:00",
        precision="minute",
        revision_id="r",
        source_materials=(material("zone"),),
        local_minute="2026-01-01T00:00",
        zone_status="evidenced_iana",
        iana_name="Mars/Olympus",
        zone_material_ids=("zone",),
    )
    assert "MATERIAL_REFERENCE_CONFLICT" in reason_codes(zone_conflict)
    display_conflict = build_evidence_timing_v2(
        effective_at="2026-01-01T00:00:00Z",
        captured_at="2026-01-02T00:00:00Z",
        raw_text="2026-01-01T00:00:00Z",
        precision="exact_timestamp",
        revision_id="r",
        source_materials=(material("display"),),
        exact_at="2026-01-01T00:00:00Z",
        display_rule="invented",
        display_material_ids=("display",),
    )
    assert "MATERIAL_REFERENCE_CONFLICT" in reason_codes(display_conflict)
    clock_conflict = build_evidence_timing_v2(
        effective_at="2026-01-01T00:00:00Z",
        captured_at="2026-01-02T00:00:00Z",
        raw_text="",
        precision="unknown",
        revision_id="r",
        source_materials=(material("zone"),),
        clock_accuracy_status="evidenced",
        max_error_seconds=-1,
        clock_material_ids=("zone",),
    )
    assert "MATERIAL_REFERENCE_CONFLICT" in reason_codes(clock_conflict)


def test_builder_covers_precision_contract_failures_and_exact_success():
    common = {
        "effective_at": "2026-01-01T00:00:00Z",
        "captured_at": "2026-01-02T00:00:00Z",
        "raw_text": "x",
        "revision_id": "r",
    }
    for precision, message in (
        ("minute", "requires local_minute"),
        ("date", "requires local_date"),
        ("interval", "requires explicit_interval"),
        ("exact_timestamp", "requires exact_at"),
        ("second", "unsupported source publication precision"),
    ):
        with pytest.raises(ValueError, match=message):
            build_evidence_timing_v2(precision=precision, **common)
    incompatible = build_evidence_timing_v2(
        precision="minute",
        local_minute="2026-01-01T00:00",
        source_materials=(material("zone"), material("display")),
        zone_status="evidenced_offset",
        utc_offset="+00:00",
        zone_material_ids=("zone",),
        display_rule="exact",
        display_material_ids=("display",),
        **common,
    )
    assert "DISPLAY_RULE_UNVERIFIED" in reason_codes(incompatible)
    dangling_interval = NominalPublicationIntervalV2(
        "2026-01-01T00:00:00Z",
        True,
        "2026-01-01T00:01:00Z",
        False,
        "explicit_interval",
        ("missing",),
    )
    dangling = build_evidence_timing_v2(
        precision="interval", explicit_interval=dangling_interval, **common
    )
    assert "MATERIAL_REFERENCE_INCOMPLETE" in reason_codes(dangling)
    exact = build_evidence_timing_v2(
        precision="exact_timestamp",
        exact_at="2026-01-01T00:00:00Z",
        source_materials=(material("display"),),
        display_rule="exact",
        display_material_ids=("display",),
        study_mode="retrospective",
        trust_model="trust_source_declared_time",
        **common,
    )
    assert exact.timing.availability.basis == "source_declared_instant"
    assert exact.timing.availability.mode == "source_declared_nominal"
