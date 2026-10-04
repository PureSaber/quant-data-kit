from __future__ import annotations

import copy
import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from quant_data_kit.financial import (
    CashDeduction,
    CurrencyReference,
    DividendEntitlement,
    DividendLifecycle,
    DividendLifecycleMigrationReceiptV2,
    DividendLifecycleMigrationV2,
    DividendLifecycleV2,
    DividendPayment,
    DividendPaymentElection,
    DividendProposal,
    EvidenceTiming,
    IssuerFxConversion,
    LegacyTimingBindingV2,
    LegacyV1BindingV2,
    PaymentPolicy,
    PhaseEvidence,
    PublishedAmount,
    RoundingPolicy,
    migrate_dividend_lifecycle_v1_to_v2,
)

FIXTURES = Path(__file__).parent / "fixtures"
EXACT_SHA256 = "9616c5730610bc46257161c4a0137a30ccf13f7f779ea81e51ca4225914d0aa6"
DATE_SHA256 = "4444e9e58e4868f44a07d2caa1d19bbc0d28c24953512eb273511ea6972786f2"


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def v1_timing(event_id: str, effective_at: str) -> EvidenceTiming:
    return EvidenceTiming(
        effective_at=effective_at,
        available_at="2026-07-01T00:00:00Z",
        captured_at="2026-07-01T00:00:00Z",
        source_published_at="2026-07-01T00:00:00Z",
    )


def v1_evidence(event_id: str, effective_at: str) -> PhaseEvidence:
    return PhaseEvidence(
        event_id,
        "migration-fixture",
        f"source:{event_id}",
        v1_timing(event_id, effective_at),
    )


def v1_amount(value: str) -> PublishedAmount:
    return PublishedAmount(value, "1", "share", len(value.partition(".")[2]), False)


def complete_v1(*, date_fixing: bool = False) -> DividendLifecycle:
    usd = CurrencyReference("USD", "USD", "identity")
    hkd = CurrencyReference("HKD", "HKD", "identity")
    policy = PaymentPolicy(
        "policy",
        "account",
        "certified",
        "holder",
        "withholding",
        RoundingPolicy("aggregate_account", 2, "ROUND_HALF_UP", "rounding"),
        v1_evidence("policy", "2026-03-01T00:00:00Z"),
    )
    conversion = IssuerFxConversion(
        evidence=v1_evidence("conversion", "2026-06-19T10:00:00Z"),
        from_currency="USD",
        to_currency="HKD",
        rate_text="7.8",
        rate_convention="quote_per_base",
        fixing_at=None if date_fixing else "2026-06-19T09:00:00Z",
        fixing_date="2026-06-19" if date_fixing else None,
        fixing_timezone="Asia/Hong_Kong" if date_fixing else None,
        published_payment_amount=v1_amount("7.80"),
    )
    return DividendLifecycle(
        dividend_id="fixture:complete",
        instrument_id="fixture",
        proposal=DividendProposal(
            v1_evidence("proposal", "2026-01-01T00:00:00Z"),
            v1_amount("1.00"),
            usd,
            "2026-06-01",
            "2026-06-20",
        ),
        entitlement=DividendEntitlement(
            v1_evidence("entitlement", "2026-02-01T00:00:00Z"),
            v1_amount("1.00"),
            usd,
            "2026-06-02",
            "2026-06-20",
            (hkd,),
            "HKD",
        ),
        election=DividendPaymentElection(
            v1_evidence("election", "2026-03-01T00:00:00Z"),
            "account",
            "policy",
            "HKD",
            "default",
        ),
        conversion=conversion,
        payment_policy=policy,
        payment=DividendPayment(
            v1_evidence("payment", "2026-06-20T00:00:00Z"),
            "account",
            "HKD",
            "policy",
            "100",
            "10",
            (CashDeduction("fee", "2", "fee-source"),),
            "0",
            "88",
        ),
    )


def test_a1_a2_v1_golden_bytes_fingerprints_and_schemas_are_unchanged():
    exact = fixture_bytes("dividend_lifecycle_v1_exact.json")
    dated = fixture_bytes("dividend_lifecycle_v1_date.json")

    assert hashlib.sha256(exact).hexdigest() == EXACT_SHA256
    assert hashlib.sha256(dated).hexdigest() == DATE_SHA256
    assert DividendLifecycle.from_json(exact).fingerprint() == EXACT_SHA256
    assert DividendLifecycle.from_json(dated).fingerprint() == DATE_SHA256
    assert DividendLifecycle.from_json(exact).to_json().encode() == exact
    assert DividendLifecycle.from_json(dated).to_json().encode() == dated


def test_a1_exact_migration_binds_actual_source_bytes_and_two_time_semantics():
    result = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="migration-exact",
        migrated_at="2026-10-04T12:00:00Z",
    )
    timing = result.lifecycle.entitlement.evidence.timing

    assert result.receipt.source_fingerprint == EXACT_SHA256
    assert result.receipt.target_fingerprint == result.lifecycle.fingerprint()
    assert timing.captured_at == "2026-01-01T00:02:00Z"
    assert timing.availability.basis == "legacy_available_at"
    assert timing.availability.boundary_at == "2026-01-01T00:01:00Z"
    assert (
        timing.admission_boundary(
            study_mode="natural_forward", trust_model="capture_receipt_only"
        ).boundary_at
        == "2026-10-04T12:00:00Z"
    )
    assert (
        timing.admission_boundary(
            study_mode="retrospective", trust_model="trust_source_declared_time"
        ).boundary_at
        == "2026-01-01T00:01:00Z"
    )


def test_a2_date_only_migration_stays_captured_only():
    result = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_date.json"),
        migration_id="migration-date",
        migrated_at="2026-10-04T12:00:00Z",
    )
    timing = result.lifecycle.entitlement.evidence.timing

    assert result.receipt.source_fingerprint == DATE_SHA256
    assert timing.source_publication.precision == "date"
    assert timing.availability.mode == "captured_only"
    assert timing.availability.boundary_at == timing.captured_at
    assert {item.code for item in timing.availability.downgrade_reasons} == {
        "PUBLICATION_DATE_SEMANTICS_UNVERIFIED"
    }


def test_complete_v1_migration_covers_every_phase_and_round_trips():
    source = complete_v1()
    result = migrate_dividend_lifecycle_v1_to_v2(
        source.to_json(), migration_id="complete", migrated_at="2026-10-04T12:00:00Z"
    )
    restored = DividendLifecycleMigrationV2.from_dict(result.to_dict())

    assert restored == result
    assert restored.lifecycle.proposal.proposed_amount.amount_text == "1.00"
    assert restored.lifecycle.conversion.fixing_time.kind == "exact_timestamp"
    assert restored.lifecycle.payment.net_cash_text == "88"
    assert len(restored.lifecycle.legacy_binding["timing_bindings"]) == 6


def test_date_fixing_and_policy_without_evidence_migrate_explicitly():
    source = complete_v1(date_fixing=True)
    migrated = migrate_dividend_lifecycle_v1_to_v2(
        source.to_json(), migration_id="date-fixing", migrated_at="2026-10-04T12:00:00Z"
    ).lifecycle
    assert migrated.conversion.fixing_time.kind == "date"
    assert migrated.conversion.fixing_time.timezone_name == "Asia/Hong_Kong"

    entitlement_only = DividendLifecycle(
        dividend_id=source.dividend_id,
        instrument_id=source.instrument_id,
        entitlement=source.entitlement,
        payment_policy=PaymentPolicy("unverified", "account", "unverified"),
    )
    migrated = migrate_dividend_lifecycle_v1_to_v2(
        entitlement_only.to_json(),
        migration_id="unverified",
        migrated_at="2026-10-04T12:00:00Z",
    ).lifecycle
    assert migrated.payment_policy.evidence is None


def test_noncanonical_or_invalid_migration_inputs_fail_closed():
    raw = fixture_bytes("dividend_lifecycle_v1_exact.json")
    with pytest.raises(ValueError, match="exact canonical"):
        migrate_dividend_lifecycle_v1_to_v2(
            raw + b"\n", migration_id="newline", migrated_at="2026-10-04T00:00:00Z"
        )
    with pytest.raises(TypeError, match="str or bytes"):
        migrate_dividend_lifecycle_v1_to_v2(
            {}, migration_id="type", migrated_at="2026-10-04T00:00:00Z"
        )
    with pytest.raises(ValueError, match="unsupported.*rule"):
        migrate_dividend_lifecycle_v1_to_v2(
            raw,
            migration_id="rule",
            migrated_at="2026-10-04T00:00:00Z",
            rule_version="2",
        )


@pytest.mark.parametrize(
    "mutation",
    ["fingerprint", "pointer", "available", "captured", "rule", "payload"],
)
def test_tampered_legacy_binding_is_rejected(mutation):
    lifecycle = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="tamper",
        migrated_at="2026-10-04T00:00:00Z",
    ).lifecycle
    payload = lifecycle.to_dict()
    binding = payload["legacy_binding"]
    if mutation == "fingerprint":
        binding["source_fingerprint"] = "0" * 64
    elif mutation == "pointer":
        binding["timing_bindings"][0]["source_pointer"] = "/missing/timing"
    elif mutation == "available":
        binding["timing_bindings"][0]["source_available_at"] = "2026-01-01T00:00:00Z"
    elif mutation == "captured":
        binding["timing_bindings"][0]["source_captured_at"] = "2026-01-01T00:00:00Z"
    elif mutation == "rule":
        binding["rule_version"] = "2"
    else:
        binding["source_v1_payload"]["instrument_id"] = "changed"
    with pytest.raises((TypeError, ValueError)):
        DividendLifecycleV2.from_dict(payload)


@pytest.mark.parametrize("change", ["instrument", "amount", "event_id"])
def test_migrated_target_economics_remain_bound_to_original_v1(change):
    lifecycle = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="economic-binding",
        migrated_at="2026-10-04T00:00:00Z",
    ).lifecycle
    payload = lifecycle.to_dict()
    if change == "instrument":
        payload["instrument_id"] = "other"
    elif change == "amount":
        payload["entitlement"]["approved_amount"]["amount_text"] = "2.00"
    else:
        payload["entitlement"]["evidence"]["event_id"] = "other-event"
        payload["legacy_binding"]["timing_bindings"][0]["target_event_id"] = "other-event"
    with pytest.raises(ValueError, match="economic facts"):
        DividendLifecycleV2.from_dict(payload)


def test_legacy_binding_is_deeply_immutable_after_validation():
    lifecycle = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="immutable",
        migrated_at="2026-10-04T00:00:00Z",
    ).lifecycle
    before = lifecycle.to_json()
    fingerprint = lifecycle.fingerprint()

    with pytest.raises(TypeError):
        lifecycle.legacy_binding["source_v1_payload"]["instrument_id"] = "changed"
    with pytest.raises(TypeError):
        lifecycle.legacy_binding["timing_bindings"][0]["source_pointer"] = "/other"
    assert lifecycle.to_json() == before
    assert lifecycle.fingerprint() == fingerprint


def test_receipt_must_bind_source_target_rule_and_migration_time():
    result = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="receipt",
        migrated_at="2026-10-04T00:00:00Z",
    )
    for field, value in (
        ("source_fingerprint", "0" * 64),
        ("target_fingerprint", "0" * 64),
        ("migrated_at", "2026-10-05T00:00:00Z"),
    ):
        with pytest.raises(ValueError, match="does not bind"):
            replace(result, receipt=replace(result.receipt, **{field: value}))


def test_binding_and_receipt_value_objects_reject_bad_shapes():
    timing = LegacyTimingBindingV2(
        "/entitlement/evidence/timing",
        "event",
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00Z",
    )
    with pytest.raises(ValueError, match="JSON pointer"):
        replace(timing, source_pointer="relative")
    with pytest.raises(ValueError, match="unique"):
        LegacyV1BindingV2({}, "0" * 64, (timing, timing), "1", "2026-01-01T00:00:00Z")
    with pytest.raises(ValueError, match="source schema"):
        DividendLifecycleMigrationReceiptV2(
            "wrong",
            "0" * 64,
            "puresaber.dividend-lifecycle/2",
            "1" * 64,
            "1",
            "2026-01-01T00:00:00Z",
            "migration",
        )


def test_migration_result_parser_rejects_unknown_fields():
    result = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="shape",
        migrated_at="2026-10-04T00:00:00Z",
    )
    payload = copy.deepcopy(result.to_dict())
    payload["unknown"] = True
    with pytest.raises(ValueError, match="unsupported fields"):
        DividendLifecycleMigrationV2.from_dict(payload)


def test_migration_value_objects_reject_container_scalar_and_receipt_contract_errors():
    timing = LegacyTimingBindingV2(
        "/entitlement/evidence/timing",
        "event",
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00Z",
    )
    with pytest.raises(TypeError, match="must be an object"):
        LegacyTimingBindingV2.from_dict([])
    with pytest.raises(ValueError, match="missing fields"):
        LegacyTimingBindingV2.from_dict({})
    with pytest.raises(TypeError, match="nonempty string"):
        replace(timing, target_event_id="")
    with pytest.raises(TypeError, match="timestamp string"):
        replace(timing, source_available_at=1)
    with pytest.raises(ValueError, match="lowercase SHA"):
        LegacyV1BindingV2({}, "X" * 64, (timing,), "1", "2026-01-01T00:00:00Z")
    with pytest.raises(TypeError, match="timing_bindings"):
        LegacyV1BindingV2({}, "0" * 64, (), "1", "2026-01-01T00:00:00Z")
    binding_payload = {
        "source_v1_payload": {},
        "source_fingerprint": "0" * 64,
        "timing_bindings": "bad",
        "rule_version": "1",
        "migrated_at": "2026-01-01T00:00:00Z",
    }
    with pytest.raises(TypeError, match="must be an array"):
        LegacyV1BindingV2.from_dict(binding_payload)

    result = migrate_dividend_lifecycle_v1_to_v2(
        fixture_bytes("dividend_lifecycle_v1_exact.json"),
        migration_id="receipt-contracts",
        migrated_at="2026-10-04T00:00:00Z",
    )
    receipt = result.receipt
    for field, value, message in (
        ("schema", "wrong", "receipt schema"),
        ("target_schema", "wrong", "target schema"),
        ("source_fingerprint", "X" * 64, "lowercase SHA"),
        ("rule_version", "2", "receipt rule"),
    ):
        with pytest.raises(ValueError, match=message):
            replace(receipt, **{field: value})
    with pytest.raises(TypeError, match="lifecycle must"):
        DividendLifecycleMigrationV2(object(), receipt)
    with pytest.raises(TypeError, match="receipt must"):
        DividendLifecycleMigrationV2(result.lifecycle, object())


def test_migration_preserves_unknown_v1_publication_as_captured_only():
    source = complete_v1()
    unknown_timing = EvidenceTiming(
        effective_at=source.entitlement.evidence.timing.effective_at,
        available_at=source.entitlement.evidence.timing.available_at,
        captured_at=source.entitlement.evidence.timing.captured_at,
    )
    entitlement = replace(
        source.entitlement,
        evidence=replace(source.entitlement.evidence, timing=unknown_timing),
    )
    source = replace(source, entitlement=entitlement)

    migrated = migrate_dividend_lifecycle_v1_to_v2(
        source.to_json(),
        migration_id="unknown-source-time",
        migrated_at="2026-10-04T00:00:00Z",
    ).lifecycle.entitlement.evidence.timing

    assert migrated.source_publication.precision == "unknown"
    assert migrated.availability.mode == "captured_only"
    assert {item.code for item in migrated.availability.downgrade_reasons} == {"PRECISION_UNKNOWN"}
