from __future__ import annotations

import copy
from dataclasses import replace
from decimal import Decimal

import pytest

from quant_data_kit.financial import (
    CashDeduction,
    CurrencyReference,
    DividendEntitlementV2,
    DividendLifecycle,
    DividendLifecycleV2,
    DividendPaymentElectionV2,
    DividendPaymentV2,
    DividendProposalV2,
    IssuerFixingTimeV2,
    IssuerFxConversionV2,
    PaymentPolicyV2,
    PhaseEvidenceV2,
    PublishedAmount,
    RoundingPolicy,
    build_evidence_timing_v2,
)

USD = CurrencyReference("USD", "USD", "identity")
HKD = CurrencyReference("HKD", "HKD", "identity")


def timing(event_id: str, effective_at: str, captured_at: str = "2026-07-01T00:00:00Z"):
    return build_evidence_timing_v2(
        effective_at=effective_at,
        captured_at=captured_at,
        raw_text="",
        precision="unknown",
        revision_id=f"revision:{event_id}",
    ).timing


def evidence(event_id: str, effective_at: str, captured_at: str = "2026-07-01T00:00:00Z"):
    return PhaseEvidenceV2(
        event_id,
        "synthetic-v2-fixture",
        f"evidence:{event_id}",
        timing(event_id, effective_at, captured_at),
    )


def amount(value: str = "1.00") -> PublishedAmount:
    return PublishedAmount(value, "1", "share", len(value.partition(".")[2]), False)


def full_lifecycle() -> DividendLifecycleV2:
    proposal = DividendProposalV2(
        evidence("proposal", "2026-01-01T00:00:00Z"),
        amount("1.00"),
        USD,
        "2026-06-01",
        "2026-06-20",
    )
    entitlement = DividendEntitlementV2(
        evidence("entitlement", "2026-02-01T00:00:00Z"),
        amount("1.00"),
        USD,
        "2026-06-02",
        "2026-06-20",
        (HKD,),
        "HKD",
    )
    election = DividendPaymentElectionV2(
        evidence("election", "2026-03-01T00:00:00Z"),
        "account",
        "policy",
        "HKD",
        "default",
    )
    conversion = IssuerFxConversionV2(
        evidence("conversion", "2026-06-19T10:00:00Z"),
        "USD",
        "HKD",
        "7.8",
        "quote_per_base",
        IssuerFixingTimeV2("exact_timestamp", "2026-06-19T09:00:00Z", None, None, None, None),
        amount("7.80"),
    )
    policy = PaymentPolicyV2(
        "policy",
        "account",
        "certified",
        "holder",
        "withholding-rule",
        RoundingPolicy("aggregate_account", 2, "ROUND_HALF_UP", "rounding-evidence"),
        evidence("policy", "2026-03-01T00:00:00Z"),
    )
    payment = DividendPaymentV2(
        evidence("payment", "2026-06-20T00:00:00Z"),
        "account",
        "HKD",
        "policy",
        "100",
        "10",
        (CashDeduction("fee", "2", "fee-evidence"),),
        "0",
        "88",
    )
    return DividendLifecycleV2(
        dividend_id="00001:2026:annual",
        instrument_id="00001",
        proposal=proposal,
        entitlement=entitlement,
        election=election,
        conversion=conversion,
        payment_policy=policy,
        payment=payment,
    )


def test_complete_lifecycle_v2_round_trip_economic_values_and_fingerprint():
    lifecycle = full_lifecycle()
    restored = DividendLifecycleV2.from_json(lifecycle.to_json())

    assert restored == lifecycle
    assert restored.fingerprint() == lifecycle.fingerprint()
    assert restored.to_dict()["schema"] == "puresaber.dividend-lifecycle/2"
    assert restored.conversion.rate == Decimal("7.8")
    assert restored.entitlement.approved_amount is not lifecycle.entitlement.approved_amount
    assert restored.payment.net_cash_text == "88"


def test_a12_v2_is_not_v1_and_parsers_are_explicitly_versioned():
    lifecycle = full_lifecycle()

    assert not isinstance(lifecycle, DividendLifecycle)
    with pytest.raises(
        ValueError, match="unsupported fields|unsupported dividend lifecycle schema"
    ):
        DividendLifecycle.from_dict(lifecycle.to_dict())
    payload = lifecycle.to_dict()
    payload["schema"] = "puresaber.dividend-lifecycle/1"
    with pytest.raises(ValueError, match="unsupported dividend lifecycle v2 schema"):
        DividendLifecycleV2.from_dict(payload)


def test_a10_unbounded_approximate_fixing_is_not_a_publication_interval():
    lifecycle = full_lifecycle()
    conversion = replace(
        lifecycle.conversion,
        fixing_time=IssuerFixingTimeV2(
            "approximate_unbounded", None, None, "Europe/London", "at or about 11:00", None
        ),
    )
    updated = replace(lifecycle, conversion=conversion)

    assert updated.conversion.fixing_time.raw_text == "at or about 11:00"
    assert updated.conversion.fixing_time.exact_at is None
    assert updated.conversion.evidence.timing.source_publication.nominal_interval is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "kind": "exact_timestamp",
            "exact_at": None,
            "local_date": None,
            "timezone_name": None,
            "raw_text": None,
            "tolerance_seconds": None,
        },
        {
            "kind": "date",
            "exact_at": None,
            "local_date": "2026-01-01",
            "timezone_name": None,
            "raw_text": None,
            "tolerance_seconds": None,
        },
        {
            "kind": "approximate_unbounded",
            "exact_at": None,
            "local_date": None,
            "timezone_name": None,
            "raw_text": "",
            "tolerance_seconds": None,
        },
        {
            "kind": "approximate_unbounded",
            "exact_at": None,
            "local_date": None,
            "timezone_name": None,
            "raw_text": "about",
            "tolerance_seconds": 60,
        },
    ],
)
def test_fixing_time_modes_reject_partial_or_bounded_shapes(kwargs):
    with pytest.raises(ValueError):
        IssuerFixingTimeV2(**kwargs)


def test_date_fixing_round_trip_and_future_date_rejected():
    lifecycle = full_lifecycle()
    fixing = IssuerFixingTimeV2("date", None, "2026-06-19", "Asia/Hong_Kong", None, None)
    conversion = replace(lifecycle.conversion, fixing_time=fixing)
    assert IssuerFixingTimeV2.from_dict(fixing.to_dict()) == fixing
    assert (
        replace(lifecycle, conversion=conversion).conversion.fixing_time.local_date == "2026-06-19"
    )

    future = replace(fixing, local_date="2026-07-02")
    with pytest.raises(ValueError, match="cannot follow"):
        replace(conversion, fixing_time=future)


def test_exact_fixing_after_availability_boundary_rejected():
    lifecycle = full_lifecycle()
    late = IssuerFixingTimeV2("exact_timestamp", "2026-07-02T00:00:00Z", None, None, None, None)
    with pytest.raises(ValueError, match="before its fixing"):
        replace(lifecycle.conversion, fixing_time=late)


def test_exact_fixing_comparison_preserves_nanoseconds():
    lifecycle = full_lifecycle()
    boundary = "2026-07-01T00:00:00.000000900Z"
    conversion = replace(
        lifecycle.conversion,
        evidence=evidence("conversion-nanoseconds", "2026-06-19T10:00:00Z", boundary),
        fixing_time=IssuerFixingTimeV2(
            "exact_timestamp", "2026-07-01T00:00:00.000000899Z", None, None, None, None
        ),
    )
    assert conversion.fixing_time.exact_at.endswith("000000899Z")

    with pytest.raises(ValueError, match="before its fixing"):
        replace(
            conversion,
            fixing_time=IssuerFixingTimeV2(
                "exact_timestamp", "2026-07-01T00:00:00.000000901Z", None, None, None, None
            ),
        )


def test_payment_balance_and_duplicate_deductions_fail_closed():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="gross=net"):
        replace(lifecycle.payment, net_cash_text="87")
    duplicate = CashDeduction("fee", "1", "second")
    with pytest.raises(ValueError, match="unique"):
        replace(
            lifecycle.payment,
            deductions=(*lifecycle.payment.deductions, duplicate),
            net_cash_text="87",
        )
    with pytest.raises(ValueError, match="received"):
        replace(lifecycle.payment, receipt_status="pending")


def test_payment_cannot_be_available_before_receipt():
    lifecycle = full_lifecycle()
    early = evidence("early-payment", "2026-07-02T00:00:00Z", "2026-07-01T00:00:00Z")
    with pytest.raises(ValueError, match="before cash is received"):
        replace(lifecycle.payment, evidence=early)


def test_payment_receipt_comparison_preserves_nanoseconds():
    lifecycle = full_lifecycle()
    boundary = "2026-07-01T00:00:00.000000900Z"
    received = evidence("payment-nanoseconds", "2026-07-01T00:00:00.000000899Z", boundary)
    assert replace(lifecycle.payment, evidence=received).evidence == received

    premature = evidence("payment-nanoseconds", "2026-07-01T00:00:00.000000901Z", boundary)
    with pytest.raises(ValueError, match="before cash is received"):
        replace(lifecycle.payment, evidence=premature)


def test_entitlement_currency_and_status_invariants():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="unique"):
        replace(lifecycle.entitlement, payment_currencies=(HKD, HKD))
    with pytest.raises(ValueError, match="approved option"):
        replace(lifecycle.entitlement, default_payment_currency="USD")
    with pytest.raises(ValueError, match="approved terms"):
        replace(lifecycle.entitlement, approval_status="proposed")
    with pytest.raises(TypeError, match="CurrencyReference"):
        replace(lifecycle.entitlement, payment_currencies=())


def test_election_invariants_and_default_currency_join():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="default or elected"):
        replace(lifecycle.election, selection_kind="unknown")
    with pytest.raises(ValueError, match="partial"):
        replace(lifecycle.election, selection_scope="partial")
    with pytest.raises(ValueError, match="approved currency"):
        replace(lifecycle, election=replace(lifecycle.election, payment_currency="CNY"))
    elected = replace(lifecycle.election, selection_kind="elected")
    assert replace(lifecycle, election=elected).election.selection_kind == "elected"


def test_policy_certification_invariants():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="requires evidence"):
        replace(lifecycle.payment_policy, evidence=None)
    with pytest.raises(ValueError, match="cannot certify"):
        replace(
            lifecycle.payment_policy,
            certification_status="unverified",
            holder_tax_profile_id=None,
            evidence=None,
        )
    unverified = PaymentPolicyV2("p", "a", "unverified")
    assert PaymentPolicyV2.from_dict(unverified.to_dict()) == unverified


def test_lifecycle_phase_ids_and_order_fail_closed():
    lifecycle = full_lifecycle()
    duplicate = replace(
        lifecycle.election,
        evidence=replace(
            lifecycle.election.evidence,
            event_id=lifecycle.entitlement.evidence.event_id,
        ),
    )
    with pytest.raises(ValueError, match="event_id must be unique"):
        replace(lifecycle, election=duplicate)
    late_proposal = replace(
        lifecycle.proposal,
        evidence=evidence("late-proposal", "2026-03-01T00:00:00Z"),
    )
    with pytest.raises(ValueError, match="after entitlement"):
        replace(lifecycle, proposal=late_proposal)


def test_currency_conversion_joins_fail_closed():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="source currency"):
        replace(lifecycle, conversion=replace(lifecycle.conversion, from_currency="CNY"))
    with pytest.raises(ValueError, match="approved currency"):
        replace(lifecycle, conversion=replace(lifecycle.conversion, to_currency="CNY"))
    same_entitlement = replace(
        lifecycle.entitlement,
        declared_currency=HKD,
        payment_currencies=(HKD,),
    )
    with pytest.raises(ValueError, match="same-currency"):
        replace(lifecycle, entitlement=same_entitlement)


def test_payment_requires_joined_election_policy_conversion_and_accounts():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="requires election"):
        replace(lifecycle, election=None)
    with pytest.raises(ValueError, match="policy IDs differ"):
        replace(lifecycle, payment=replace(lifecycle.payment, policy_id="other"))
    with pytest.raises(ValueError, match="policy accounts differ"):
        replace(lifecycle, payment=replace(lifecycle.payment, account_id="other"))
    with pytest.raises(ValueError, match="currency differs"):
        replace(lifecycle, payment=replace(lifecycle.payment, payment_currency="USD"))
    with pytest.raises(ValueError, match="requires issuer conversion"):
        replace(lifecycle, conversion=None)


@pytest.mark.parametrize(
    ("field", "effective", "message"),
    [
        ("payment_policy", "2026-06-21T00:00:00Z", "policy cannot"),
        ("election", "2026-06-21T00:00:00Z", "election cannot"),
        ("conversion", "2026-06-21T00:00:00Z", "cannot precede"),
    ],
)
def test_payment_phase_time_order(field, effective, message):
    lifecycle = full_lifecycle()
    phase = getattr(lifecycle, field)
    updated = replace(phase, evidence=evidence(f"late-{field}", effective))
    with pytest.raises(ValueError, match=message):
        replace(lifecycle, **{field: updated})


def test_phase_parsers_reject_wrong_phase_unknown_fields_and_nonarrays():
    lifecycle = full_lifecycle()
    payload = lifecycle.to_dict()
    payload["entitlement"]["phase"] = "proposal"
    with pytest.raises(ValueError, match="phase must"):
        DividendLifecycleV2.from_dict(payload)

    payload = lifecycle.to_dict()
    payload["entitlement"]["unknown"] = True
    with pytest.raises(ValueError, match="unsupported fields"):
        DividendLifecycleV2.from_dict(payload)

    payload = lifecycle.to_dict()
    payload["entitlement"]["payment_currencies"] = {}
    with pytest.raises(TypeError, match="array"):
        DividendLifecycleV2.from_dict(payload)

    payload = lifecycle.to_dict()
    payload["payment"]["deductions"] = {}
    with pytest.raises(TypeError, match="array"):
        DividendLifecycleV2.from_dict(payload)


def test_phase_value_type_guards_and_conversion_rate_rules():
    lifecycle = full_lifecycle()
    with pytest.raises(TypeError, match="PhaseEvidenceV2"):
        replace(lifecycle.entitlement, evidence=object())
    with pytest.raises(ValueError, match="different currencies"):
        replace(lifecycle.conversion, from_currency="HKD")
    with pytest.raises(ValueError, match="quote_per_base"):
        replace(lifecycle.conversion, rate_convention="base_per_quote")
    with pytest.raises(ValueError, match="sign or finiteness"):
        replace(lifecycle.conversion, rate_text="0")


def test_plain_v2_cannot_carry_legacy_basis_or_mutable_legacy_binding():
    lifecycle = full_lifecycle()
    payload = lifecycle.to_dict()
    timing_payload = payload["entitlement"]["evidence"]["timing"]
    timing_payload["availability"].update(
        mode="source_declared_nominal",
        basis="legacy_available_at",
        boundary_at=timing_payload["captured_at"],
        trust_model="trust_source_declared_time",
        usage_scope="retrospective_only",
        risk_note="legacy",
        downgrade_reasons=[],
        legacy_timing_pointer="/entitlement/evidence/timing",
    )
    timing_payload["revision"]["usage_scope"] = "retrospective_only"
    with pytest.raises(ValueError, match="binding"):
        DividendLifecycleV2.from_dict(payload)


def test_json_parser_type_and_syntax_fail_closed():
    with pytest.raises(TypeError, match="str or bytes"):
        DividendLifecycleV2.from_json({})
    with pytest.raises(ValueError, match="invalid"):
        DividendLifecycleV2.from_json("{")
    payload = full_lifecycle().to_dict()
    payload["legacy_binding"] = []
    with pytest.raises(TypeError, match="object or null"):
        DividendLifecycleV2.from_dict(payload)


def test_serialized_payload_is_a_deep_copy():
    lifecycle = full_lifecycle()
    payload = lifecycle.to_dict()
    payload["entitlement"]["approved_amount"]["amount_text"] = "99"
    assert lifecycle.entitlement.approved_amount.amount_text == "1.00"
    restored = DividendLifecycleV2.from_dict(copy.deepcopy(lifecycle.to_dict()))
    assert restored == lifecycle


def test_lifecycle_leaf_and_phase_type_contracts_fail_closed():
    lifecycle = full_lifecycle()
    with pytest.raises(TypeError, match="must be an object"):
        DividendLifecycleV2.from_dict([])
    with pytest.raises(ValueError, match="missing fields"):
        DividendLifecycleV2.from_dict({})
    with pytest.raises(TypeError, match="nonempty string"):
        replace(lifecycle.entitlement.evidence, event_id="")
    with pytest.raises(TypeError, match="timing must"):
        replace(lifecycle.entitlement.evidence, timing=object())
    with pytest.raises(TypeError, match="calendar-date string"):
        replace(lifecycle.entitlement, record_date=1)
    with pytest.raises(ValueError, match="uppercase calculation currency"):
        replace(lifecycle.election, payment_currency="usd")
    with pytest.raises(TypeError, match="exact decimal text"):
        replace(lifecycle.payment, gross_cash_text=100)
    with pytest.raises(ValueError, match="finite decimal text"):
        replace(lifecycle.payment, gross_cash_text="01")

    for field, value, message in (
        ("evidence", object(), "proposal evidence"),
        ("proposed_amount", object(), "proposed_amount"),
        ("declared_currency", object(), "declared_currency"),
    ):
        with pytest.raises(TypeError, match=message):
            replace(lifecycle.proposal, **{field: value})
    for field, value, message in (
        ("approved_amount", object(), "approved_amount"),
        ("declared_currency", object(), "declared_currency"),
    ):
        with pytest.raises(TypeError, match=message):
            replace(lifecycle.entitlement, **{field: value})
    with pytest.raises(TypeError, match="payment election evidence"):
        replace(lifecycle.election, evidence=object())


def test_fixing_conversion_policy_payment_and_lifecycle_type_guards():
    lifecycle = full_lifecycle()
    with pytest.raises(ValueError, match="fixing time kind"):
        IssuerFixingTimeV2("bounded", None, None, None, "about", None)
    with pytest.raises(ValueError, match="valid IANA"):
        IssuerFixingTimeV2("date", None, "2026-06-19", "Mars/Olympus", None, None)
    for field, value, message in (
        ("evidence", object(), "conversion evidence"),
        ("fixing_time", object(), "fixing_time"),
        ("published_payment_amount", object(), "published_payment_amount"),
    ):
        with pytest.raises(TypeError, match=message):
            replace(lifecycle.conversion, **{field: value})
    with pytest.raises(ValueError, match="certification_status"):
        replace(lifecycle.payment_policy, certification_status="claimed")
    with pytest.raises(TypeError, match="policy evidence"):
        replace(lifecycle.payment_policy, evidence=object())
    with pytest.raises(TypeError, match="payment evidence"):
        replace(lifecycle.payment, evidence=object())
    with pytest.raises(TypeError, match="CashDeduction"):
        replace(lifecycle.payment, deductions=(object(),))
    with pytest.raises(TypeError, match="entitlement must"):
        replace(lifecycle, entitlement=object())
    with pytest.raises(TypeError, match="invalid v2 type"):
        replace(lifecycle, proposal=object())


def test_remaining_cross_phase_join_and_order_contracts():
    lifecycle = full_lifecycle()
    entitlement = replace(
        lifecycle.entitlement,
        payment_currencies=(HKD, USD),
        default_payment_currency="HKD",
    )
    default_usd = replace(lifecycle.election, payment_currency="USD")
    with pytest.raises(ValueError, match="issuer default"):
        replace(lifecycle, entitlement=entitlement, election=default_usd, payment=None)
    with pytest.raises(ValueError, match="election and policy IDs"):
        replace(
            lifecycle,
            election=replace(lifecycle.election, account_policy_id="other"),
            payment=None,
        )
    with pytest.raises(ValueError, match="election and policy accounts"):
        replace(
            lifecycle,
            election=replace(lifecycle.election, account_id="other"),
            payment=None,
        )
    cny = CurrencyReference("CNY", "CNY", "identity")
    entitlement = replace(lifecycle.entitlement, payment_currencies=(HKD, cny))
    election = replace(lifecycle.election, payment_currency="CNY", selection_kind="elected")
    with pytest.raises(ValueError, match="does not join"):
        replace(lifecycle, entitlement=entitlement, election=election, payment=None)
    uncertified = PaymentPolicyV2("policy", "account", "unverified", holder_tax_profile_id="holder")
    with pytest.raises(ValueError, match="certified policy"):
        replace(lifecycle, payment_policy=uncertified)
    late_entitlement = replace(
        lifecycle.entitlement,
        evidence=evidence("late-entitlement", "2026-06-21T00:00:00Z"),
    )
    with pytest.raises(ValueError, match="before entitlement"):
        replace(lifecycle, entitlement=late_entitlement)
