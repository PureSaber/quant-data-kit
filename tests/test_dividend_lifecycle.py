import hashlib
import json
from dataclasses import replace
from decimal import Decimal, localcontext
from pathlib import Path

import pytest

from quant_data_kit.financial import (
    DIVIDEND_LIFECYCLE_SCHEMA_ID,
    PIT_FX_SCHEMA_ID,
    CurrencyReference,
    DividendEntitlement,
    DividendLifecycle,
    DividendPayment,
    DividendPaymentElection,
    EvidenceTiming,
    IssuerFxConversion,
    PaymentPolicy,
    PhaseEvidence,
    PitFxRate,
    PublishedAmount,
    RoundingPolicy,
    fx_rate_asof,
    select_pit_fx,
)
from quant_data_kit.schemas_v2 import SCHEMA_VERSION_V2

USD = CurrencyReference("USD", "USD", "identity")
HKD = CurrencyReference("HKD", "HKD", "identity")
CNY = CurrencyReference("RMB", "CNY", "ISO-4217:RMB-to-CNY")

OFFICIAL_CASE_FACTS = {
    "hsbc": {
        "amount": "0.10",
        "declaration_date": "2025-07-30",
        "ex_date": "2025-08-14",
        "record_date": "2025-08-15",
        "payment_date": "2025-09-26",
        "fixing_at": "2025-09-15T11:00:00+01:00",
        "fx_publication_date": "2025-09-15",
        "rate": "7.776781",
        "hkd_amount": "0.777678",
    },
    "tencent": {
        "amount": "5.3",
        "declaration_date": "2026-03-18",
        "ex_date": "2026-05-15",
        "record_date": "2026-05-20",
        "payment_date": "2026-06-01",
    },
    "ccb": {
        "amount": "1.858",
        "source_units": "10",
        "schedule_publication_date": "2025-11-07",
        "ex_date": "2025-12-03",
        "record_date": "2025-12-10",
        "payment_date": "2026-01-26",
        "fx_publication_date": "2025-12-12",
        "rate": "1.1014865663",
        "hkd_amount": "2.04656204",
    },
    "china_mobile": {
        "amount": "2.52",
        "declaration_date": "2026-03-26",
        "ex_date": "2026-06-05",
        "record_date": "2026-06-11",
        "payment_date": "2026-06-24",
    },
    "alibaba": {
        "amount": "0.13125",
        "declaration_date": "2026-05-13",
        "ex_date": "2026-06-10",
        "record_date": "2026-06-11",
        "payment_date": "2026-07-06",
    },
}

SYNTHETIC_TEST_INPUTS = {
    "hsbc": {
        "ex_effective_at": "2025-08-14T01:30:00Z",
        "entitlement_capture_at": "2025-07-30T16:00:00Z",
        "conversion_effective_at": "2025-09-15T14:00:00Z",
        "conversion_capture_at": "2025-09-15T14:00:00Z",
    },
    "tencent": {
        "ex_effective_at": "2026-05-15T01:30:00Z",
        "entitlement_capture_at": "2026-03-18T16:00:00Z",
    },
    "ccb": {
        "ex_effective_at": "2025-12-03T01:30:00Z",
        "entitlement_capture_at": "2025-11-07T16:00:00Z",
        "conversion_effective_at": "2025-12-12T16:00:00Z",
        "conversion_capture_at": "2025-12-12T16:00:00Z",
        "fixing_date": "2025-12-12",
    },
    "china_mobile": {
        "ex_effective_at": "2026-06-05T01:30:00Z",
        "entitlement_capture_at": "2026-03-26T16:00:00Z",
    },
    "alibaba": {
        "ex_effective_at": "2026-06-10T01:30:00Z",
        "entitlement_capture_at": "2026-05-13T16:00:00Z",
    },
}


def amount(value, units="1", *, approximate=False):
    return PublishedAmount(
        amount_text=value,
        source_unit_text=units,
        source_unit_name="share",
        published_decimal_places=len(value.partition(".")[2]),
        approximate=approximate,
    )


def date_only_timing(effective_at, publication_date, captured_at):
    """Exact test capture clocks are synthetic; the source fact remains date-only."""

    return EvidenceTiming(
        effective_at=effective_at,
        available_at=captured_at,
        captured_at=captured_at,
        source_publication_date=publication_date,
    )


def evidence(event_id, timing, *, source="official-fact-with-synthetic-test-clock"):
    return PhaseEvidence(
        event_id=event_id,
        source=source,
        evidence_id=f"fixture:{event_id}",
        timing=timing,
    )


def entitlement(
    symbol,
    declared_amount,
    declared_currency,
    payment_currencies,
    default_currency,
    *,
    ex_at,
    record_date,
    payment_date,
    publication_date,
    captured_at,
):
    """Combine official terms with an explicitly synthetic approval event and clock."""

    return DividendEntitlement(
        evidence=evidence(
            f"{symbol}:entitlement",
            date_only_timing(ex_at, publication_date, captured_at),
            source="official-terms-with-synthetic-approved-event-and-clock",
        ),
        approved_amount=declared_amount,
        declared_currency=declared_currency,
        record_date=record_date,
        scheduled_payment_date=payment_date,
        payment_currencies=payment_currencies,
        default_payment_currency=default_currency,
    )


def election(
    symbol,
    currency,
    *,
    at,
    policy_id="unverified-account-policy",
    account_id="test-account",
):
    return DividendPaymentElection(
        evidence=evidence(
            f"{symbol}:election",
            EvidenceTiming(
                effective_at=at,
                available_at=at,
                captured_at=at,
                source_published_at=at,
            ),
            source="synthetic-account-policy",
        ),
        account_id=account_id,
        account_policy_id=policy_id,
        payment_currency=currency,
        selection_kind="default",
    )


def unverified_policy(policy_id="unverified-account-policy", account_id="test-account"):
    return PaymentPolicy(
        policy_id=policy_id,
        account_id=account_id,
        certification_status="unverified",
    )


def hsbc_lifecycle():
    fact = OFFICIAL_CASE_FACTS["hsbc"]
    synthetic = SYNTHETIC_TEST_INPUTS["hsbc"]
    terms = entitlement(
        "00005",
        amount(fact["amount"]),
        USD,
        (USD, HKD),
        "HKD",
        ex_at=synthetic["ex_effective_at"],
        record_date=fact["record_date"],
        payment_date=fact["payment_date"],
        publication_date=fact["declaration_date"],
        captured_at=synthetic["entitlement_capture_at"],
    )
    conversion_timing = date_only_timing(
        synthetic["conversion_effective_at"],
        fact["fx_publication_date"],
        synthetic["conversion_capture_at"],
    )
    conversion = IssuerFxConversion(
        evidence=evidence("00005:conversion", conversion_timing),
        from_currency="USD",
        to_currency="HKD",
        rate_text=fact["rate"],
        rate_convention="quote_per_base",
        fixing_at=fact["fixing_at"],
        fixing_date=None,
        published_payment_amount=amount(fact["hkd_amount"], approximate=True),
    )
    return DividendLifecycle(
        dividend_id="00005:2025:second-interim",
        instrument_id="00005",
        entitlement=terms,
        election=election("00005", "HKD", at=synthetic["entitlement_capture_at"]),
        conversion=conversion,
        payment_policy=unverified_policy(),
    )


def test_hsbc_round_trip_preserves_currency_timing_precision_and_fingerprint():
    lifecycle = hsbc_lifecycle()
    serialized = lifecycle.to_json()
    restored = DividendLifecycle.from_json(serialized)

    assert json.loads(serialized)["schema"] == DIVIDEND_LIFECYCLE_SCHEMA_ID
    assert restored == lifecycle
    assert restored.fingerprint() == lifecycle.fingerprint()
    assert restored.entitlement.approved_amount.amount_text == "0.10"
    assert restored.conversion.published_payment_amount.amount_text == "0.777678"
    assert restored.conversion.published_payment_amount.approximate is True
    assert restored.conversion.fixing_at == "2025-09-15T10:00:00Z"
    assert restored.conversion.evidence.timing.available_at == "2025-09-15T14:00:00Z"


def test_ccb_preserves_official_terms_and_explicit_synthetic_fixing_date():
    fact = OFFICIAL_CASE_FACTS["ccb"]
    synthetic = SYNTHETIC_TEST_INPUTS["ccb"]
    terms = entitlement(
        "00939",
        amount(fact["amount"], fact["source_units"]),
        CNY,
        (HKD,),
        "HKD",
        ex_at=synthetic["ex_effective_at"],
        record_date=fact["record_date"],
        payment_date=fact["payment_date"],
        publication_date=fact["schedule_publication_date"],
        captured_at=synthetic["entitlement_capture_at"],
    )
    conversion = IssuerFxConversion(
        evidence=evidence(
            "00939:conversion",
            date_only_timing(
                synthetic["conversion_effective_at"],
                fact["fx_publication_date"],
                synthetic["conversion_capture_at"],
            ),
            source="official-fx-values-with-synthetic-fixing-date-and-clock",
        ),
        from_currency="CNY",
        to_currency="HKD",
        rate_text=fact["rate"],
        rate_convention="quote_per_base",
        fixing_at=None,
        fixing_date=synthetic["fixing_date"],
        fixing_timezone="Asia/Hong_Kong",
        published_payment_amount=amount(fact["hkd_amount"], fact["source_units"]),
    )
    lifecycle = DividendLifecycle(
        dividend_id="00939:2025:interim",
        instrument_id="00939",
        entitlement=terms,
        election=election("00939", "HKD", at=synthetic["entitlement_capture_at"]),
        conversion=conversion,
        payment_policy=unverified_policy(),
    )

    assert lifecycle.entitlement.declared_currency.source_label == "RMB"
    assert lifecycle.entitlement.declared_currency.calculation_currency == "CNY"
    assert lifecycle.entitlement.approved_amount.per_source_unit == Decimal("0.1858")
    assert lifecycle.conversion.published_payment_amount.per_source_unit == Decimal("0.204656204")
    assert lifecycle.conversion.fixing_at is None
    assert lifecycle.conversion.fixing_date == "2025-12-12"
    assert DividendLifecycle.from_json(lifecycle.to_json()).conversion.fixing_timezone == (
        "Asia/Hong_Kong"
    )


def test_alibaba_usd_direct_payment_needs_no_issuer_conversion():
    fact = OFFICIAL_CASE_FACTS["alibaba"]
    synthetic = SYNTHETIC_TEST_INPUTS["alibaba"]
    terms = entitlement(
        "09988",
        amount(fact["amount"]),
        USD,
        (USD,),
        "USD",
        ex_at=synthetic["ex_effective_at"],
        record_date=fact["record_date"],
        payment_date=fact["payment_date"],
        publication_date=fact["declaration_date"],
        captured_at=synthetic["entitlement_capture_at"],
    )
    lifecycle = DividendLifecycle(
        dividend_id="09988:2026:annual",
        instrument_id="09988",
        entitlement=terms,
        election=election("09988", "USD", at=synthetic["entitlement_capture_at"]),
        payment_policy=unverified_policy(),
    )

    assert lifecycle.conversion is None
    assert lifecycle.election.payment_currency == "USD"


def test_tencent_same_currency_path_does_not_invent_conversion():
    fact = OFFICIAL_CASE_FACTS["tencent"]
    synthetic = SYNTHETIC_TEST_INPUTS["tencent"]
    # Amount and calendar dates are official; approval evidence and intraday clocks are synthetic.
    terms = entitlement(
        "00700",
        amount(fact["amount"]),
        HKD,
        (HKD,),
        "HKD",
        ex_at=synthetic["ex_effective_at"],
        record_date=fact["record_date"],
        payment_date=fact["payment_date"],
        publication_date=fact["declaration_date"],
        captured_at=synthetic["entitlement_capture_at"],
    )
    lifecycle = DividendLifecycle(
        dividend_id="00700:2025:final",
        instrument_id="00700",
        entitlement=terms,
        election=election("00700", "HKD", at=synthetic["entitlement_capture_at"]),
        payment_policy=unverified_policy(),
    )

    assert lifecycle.entitlement.approved_amount.amount_text == "5.3"
    assert lifecycle.entitlement.record_date == "2026-05-20"
    assert lifecycle.entitlement.declared_currency.calculation_currency == "HKD"
    assert lifecycle.conversion is None
    with pytest.raises(ValueError, match="must not invent"):
        replace(lifecycle, conversion=hsbc_lifecycle().conversion)


def certified_policy(
    policy_id,
    holder,
    rule,
    account_id="test-account",
    policy_timing=None,
):
    if policy_timing is None:
        policy_timing = EvidenceTiming(
            effective_at="2026-03-26T16:00:00Z",
            available_at="2026-03-26T16:00:00Z",
            captured_at="2026-03-26T16:00:00Z",
            source_published_at="2026-03-26T16:00:00Z",
        )
    return PaymentPolicy(
        policy_id=policy_id,
        account_id=account_id,
        certification_status="certified",
        holder_tax_profile_id=holder,
        withholding_rule_id=rule,
        rounding=RoundingPolicy(
            scope="aggregate_account",
            decimal_places=2,
            mode="ROUND_HALF_UP",
            evidence_id=f"synthetic:{policy_id}:rounding",
        ),
        evidence=evidence(
            f"payment-policy:{policy_id}",
            policy_timing,
            source="synthetic-account-policy",
        ),
    )


def payment(symbol, policy_id, gross, withholding, net, account_id="test-account"):
    return DividendPayment(
        evidence=evidence(
            f"{symbol}:payment:{policy_id}",
            EvidenceTiming(
                effective_at="2026-06-24T08:00:00Z",
                available_at="2026-06-24T08:00:00Z",
                captured_at="2026-06-24T08:00:00Z",
                source_published_at="2026-06-24T08:00:00Z",
            ),
            source="synthetic-account-receipt",
        ),
        account_id=account_id,
        payment_currency="HKD",
        policy_id=policy_id,
        gross_cash_text=gross,
        withholding_cash_text=withholding,
        deductions=(),
        rounding_adjustment_text="0",
        net_cash_text=net,
    )


def test_china_mobile_account_tax_profiles_are_explicit_and_not_defaulted():
    fact = OFFICIAL_CASE_FACTS["china_mobile"]
    synthetic = SYNTHETIC_TEST_INPUTS["china_mobile"]
    terms = entitlement(
        "00941",
        amount(fact["amount"]),
        HKD,
        (HKD,),
        "HKD",
        ex_at=synthetic["ex_effective_at"],
        record_date=fact["record_date"],
        payment_date=fact["payment_date"],
        publication_date=fact["declaration_date"],
        captured_at=synthetic["entitlement_capture_at"],
    )
    zero_policy = certified_policy("holder-zero", "synthetic-zero", "synthetic-explicit-0")
    taxed_policy = certified_policy("holder-taxed", "synthetic-taxed", "synthetic-explicit-10")
    base = {
        "dividend_id": "00941:2025:final",
        "instrument_id": "00941",
        "entitlement": terms,
    }
    untaxed = DividendLifecycle(
        **base,
        election=election(
            "00941",
            "HKD",
            at=synthetic["entitlement_capture_at"],
            policy_id=zero_policy.policy_id,
        ),
        payment_policy=zero_policy,
        payment=payment("00941", zero_policy.policy_id, "1260", "0", "1260"),
    )
    taxed = DividendLifecycle(
        **base,
        election=election(
            "00941",
            "HKD",
            at=synthetic["entitlement_capture_at"],
            policy_id=taxed_policy.policy_id,
        ),
        payment_policy=taxed_policy,
        payment=payment("00941", taxed_policy.policy_id, "1260", "126", "1134"),
    )

    assert untaxed.payment.gross_cash_text == taxed.payment.gross_cash_text == "1260"
    assert untaxed.entitlement.record_date == "2026-06-11"
    assert untaxed.payment.net_cash_text == "1260"
    assert taxed.payment.net_cash_text == "1134"
    with pytest.raises(ValueError, match="certified account payment policy"):
        replace(untaxed, payment_policy=unverified_policy(zero_policy.policy_id))


@pytest.mark.parametrize("value", [True, 1.25, "NaN", "Infinity", "1e-3", "+1"])
def test_money_fields_reject_boolean_binary_float_nonfinite_and_nonplain_text(value):
    with pytest.raises((TypeError, ValueError)):
        PublishedAmount(value, "1", "share", 0, False)


def test_date_only_publication_cannot_invent_intraday_availability():
    with pytest.raises(ValueError, match="must equal captured_at"):
        EvidenceTiming(
            effective_at="2025-08-14T01:30:00Z",
            available_at="2025-07-30T00:00:00Z",
            captured_at="2025-07-30T16:00:00Z",
            source_publication_date="2025-07-30",
        )
    with pytest.raises(ValueError, match="cannot follow"):
        EvidenceTiming(
            effective_at="2025-08-14T01:30:00Z",
            available_at="2025-07-30T16:00:00Z",
            captured_at="2025-07-30T16:00:00Z",
            source_publication_date="2025-07-31",
        )


def test_payment_balance_and_unverified_cash_fail_closed():
    with pytest.raises(ValueError, match="gross=net"):
        payment("00941", "holder-zero", "1260", "0", "1259")
    lifecycle = hsbc_lifecycle()
    paid = payment("00005", "unverified-account-policy", "311.0712", "0", "311.0712")
    with pytest.raises(ValueError, match="certified account payment policy"):
        replace(lifecycle, payment=paid)
    with pytest.raises(ValueError, match="requires evidence"):
        replace(
            certified_policy("must-have-evidence", "holder", "rule"),
            evidence=None,
        )
    assert unverified_policy().to_dict()["evidence"] is None


def test_exact_decimal_invariants_ignore_process_context():
    with localcontext() as context:
        context.prec = 2
        with pytest.raises(ValueError, match="gross=net"):
            payment("00941", "holder-zero", "100", "0.01", "100")
        assert amount("1.858", "10").per_source_unit == Decimal("0.1858")
        with pytest.raises(ValueError, match="not an exactly representable finite decimal"):
            _ = amount("1", "3").per_source_unit


def test_date_only_issuer_fixing_cannot_be_future_known_fact():
    with pytest.raises(ValueError, match="cannot follow"):
        IssuerFxConversion(
            evidence=evidence(
                "future-fixing",
                EvidenceTiming(
                    effective_at="2026-01-01T00:00:00Z",
                    available_at="2026-01-01T00:00:00Z",
                    captured_at="2026-01-01T00:00:00Z",
                    source_published_at="2026-01-01T00:00:00Z",
                ),
            ),
            from_currency="USD",
            to_currency="HKD",
            rate_text="7.8",
            rate_convention="quote_per_base",
            fixing_at=None,
            fixing_date="2027-01-01",
            fixing_timezone="Asia/Hong_Kong",
            published_payment_amount=amount("0.78"),
        )


def test_date_only_issuer_fixing_uses_declared_local_calendar_day():
    conversion = IssuerFxConversion(
        evidence=evidence(
            "local-date-fixing",
            EvidenceTiming(
                effective_at="2025-12-11T16:30:00Z",
                available_at="2025-12-11T16:30:00Z",
                captured_at="2025-12-11T16:30:00Z",
                source_published_at="2025-12-11T16:30:00Z",
            ),
        ),
        from_currency="CNY",
        to_currency="HKD",
        rate_text="1.1014865663",
        rate_convention="quote_per_base",
        fixing_at=None,
        fixing_date="2025-12-12",
        fixing_timezone="Asia/Hong_Kong",
        published_payment_amount=amount("2.04656204", "10"),
    )

    assert conversion.fixing_at is None
    assert conversion.fixing_date == "2025-12-12"
    assert conversion.fixing_timezone == "Asia/Hong_Kong"
    with pytest.raises(ValueError, match="valid IANA"):
        replace(conversion, fixing_timezone="HongKong/Unknown")


def test_entitlement_only_snapshot_round_trips_without_future_account_facts():
    known = hsbc_lifecycle()
    snapshot = DividendLifecycle(
        dividend_id=known.dividend_id,
        instrument_id=known.instrument_id,
        entitlement=known.entitlement,
    )

    restored = DividendLifecycle.from_json(snapshot.to_json())

    assert restored == snapshot
    assert restored.dividend_id == "00005:2025:second-interim"
    assert restored.instrument_id == "00005"
    assert restored.election is None
    assert restored.payment_policy is None
    assert restored.to_dict()["election"] is None
    assert restored.to_dict()["payment_policy"] is None


def test_late_account_evidence_preserves_effective_and_available_times():
    terms = entitlement(
        "late-fact",
        amount("1.00"),
        HKD,
        (HKD,),
        "HKD",
        ex_at="2026-06-05T01:30:00Z",
        record_date="2026-06-08",
        payment_date="2026-06-24",
        publication_date="2026-03-26",
        captured_at="2026-03-26T16:00:00Z",
    )
    late_election = replace(
        election(
            "late-fact",
            "HKD",
            at="2026-06-20T08:00:00Z",
            policy_id="late-policy",
        ),
        evidence=evidence(
            "late-fact:election",
            EvidenceTiming(
                effective_at="2026-06-20T08:00:00Z",
                available_at="2026-07-01T08:00:00Z",
                captured_at="2026-07-01T08:00:00Z",
                source_published_at="2026-07-01T08:00:00Z",
            ),
            source="synthetic-late-account-record",
        ),
    )
    late_payment = replace(
        payment("late-fact", "late-policy", "100", "0", "100"),
        evidence=evidence(
            "late-fact:payment:late-policy",
            EvidenceTiming(
                effective_at="2026-06-24T08:00:00Z",
                available_at="2026-07-02T08:00:00Z",
                captured_at="2026-07-02T08:00:00Z",
                source_published_at="2026-07-02T08:00:00Z",
            ),
            source="synthetic-late-account-record",
        ),
    )
    late_policy = certified_policy(
        "late-policy",
        "synthetic-holder",
        "synthetic-withholding-rule",
        policy_timing=EvidenceTiming(
            effective_at="2026-06-20T08:00:00Z",
            available_at="2026-07-01T09:00:00Z",
            captured_at="2026-07-01T09:00:00Z",
            source_published_at="2026-07-01T09:00:00Z",
        ),
    )
    lifecycle = DividendLifecycle(
        dividend_id="late-fact:2026",
        instrument_id="late-fact",
        entitlement=terms,
        election=late_election,
        payment_policy=late_policy,
        payment=late_payment,
    )

    restored = DividendLifecycle.from_json(lifecycle.to_json())
    assert restored.election.evidence.timing.effective_at == "2026-06-20T08:00:00Z"
    assert restored.election.evidence.timing.available_at == "2026-07-01T08:00:00Z"
    assert restored.payment_policy.evidence.timing.effective_at == "2026-06-20T08:00:00Z"
    assert restored.payment_policy.evidence.timing.available_at == "2026-07-01T09:00:00Z"
    assert restored.payment.evidence.timing.effective_at == "2026-06-24T08:00:00Z"
    assert restored.payment.evidence.timing.available_at == "2026-07-02T08:00:00Z"
    future_policy = replace(
        late_policy,
        evidence=replace(
            late_policy.evidence,
            timing=EvidenceTiming(
                effective_at="2026-06-25T08:00:00Z",
                available_at="2026-07-01T09:00:00Z",
                captured_at="2026-07-01T09:00:00Z",
                source_published_at="2026-07-01T09:00:00Z",
            ),
        ),
    )
    with pytest.raises(ValueError, match="payment policy cannot become effective after payment"):
        replace(lifecycle, payment_policy=future_policy)


def test_phase_links_conflicts_and_unsupported_partial_payload_fail_closed():
    lifecycle = hsbc_lifecycle()
    with pytest.raises(ValueError, match="source currency must match entitlement"):
        replace(lifecycle, conversion=replace(lifecycle.conversion, from_currency="CNY"))
    payload = lifecycle.to_dict()
    payload["payments"] = [payload["payment"]]
    with pytest.raises(ValueError, match="unsupported fields"):
        DividendLifecycle.from_dict(payload)
    duplicate = replace(
        lifecycle.election,
        evidence=replace(
            lifecycle.election.evidence,
            event_id=lifecycle.entitlement.evidence.event_id,
        ),
    )
    with pytest.raises(ValueError, match="event_id must be unique"):
        replace(lifecycle, election=duplicate)
    with pytest.raises(ValueError, match="partial payment election is unsupported"):
        replace(
            lifecycle,
            election=replace(lifecycle.election, selection_scope="partial"),
        )
    with pytest.raises(ValueError, match="same account ID"):
        replace(lifecycle, payment_policy=unverified_policy(account_id="other-account"))


def fx(event_id, rate, observed, available, captured=None):
    return PitFxRate(
        event_id=event_id,
        base_currency="USD",
        quote_currency="HKD",
        rate_text=rate,
        rate_convention="quote_per_base",
        observed_at=observed,
        available_at=available,
        captured_at=captured or available,
        source="synthetic-fx",
        evidence_id=f"fixture:{event_id}",
    )


def test_pit_fx_round_trip_asof_identity_future_and_missing_boundaries():
    old = fx("old", "7.78", "2025-08-13T08:00:00Z", "2025-08-13T08:00:01Z")
    future_observation = fx(
        "future-observation", "7.79", "2025-08-15T08:00:00Z", "2025-08-15T08:00:01Z"
    )
    late = fx(
        "late",
        "7.77",
        "2025-08-14T08:00:00Z",
        "2025-09-15T08:00:00Z",
        "2025-09-15T08:00:00Z",
    )
    selected = select_pit_fx(
        [old, future_observation, late],
        base_currency="USD",
        quote_currency="HKD",
        cutoff="2025-08-14T09:00:00Z",
    )
    assert selected.rate == Decimal("7.78") and selected.event_id == "old"
    assert PitFxRate.from_json(old.to_json()) == old
    assert json.loads(old.to_json())["schema"] == PIT_FX_SCHEMA_ID
    assert (
        fx_rate_asof([], base_currency="HKD", quote_currency="HKD", cutoff="2025-08-14T09:00:00Z")
        == 1
    )
    with pytest.raises(ValueError, match="missing PIT FX"):
        fx_rate_asof([], base_currency="USD", quote_currency="HKD", cutoff="2025-08-14T09:00:00Z")


def test_pit_fx_duplicate_conflict_and_ambiguous_latest_fail_closed():
    first = fx("same", "7.78", "2025-08-14T08:00:00Z", "2025-08-14T08:00:01Z")
    conflict = replace(first, rate_text="7.79")
    with pytest.raises(ValueError, match="reused with conflicting"):
        select_pit_fx(
            [first, conflict],
            base_currency="USD",
            quote_currency="HKD",
            cutoff="2025-08-14T09:00:00Z",
        )
    second_source = replace(first, event_id="other", evidence_id="fixture:other")
    with pytest.raises(ValueError, match="ambiguous PIT FX"):
        select_pit_fx(
            [first, second_source],
            base_currency="USD",
            quote_currency="HKD",
            cutoff="2025-08-14T09:00:00Z",
        )


def test_legacy_market_event_golden_and_version_are_unchanged():
    golden = Path(__file__).parent / "golden/v2/records.json"
    # Freeze text content rather than OS checkout bytes; LF and CRLF are equivalent.
    normalized = golden.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    crlf = normalized.replace("\n", "\r\n")
    expected = "e43ae9ea55b39746b5f9e8b45e7fe8f9e3320024d1c2fee9601553a61006cbd1"
    assert hashlib.sha256(normalized.encode("utf-8")).hexdigest() == expected
    assert hashlib.sha256(crlf.replace("\r\n", "\n").encode("utf-8")).hexdigest() == expected
    assert SCHEMA_VERSION_V2 == "2.0.0"
