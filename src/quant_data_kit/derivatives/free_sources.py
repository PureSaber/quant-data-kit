"""Three public derivative sources with explicit research-use restrictions."""

from .bundle import write_bundle
from .free_common import Resources, margin_values, public_request, validate_request
from .free_daily import cboe_vix, nse_fo
from .free_deribit import current_snapshot, historical_trades
from .models import decimal


def collect_free(
    output,
    *,
    provider,
    kind,
    selection,
    start,
    end,
    rights_note,
    source_mode="daily",
    tick="0.05",
    initial_margin=None,
    maintenance_margin=None,
    files_directory=None,
    fetch=public_request,
    now=None,
):
    selected, first, last = validate_request(provider, kind, selection, start, end, source_mode)
    if not isinstance(rights_note, str) or not 1 <= len(rights_note.strip()) <= 1000:
        raise ValueError("declare personal research rights/use scope without credentials")
    tick = decimal(tick)
    if not 0 < tick <= 10000:
        raise ValueError("positive analytical/research tick within 10000 required")
    margins = margin_values(initial_margin, maintenance_margin)
    resources = Resources(fetch=fetch, directory=files_directory)
    if provider == "cboe-vix":
        contracts, quotes, quality = cboe_vix(
            resources, selected, first, last, tick=tick, margins=margins
        )
    elif provider == "nse-fo":
        contracts, quotes, quality = nse_fo(
            resources, selected, first, last, kind=kind, tick=tick, margins=margins
        )
    else:
        if margins != (None, None):
            raise ValueError("coin-settled Deribit projections cannot enable account replay")
        if source_mode == "snapshot":
            contracts, quotes, quality = current_snapshot(resources, selected, tick=tick, now=now)
        else:
            contracts, quotes, quality = historical_trades(
                resources, selected, first, last, tick=tick
            )
    allowed = quality.pop(
        "allowed_modes", ["analysis", "replay"] if margins[0] is not None else ["analysis"]
    )
    quotes.sort(key=lambda q: (q.at, q.instrument_id))
    contracts.sort(key=lambda c: c.instrument_id)
    suggestion = {"mode": "analysis"}
    if kind == "option":
        suggestion["legs"] = contracts[0].instrument_id + "=1"
    else:
        suggestion["product"] = contracts[0].product
    source = {
        "schema": "qdk.free-derivatives/v1",
        "provider": provider,
        "source_mode": source_mode,
        "selection": selected,
        "requested_start": start,
        "requested_end": end,
        "allowed_modes": allowed,
        "suggested_config": suggestion,
        "resources": resources.receipts,
        "quality": quality,
        "tick_assumption": str(tick),
        "margin_assumptions": [str(m) if m is not None else None for m in margins],
    }
    limits = "Public source does not imply redistribution rights. Retrospective research only; actual historical publication/contract listing times are not certified. Zero-volume daily rows are excluded and missing source dates disclosed; no complete calendar coverage claim. Tick/margin values are explicit research assumptions. "
    if provider == "deribit":
        limits += "ANALYSIS ONLY: BTC/ETH premium times observed USD index is a valuation projection. The native coin settlement, variable tick, collateral and inverse payoff are not implemented in this USD account; no replay or executable bid/ask claim. "
    else:
        limits += "Index contracts only; sample ends before expiry; final fixing and historical exchange margin are not inferred. Replay requires explicit per-contract margin assumptions. "
    limits += "Price basis: " + quality.get(
        "price_basis", "source daily traded OHLC plus explicitly reported settlement"
    )
    return write_bundle(
        output,
        contracts,
        quotes,
        provider=provider,
        evidence_kind="retrospective",
        rights_note=rights_note,
        limits=limits,
        source=source,
    )
