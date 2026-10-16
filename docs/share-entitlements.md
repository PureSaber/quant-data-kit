# Evidenced share entitlements

`CorporateActionEvent.share_entitlement` is an optional, account-specific extension.
Legacy JSON bytes and legacy Arrow corporate-action tables remain accepted; the
new Arrow column is nullable. Readers of the old package must not read events
with the new extension. Upgrade the data and execution packages together.

`ordinary_additional_ratio` is shares added per ordinary shareholder share, not
the issuer's total capital expansion or its exchange reference-price adjustment.
The record-date quantity, evidence availability, credit time and sellable date
are separate. Evidence includes a source and SHA-256. Confirmed registry quantity
is bound to the named account. An unknown fractional allocation raises
`EntitlementUnresolved`; it never creates fractional tradable shares or cash.

An explicitly labelled sensitivity branch may use floor/ceiling only where the
documented allocation rule permits it. A floor-only rule rejects the ceiling
branch. Sensitivity quantities are not registered historical facts. A zero
entitlement is harmless even if other holders' fractional outcomes are unknown.

The stable entitlement ID and account form the event ID. Corrections reuse that
identity and are rejected if already applied with different content. Restore an
account checkpoint before the event and replay a new version to examine a
revision; retain the old events, checkpoint and ledger. Do not rename a corrected
event merely to make it apply again.

`tests/golden/v2/share_credit.json` is the exact new contract example; the existing
v2 golden records remain unchanged.
