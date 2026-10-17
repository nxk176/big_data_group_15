# Visit rule decision table (visits-v1)

All observations are sorted by `(mmsi, event_time_utc, logical_id, source_record_id)` before entering one state machine per vessel and port. The point-in-polygon predicate is `covers`, so an exact boundary point counts as inside but receives `near_boundary` when within the configured distance. Overlap between selected polygons is ambiguous and does not establish inside or outside for either port. A duplicate logical observation is ignored. Same-MMSI/time observations at different positions are quarantined before this machine.

| Current state | Input / condition | Action | Output |
|---|---|---|---|
| No candidate | Outside | Remember last outside time | None |
| No candidate | Inside | Start candidate; entry interval starts at last outside, or left censored | None |
| Candidate | Inside, elapsed inside observations >= 10 minutes | Confirm presence | None |
| Candidate/confirmed | First outside | Start candidate exit | None |
| Candidate exit | Inside before exit persistence | Cancel exit; flag short excursion or boundary jitter | None |
| Candidate exit | Another outside >= 10 minutes after first outside | Complete visit; possible exit interval is last inside to first outside | One visit |
| Any active state | Consecutive observation gap > 30 minutes | Close at last inside with unknown continuity; begin fresh state for current observation | Censored visit |
| Any active state | Ambiguous polygon assignment | Preserve ambiguity flag; do not treat as inside or outside | None |
| Active state | End of input range | Close without a known exit | Right-censored visit |

The first and last observed inside timestamps define `observed_span_seconds`. It is **not** a claim of uninterrupted occupancy. A candidate with too little observed time is retained with `insufficient_confirmation` and status `candidate`; analysis excludes it by default. `source_record_ids` retains the first 16 IDs; a rolling digest and count cover the full visit without unbounded memory. Each port machine stores constant state per vessel. Confirmation, exit persistence, and gap thresholds are hypotheses to vary in sensitivity tests, not maritime facts.
