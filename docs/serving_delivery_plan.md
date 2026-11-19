# Serving delivery plan: weeks 7–15

Date: 20 November 2026.
Owner: Nguyen Quang Huy (20235505).

The requested PR extends the existing serving component after week 6.
The implementation keeps contract `1.0` and the existing engine interfaces.
Local experiments use isolated databases and labelled inputs.

## Design

The engine writes completed batch artifacts and provisional update directories.
Serving imports complete artifacts and applies updates in numeric batch order.
The importer ignores unfinished batches and relies on MongoDB state for safe retries.
Conflicting updates cause failure and retain the last complete view.

MongoDB publication must reject expired publisher leases.
Concurrent provisional writers must preserve the winning update and readers of the previous snapshot.
Snapshot identity separates uncommitted rows from committed rows.
Old snapshots remain available to readers until an explicit maintenance action.

The dashboard applies the same filters to its tables, summaries, and time chart.
Analytics exports correct CSV and records input provenance, timing, and sample limitations.
The report links reproducible commands to observed evidence and outstanding external dependencies.

## Work units

| Unit | Files | Acceptance |
|---|---|---|
| Publication and updates | `src/harbortrace/serving.py`, serving tests | Expired leases fail. racing updates and pinned readers retain complete snapshots |
| Engine directory import | `src/harbortrace/update_import.py`, `scripts/serving_import.py`, importer tests | Numeric ordering, completion markers, retries, invalid input, conflicts, and restart behavior |
| Analytics | `src/harbortrace/analytics.py`, `scripts/analytics_run.py`, analytics tests | CSV preserves values. outputs include provenance and measurements. aggregate work avoids repeated actions |
| Dashboard | `dashboard/app.py`, dashboard tests | Consistent date, status, vessel, and port filters. empty results. many-port summaries |
| Evidence and report | Component guide, report, evidence index | Separate measured local behavior from real-data, review, and deployed requirements |

## Execution

1. Run the existing serving and dashboard tests before implementation.
2. Add regression tests before each behavior change.
3. Implement independent code units in separate file groups.
4. Run the component suites and the full suite with Java 17.
5. Run real MongoDB integration and an engine-to-dashboard example.
6. Record observed results and remaining dependencies.
7. Review the final diff, commit, push the branch, and create the PR.

## Completion boundaries

No shared cluster, remote service credentials, or larger real artifacts are available in this checkout at the initial audit.
The committed real AIS excerpt supports a small local integration example.
Synthetic input cannot establish model quality or real-world accuracy.
Cloud deployment, sufficient real samples, peer review, and the human case review require separate evidence.
The PR must state these gaps instead of marking all future weeks complete.
