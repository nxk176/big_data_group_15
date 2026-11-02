# HarborTrace contribution and evidence ledger

Use with [the independent-component plan](team_plan.md). This ledger starts after week five. **All entries are currently planned, not completed.** Allocation does not prove authorship.

Each person owns one major component through completion: Khai the processing engine, Vu data/infrastructure, Huy serving/analytics. Record work and merge evidence independently. There is no alternating weekly release lead and no requirement to wait for another component's weekly work. Keep integration status separate from component completion.

| Week | Khai: engine work / commit / check | Vu: data-infrastructure work / commit / check | Huy: serving-analytics work / commit / check |
|---|---|---|---|
| 6 | Planned | Planned | Planned |
| 7 | Planned | Planned | Planned |
| 8 | Planned | Planned | Planned |
| 9 | Planned | Planned | Planned |
| 10 | Planned | Planned | Planned |
| 11 | Planned | Planned | Planned |
| 12 | Planned | Planned | Planned |
| 13 | Planned | Planned | Planned |
| 14 | Planned | Planned | Planned |
| 15 | Planned | Planned | Planned |

## Integration evidence

| Gate | Coordination | Status | Actual run / result / boundary defects |
|---|---|---|---|
| Week 6 contract agreement | Khai; all owners review their boundaries | Planned | Pending |
| End of week 9: real local pipeline | Khai | Planned | Pending |
| End of week 12: deployed pipeline and real analytics | Vu | Planned | Pending |
| End of week 14: final reproduction | Huy | Planned | Pending |

Coordination means arranging the joint check and recording gaps, not taking over everyone's implementation. Each component owner resolves their defects.

## Per-person entry template

```text
Week / member / owned component:
Problem and acceptance criterion:
Inherited baseline / assistance / external attribution:
Actual changes and owned files:
Commit(s) / PR (pull request):
Standalone command / environment / contract version:
Input fixture or real manifest (label which):
Observed result / evidence / limitations:
Reviewer / feedback resolved:
Actual effort / requested workload adjustment:
Report section / individual demonstration:
Component status: planned | in progress | blocked | reviewed | merged
Integration status, if tested / boundary defect / owner / retry date:
```

## First shared cycle

Each member reproduces the published week-five baseline, agrees the boundary contract, creates their own standalone fixture-based demonstration, and authors a substantive reviewed change using their own Git identity. Do not claim that reproducing an existing example is new feature authorship. Meet the lecturer's minimum individual-commit requirement through real work in week 6, then maintain useful weekly contributions. Link actual commits/results; do not fabricate credits, hours or completed checks.

Vu records deployment resource availability; Huy records real-data analytics criteria; all three review feasibility early. A missing external resource is tracked as a specific blocker, not a reason to stop unrelated component work.
