"""Reading a stream output directory: batch order, update order and the provisional view."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from engine_stream import provisional_view, read_artifact  # noqa: E402
from harbortrace.streaming import iter_updates, read_updates  # noqa: E402


def visit(visit_id: str, last_inside: str) -> dict:
    return {"visit_id": visit_id, "last_inside": last_inside}


def update(cohort: str, seq: int, visits: list[dict], deleted: tuple = ()) -> dict:
    return {"cohort_id": cohort, "seq": seq, "visits": visits, "deleted_visit_ids": list(deleted)}


def write_batch(output: Path, batch: int, parts: list[list[dict]]) -> None:
    directory = output / "updates" / f"batch={batch:08d}"
    directory.mkdir(parents=True)
    for index, rows in enumerate(parts):
        (directory / f"part-{index:05d}.txt").write_text("".join(json.dumps(row) + "\n" for row in rows))
    (directory / "_SUCCESS").write_text("")


def old_view(output: Path) -> dict:
    """The provisional view as engine_stream built it before reading one batch at a time."""
    view = {}
    for row in read_updates(output):
        view.update({item["visit_id"]: item for item in row["visits"]})
    return view


def sample(output: Path) -> None:
    # Updates inside a batch are written in any order; batch 10 sorts after batch 2 by name.
    write_batch(output, 2, [[update("b", 1, [visit("v3", "t1")])],
                            [update("a", 2, [visit("v1", "t2")]), update("a", 1, [visit("v1", "t1")])]])
    write_batch(output, 10, [[update("a", 3, [visit("v1", "t3"), visit("v2", "t3")])]])
    write_batch(output, 11, [[]])


def test_updates_come_in_batch_then_cohort_and_seq_order(tmp_path):
    sample(tmp_path)
    order = [(row["cohort_id"], row["seq"]) for row in iter_updates(tmp_path)]
    assert order == [("a", 1), ("a", 2), ("b", 1), ("a", 3)]
    assert list(iter_updates(tmp_path)) == read_updates(tmp_path)


def test_streamed_view_equals_the_whole_list_view(tmp_path):
    sample(tmp_path)
    view = provisional_view(tmp_path)
    assert view == old_view(tmp_path)
    assert {key: row["last_inside"] for key, row in view.items()} == {"v1": "t3", "v2": "t3", "v3": "t1"}


def test_deleted_visits_leave_the_view(tmp_path):
    sample(tmp_path)
    write_batch(tmp_path, 12, [[update("a", 4, [], deleted=("v2",))]])
    assert set(provisional_view(tmp_path)) == {"v1", "v3"}


def test_artifact_is_read_line_by_line(tmp_path):
    rows = [visit("v1", "t1"), visit("v2", "t2")]
    (tmp_path / "visits.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    assert read_artifact(tmp_path) == {"v1": rows[0], "v2": rows[1]}
