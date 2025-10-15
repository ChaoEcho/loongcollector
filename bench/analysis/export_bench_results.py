#!/usr/bin/env python3
"""Extract benchmark result summary rows into a CSV file."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass
class Row:
    timestamp_utc: str
    rate_mb: float
    target: str
    elapsed_sec: float
    msg_per_sec: float
    approx_mb_per_sec: float
    kafka_payload_mb_per_sec: float | None
    source_mb_per_sec: float | None

    def to_csv_row(self) -> List[str]:
        return [
            self.timestamp_utc,
            f"{self.rate_mb:.2f}",
            self.target,
            f"{self.elapsed_sec:.2f}",
            f"{self.msg_per_sec:.2f}",
            f"{self.approx_mb_per_sec:.2f}",
            (
                ""
                if self.kafka_payload_mb_per_sec is None
                else f"{self.kafka_payload_mb_per_sec:.2f}"
            ),
            "" if self.source_mb_per_sec is None else f"{self.source_mb_per_sec:.2f}",
        ]


def iter_result_files(results_dir: Path) -> Iterable[Path]:
    for path in sorted(results_dir.glob("benchmark-*.json")):
        if path.is_file():
            yield path


def load_rows(result_path: Path) -> Iterable[Row]:
    with result_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    timestamp = payload.get("timestamp_utc", result_path.stem.split("benchmark-")[-1])
    rate = float(payload["params"]["rate_mb"])
    results = payload.get("results", [])

    for entry in results:
        yield Row(
            timestamp_utc=timestamp,
            rate_mb=rate,
            target=str(entry.get("target", "")),
            elapsed_sec=float(entry.get("elapsed_sec", 0.0)),
            msg_per_sec=float(entry.get("msg_per_sec", 0.0)),
            approx_mb_per_sec=float(entry.get("approx_mb_per_sec", 0.0)),
            kafka_payload_mb_per_sec=(
                None
                if entry.get("kafka_payload_mb_per_sec") in (None, "")
                else float(entry["kafka_payload_mb_per_sec"])
            ),
            source_mb_per_sec=(
                None
                if entry.get("source_mb_per_sec") in (None, "")
                else float(entry["source_mb_per_sec"])
            ),
        )


def filter_by_span(
    rows: Iterable[Row], start: str | None, end: str | None
) -> Iterable[Row]:
    for row in rows:
        if start is not None and row.timestamp_utc < start:
            continue
        if end is not None and row.timestamp_utc > end:
            continue
        yield row


def write_csv(rows: Iterable[Row], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "timestamp_utc",
                "rate_mb",
                "target",
                "elapsed_sec",
                "msg_per_sec",
                "approx_mb_per_sec",
                "kafka_payload_mb_per_sec",
                "source_mb_per_sec",
            ]
        )
        for row in rows:
            writer.writerow(row.to_csv_row())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract benchmark results into CSV")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("bench/results"),
        help="Directory containing benchmark-*.json files",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Inclusive starting timestamp (e.g. 20251015-020846)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="Inclusive ending timestamp (e.g. 20251015-023520)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("bench/results/benchmark_series.csv"),
        help="Output CSV path",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    all_rows: List[Row] = []
    for result_file in iter_result_files(args.results_dir):
        all_rows.extend(load_rows(result_file))

    filtered_rows = list(filter_by_span(all_rows, args.start, args.end))
    filtered_rows.sort(key=lambda r: (r.timestamp_utc, r.target))

    write_csv(filtered_rows, args.output)

    print(f"Wrote {len(filtered_rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
