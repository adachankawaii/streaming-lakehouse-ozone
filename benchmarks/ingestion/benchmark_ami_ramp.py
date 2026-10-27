#!/usr/bin/env python3
"""
Report-grade AMI RAMP benchmark runner/collector (collector v4).

Runs:
    AMI simulator: 1K -> 2K -> 5K events/s
Collects:
    - Kafka broker append rate from topic LOG-END-OFFSET deltas
    - Kafka committed-consume rate and lag
    - Flink source records-out rate
    - Flink checkpoint counts/duration/size
    - Iceberg sink commit metrics
    - Ozone/Iceberg physical namespace inventory at a lower frequency

No third-party Python packages are required.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import queue
import re
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


DEFAULT_JOB_MATCH = "insert-into_lakehouse.bronze.ami_meter_raw"
DEFAULT_TOPIC = "ami.meter.events"
DEFAULT_GROUP = "bronze-ami-meter-v1"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--stage-seconds", type=int, default=60)
    p.add_argument("--sample-interval", type=float, default=5.0)
    p.add_argument("--storage-sample-interval", type=float, default=30.0)
    p.add_argument("--drain-seconds", type=int, default=30)
    p.add_argument("--flink-url", default="http://localhost:8081")
    p.add_argument("--job-match", default=DEFAULT_JOB_MATCH)
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--output-root", default="benchmarks/results")
    p.add_argument(
        "--skip-simulator",
        action="store_true",
        help="Collect only; useful for debugging an already-running workload.",
    )
    p.add_argument(
        "--collector-seconds",
        type=int,
        default=0,
        help="Required with --skip-simulator. 0 otherwise uses ramp + drain.",
    )
    return p.parse_args()


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run_cmd(args: list[str], timeout: int = 30, check: bool = True) -> str:
    proc = subprocess.run(
        args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {' '.join(args)}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc.stdout


def get_json(url: str, timeout: int = 10) -> Any:
    with urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def as_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value, default=None):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def find_running_job(flink_url: str, name_match: str):
    overview = get_json(f"{flink_url}/jobs/overview")
    candidates = []

    for job in overview.get("jobs", []):
        if (
            job.get("state") in {"RUNNING", "RESTARTING"}
            and name_match in job.get("name", "")
        ):
            candidates.append(job)

    if not candidates:
        running = [
            f"{j.get('jid')} {j.get('state')} {j.get('name')}"
            for j in overview.get("jobs", [])
            if j.get("state") in {"RUNNING", "RESTARTING"}
        ]
        raise RuntimeError(
            f"No running Flink job matched: {name_match!r}\n"
            f"Running jobs:\n" + "\n".join(running)
        )

    if len(candidates) > 1:
        raise RuntimeError(
            "More than one matching Bronze job is running. "
            "Cancel duplicates before benchmarking:\n"
            + "\n".join(f"{j['jid']} {j['name']}" for j in candidates)
        )

    return candidates[0]


def find_vertices(flink_url: str, job_id: str):
    job = get_json(f"{flink_url}/jobs/{job_id}")
    source = None
    committer = None

    for vertex in job.get("vertices", []):
        name = vertex.get("name", "")
        if "kafka_ami_meter_events" in name and "IcebergStreamWriter" in name:
            source = vertex
        if "IcebergFilesCommitter" in name:
            committer = vertex

    if source is None or committer is None:
        details = "\n".join(
            f"{v.get('id')} | {v.get('status')} | {v.get('name')}"
            for v in job.get("vertices", [])
        )
        raise RuntimeError(
            "Could not identify Bronze source/writer and Iceberg committer vertices.\n"
            + details
        )

    return source, committer


def list_metric_ids(flink_url: str, job_id: str, vertex_id: str):
    data = get_json(
        f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics"
    )
    return [item.get("id", "") for item in data if item.get("id")]


def select_metric(metric_ids: list[str], suffix: str):
    if suffix in metric_ids:
        return suffix

    exact_end = [m for m in metric_ids if m.endswith(suffix)]
    if exact_end:
        exact_end.sort(key=len)
        return exact_end[0]

    contains = [m for m in metric_ids if suffix in m]
    if contains:
        contains.sort(key=len)
        return contains[0]

    return None


def query_aggregated_metric(
    flink_url: str,
    job_id: str,
    vertex_id: str,
    metric_id: str | None,
    agg: str,
):
    if not metric_id:
        return None

    query = urlencode({"get": metric_id, "agg": agg})
    url = (
        f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}"
        f"/subtasks/metrics?{query}"
    )
    data = get_json(url)
    if not data:
        return None

    item = data[0]
    return as_float(item.get(agg) or item.get("value"))



def query_subtask_metric_sum(
    flink_url: str,
    job_id: str,
    vertex_id: str,
    metric_id: str | None,
):
    """
    Query a metric from each subtask individually and sum numeric values.

    This is intentionally used for KafkaSource operator metrics such as
    pendingRecords because the aggregated-subtasks REST endpoint can expose
    the metric id while returning an empty aggregate for some operator gauges.
    """
    if not metric_id:
        return None

    vertex = get_json(f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}")
    parallelism = int(vertex.get("parallelism", 0))

    if parallelism <= 0:
        return None

    total = 0.0
    seen_numeric = False

    for subtask_index in range(parallelism):
        query = urlencode({"get": metric_id})
        url = (
            f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}"
            f"/subtasks/{subtask_index}/metrics?{query}"
        )

        try:
            data = get_json(url)
        except Exception:
            continue

        if not data:
            continue

        value = as_float(data[0].get("value"))
        if value is None:
            continue

        total += value
        seen_numeric = True

    return total if seen_numeric else None



def query_subtask_metric_ids_sum(
    flink_url: str,
    job_id: str,
    vertex_id: str,
    metric_ids: list[str],
):
    """Sum multiple metric IDs across all subtasks."""
    if not metric_ids:
        return None

    total = 0.0
    seen_numeric = False

    for metric_id in metric_ids:
        value = query_subtask_metric_sum(
            flink_url,
            job_id,
            vertex_id,
            metric_id,
        )
        if value is None:
            continue
        total += value
        seen_numeric = True

    return total if seen_numeric else None

def kafka_group_offsets(topic: str, group: str):
    output = run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "kafka:19092",
            "--describe",
            "--group",
            group,
        ],
        timeout=30,
        check=False,
    )

    partitions = {}
    # Expected data row:
    # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
    for raw in output.splitlines():
        line = raw.strip()
        if not line or line.startswith("GROUP ") or topic not in line:
            continue

        cols = re.split(r"\s+", line)
        try:
            topic_idx = cols.index(topic)
            partition = int(cols[topic_idx + 1])
            current = int(cols[topic_idx + 2])
            log_end = int(cols[topic_idx + 3])
            lag = int(cols[topic_idx + 4])
        except (ValueError, IndexError):
            continue

        partitions[partition] = {
            "current": current,
            "log_end": log_end,
            "lag": lag,
        }

    if not partitions:
        raise RuntimeError(
            "Could not parse Kafka consumer-group offsets.\nRaw output:\n" + output
        )

    return partitions


def checkpoint_stats(flink_url: str, job_id: str):
    data = get_json(f"{flink_url}/jobs/{job_id}/checkpoints")
    counts = data.get("counts", {})
    latest = data.get("latest", {}).get("completed") or {}

    return {
        "checkpoint_total": counts.get("total", 0),
        "checkpoint_completed": counts.get("completed", 0),
        "checkpoint_failed": counts.get("failed", 0),
        "checkpoint_in_progress": counts.get("in_progress", 0),
        "latest_checkpoint_id": latest.get("id"),
        "latest_checkpoint_duration_ms": latest.get("end_to_end_duration"),
        "latest_checkpoint_size_bytes": latest.get("checkpointed_size"),
        "latest_checkpoint_path": latest.get("external_path"),
        "latest_checkpoint_trigger_ms": latest.get("trigger_timestamp"),
    }


def discover_ozone_table_path():
    output = run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "om",
            "ozone",
            "fs",
            "-ls",
            "-R",
            "ofs://om/s3v/warehouse",
        ],
        timeout=60,
    )

    roots = []
    marker = "/ami_meter_raw-"

    for line in output.splitlines():
        if marker not in line:
            continue
        path = line.split()[-1]
        idx = path.find(marker)
        if idx >= 0:
            root = path[: idx + len(marker)] + path[idx + len(marker):].split("/")[0]
            roots.append(root)

    roots = sorted(set(roots), key=len)
    if not roots:
        return None

    return roots[0]


def ozone_inventory(table_path: str | None):
    if not table_path:
        return {}

    output = run_cmd(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "om",
            "ozone",
            "fs",
            "-ls",
            "-R",
            table_path,
        ],
        timeout=60,
    )

    result = {
        "ozone_data_files": 0,
        "ozone_metadata_json_files": 0,
        "ozone_manifest_files": 0,
        "ozone_snapshot_files": 0,
        "ozone_total_files": 0,
        "ozone_logical_bytes": 0,
    }

    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("-"):
            continue

        cols = re.split(r"\s+", line)
        if len(cols) < 8:
            continue

        try:
            size = int(cols[4])
        except ValueError:
            size = 0

        path = cols[-1]
        filename = path.rsplit("/", 1)[-1]

        result["ozone_total_files"] += 1
        result["ozone_logical_bytes"] += size

        if "/data/" in path and filename.endswith(".parquet"):
            result["ozone_data_files"] += 1

        if filename.endswith(".metadata.json"):
            result["ozone_metadata_json_files"] += 1

        if re.search(r"-m\d+\.avro$", filename):
            result["ozone_manifest_files"] += 1

        if filename.startswith("snap-") and filename.endswith(".avro"):
            result["ozone_snapshot_files"] += 1

    return result


class SimulatorState:
    def __init__(self):
        self.lock = threading.Lock()
        self.stage = "startup"
        self.target_rate = 0.0
        self.finished = False

    def update_from_line(self, line: str):
        # New simulator output:
        # Stage: ramp-1k | target=1,000.00/s | acceleration=300.0x
        match = re.search(
            r"Stage:\s*([^|]+)\|\s*target=([\d,]+(?:\.\d+)?)/s",
            line,
        )
        if match:
            stage = match.group(1).strip()
            rate = float(match.group(2).replace(",", ""))
            with self.lock:
                self.stage = stage
                self.target_rate = rate

    def mark_finished(self):
        with self.lock:
            self.finished = True
            self.stage = "drain"
            self.target_rate = 0.0

    def snapshot(self):
        with self.lock:
            return self.stage, self.target_rate, self.finished


def stream_simulator_output(proc, log_file: Path, state: SimulatorState):
    with log_file.open("w", encoding="utf-8", newline="\n") as f:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            f.write(line)
            f.flush()
            state.update_from_line(line)
    state.mark_finished()


def percentile(values, q):
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return vals[lo]
    return vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


def write_stage_summary(samples, checkpoints, commits, output_path: Path):
    fields = [
        "stage",
        "samples",
        "target_rate_eps",
        "avg_kafka_input_rate_eps",
        "p95_kafka_input_rate_eps",
        "avg_flink_source_read_rate_eps",
        "max_kafka_lag",
        "end_kafka_lag",
        "checkpoint_count",
        "checkpoint_duration_p50_ms",
        "checkpoint_duration_p95_ms",
        "iceberg_commit_events",
        "iceberg_commit_duration_p50_ms",
        "iceberg_commit_duration_p95_ms",
        "max_seconds_since_last_commit",
        "end_committed_records",
        "end_committed_data_files",
        "end_ozone_data_files",
        "end_ozone_logical_bytes",
    ]

    stages = []
    for row in samples:
        stage = row["stage"]
        if stage not in stages and stage not in {"startup", "drain"}:
            stages.append(stage)

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for stage in stages:
            rows = [r for r in samples if r["stage"] == stage]
            cps = [r for r in checkpoints if r["stage"] == stage]
            cms = [r for r in commits if r["stage"] == stage]

            input_rates = [
                as_float(r.get("kafka_input_rate_eps"))
                for r in rows
                if as_float(r.get("kafka_input_rate_eps")) is not None
            ]
            flink_rates = [
                as_float(r.get("flink_source_read_rate_eps"))
                for r in rows
                if as_float(r.get("flink_source_read_rate_eps")) is not None
            ]
            lags = [
                as_float(r.get("kafka_lag_total"))
                for r in rows
                if as_float(r.get("kafka_lag_total")) is not None
            ]
            cp_durations = [
                as_float(r.get("duration_ms"))
                for r in cps
                if as_float(r.get("duration_ms")) is not None
            ]
            commit_durations = [
                as_float(r.get("last_commit_duration_ms"))
                for r in cms
                if as_float(r.get("last_commit_duration_ms")) is not None
            ]
            since_commit = [
                as_float(r.get("iceberg_seconds_since_last_commit"))
                for r in rows
                if as_float(r.get("iceberg_seconds_since_last_commit")) is not None
            ]

            last = rows[-1]

            writer.writerow(
                {
                    "stage": stage,
                    "samples": len(rows),
                    "target_rate_eps": last.get("target_rate_eps"),
                    "avg_kafka_input_rate_eps": round(statistics.mean(input_rates), 2) if input_rates else "",
                    "p95_kafka_input_rate_eps": round(percentile(input_rates, 0.95), 2) if input_rates else "",
                    "avg_flink_source_read_rate_eps": round(statistics.mean(flink_rates), 2) if flink_rates else "",
                    "max_kafka_lag": int(max(lags)) if lags else "",
                    "end_kafka_lag": last.get("kafka_lag_total", ""),
                    "checkpoint_count": len(cps),
                    "checkpoint_duration_p50_ms": round(percentile(cp_durations, 0.50), 2) if cp_durations else "",
                    "checkpoint_duration_p95_ms": round(percentile(cp_durations, 0.95), 2) if cp_durations else "",
                    "iceberg_commit_events": len(cms),
                    "iceberg_commit_duration_p50_ms": round(percentile(commit_durations, 0.50), 2) if commit_durations else "",
                    "iceberg_commit_duration_p95_ms": round(percentile(commit_durations, 0.95), 2) if commit_durations else "",
                    "max_seconds_since_last_commit": max(since_commit) if since_commit else "",
                    "end_committed_records": last.get("iceberg_committed_records", ""),
                    "end_committed_data_files": last.get("iceberg_committed_data_files", ""),
                    "end_ozone_data_files": last.get("ozone_data_files", ""),
                    "end_ozone_logical_bytes": last.get("ozone_logical_bytes", ""),
                }
            )


def main():
    args = parse_args()

    if args.stage_seconds <= 0:
        raise ValueError("--stage-seconds must be > 0")
    if args.sample_interval <= 0:
        raise ValueError("--sample-interval must be > 0")
    if args.storage_sample_interval <= 0:
        raise ValueError("--storage-sample-interval must be > 0")

    repo_root = Path.cwd()
    run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = repo_root / args.output_root / f"{run_stamp}-ami-ramp"
    run_dir.mkdir(parents=True, exist_ok=True)

    print("Finding running Bronze Flink job...")
    job = find_running_job(args.flink_url, args.job_match)
    job_id = job["jid"]
    print(f"Job ID: {job_id}")
    print(f"Job   : {job['name']}")

    source_vertex, committer_vertex = find_vertices(args.flink_url, job_id)
    print(f"Source vertex   : {source_vertex['id']}")
    print(f"Committer vertex: {committer_vertex['id']}")

    source_metric_ids = list_metric_ids(
        args.flink_url, job_id, source_vertex["id"]
    )
    committer_metric_ids = list_metric_ids(
        args.flink_url, job_id, committer_vertex["id"]
    )

    metric_map = {
        "flink_pending_records": select_metric(
            source_metric_ids, "pendingRecords"
        ),
        "flink_current_offset_metrics": sorted(
            metric_id
            for metric_id in source_metric_ids
            if ".KafkaSourceReader.topic.ami_meter_events.partition."
            in metric_id
            and metric_id.endswith(".currentOffset")
        ),
        "flink_source_native_rate": next(
            (
                metric_id
                for metric_id in source_metric_ids
                if "Source__kafka_ami_meter_events" in metric_id
                and metric_id.endswith(".numRecordsOutPerSecond")
            ),
            None,
        ),
        "flink_kafka_consumer_rate": next(
            (
                metric_id
                for metric_id in source_metric_ids
                if metric_id.endswith(
                    ".KafkaSourceReader.KafkaConsumer.records-consumed-rate"
                )
            ),
            None,
        ),
        "flink_commits_succeeded": next(
            (
                metric_id
                for metric_id in source_metric_ids
                if metric_id.endswith(".KafkaSourceReader.commitsSucceeded")
            ),
            None,
        ),
        "flink_commits_failed": next(
            (
                metric_id
                for metric_id in source_metric_ids
                if metric_id.endswith(".KafkaSourceReader.commitsFailed")
            ),
            None,
        ),
        "iceberg_last_checkpoint_duration_ms": select_metric(
            committer_metric_ids, "lastCheckpointDurationMs"
        ),
        "iceberg_last_commit_duration_ms": select_metric(
            committer_metric_ids, "lastCommitDurationMs"
        ),
        "iceberg_committed_data_files": select_metric(
            committer_metric_ids, "committedDataFilesCount"
        ),
        "iceberg_committed_records": select_metric(
            committer_metric_ids, "committedDataFilesRecordCount"
        ),
        "iceberg_committed_bytes": select_metric(
            committer_metric_ids, "committedDataFilesByteCount"
        ),
        "iceberg_seconds_since_last_commit": select_metric(
            committer_metric_ids, "elapsedSecondsSinceLastSuccessfulCommit"
        ),
    }

    print("Resolved Flink/Iceberg metrics:")
    for key, value in metric_map.items():
        print(f"  {key}: {value or '<not exposed>'}")

    table_path = discover_ozone_table_path()
    print(f"Ozone table path: {table_path or '<not found>'}")

    try:
        start_offsets = kafka_group_offsets(args.topic, args.group)
    except Exception:
        start_offsets = {}

    start_kafka_committed_lag = sum(
        value.get("lag", 0) for value in start_offsets.values()
    )

    initial_pending = query_subtask_metric_sum(
        args.flink_url,
        job_id,
        source_vertex["id"],
        metric_map["flink_pending_records"],
    )

    if (
        initial_pending is not None
        and initial_pending > 0
        and not args.skip_simulator
    ):
        raise RuntimeError(
            "Benchmark requires a clean Flink KafkaSource start, but "
            f"pendingRecords={int(initial_pending)}. "
            "Let the Bronze ingestion job catch up before starting a new run."
        )

    manifest = {
        "run_id": run_dir.name,
        "created_at_utc": now_iso(),
        "job_id": job_id,
        "job_name": job["name"],
        "topic": args.topic,
        "consumer_group": args.group,
        "stage_seconds": args.stage_seconds,
        "sample_interval_seconds": args.sample_interval,
        "storage_sample_interval_seconds": args.storage_sample_interval,
        "drain_seconds": args.drain_seconds,
        "ozone_table_path": table_path,
        "metric_map": metric_map,
        "kafka_offsets_start": start_offsets,
        "kafka_committed_lag_start": start_kafka_committed_lag,
        "flink_pending_records_start": (
            int(initial_pending) if initial_pending is not None else None
        ),
        "notes": {
            "kafka_input_rate_eps": (
                "Delta of Kafka LOG-END-OFFSET / wall-clock seconds."
            ),
            "kafka_committed_consume_rate_eps": (
                "Delta of Kafka consumer-group committed CURRENT-OFFSET / "
                "wall-clock seconds. Diagnostic only; it is not treated as "
                "the authoritative live Flink backlog."
            ),
            "flink_pending_records": (
                "Native KafkaSource pendingRecords aggregated across source "
                "subtasks. This is the primary live-source backlog signal."
            ),
            "flink_source_read_rate_eps": (
                "Delta of the sum of KafkaSource per-partition currentOffset "
                "gauges divided by wall-clock time. Primary processed/read "
                "throughput metric."
            ),
            "flink_source_native_rate_eps": (
                "Native KafkaSource numRecordsOutPerSecond rate meter. Kept "
                "for diagnostics because it is smoothed and can remain nonzero "
                "briefly after input stops."
            ),
            "flink_kafka_consumer_rate_eps": (
                "KafkaConsumer records-consumed-rate aggregated across source "
                "subtasks; used as a cross-check."
            ),
            "ozone_logical_bytes": (
                "Logical file bytes reported by ozone fs -ls, not RATIS "
                "physical replicated bytes."
            ),
            "storage_inventory": (
                "Sampled less frequently to reduce observer overhead."
            ),
        },
    }

    (run_dir / "benchmark_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    metrics_csv = run_dir / "metrics.csv"
    checkpoints_csv = run_dir / "checkpoints.csv"
    commits_csv = run_dir / "iceberg_commits.csv"
    simulator_log = run_dir / "simulator.log"

    metric_fields = [
        "timestamp_utc",
        "elapsed_s",
        "stage",
        "target_rate_eps",
        "flink_job_state",
        "kafka_log_end_total",
        "kafka_committed_offset_total",
        "kafka_lag_total",
        "kafka_p0_current_offset",
        "kafka_p0_log_end",
        "kafka_p0_lag",
        "kafka_p1_current_offset",
        "kafka_p1_log_end",
        "kafka_p1_lag",
        "kafka_p2_current_offset",
        "kafka_p2_log_end",
        "kafka_p2_lag",
        "kafka_input_rate_eps",
        "kafka_committed_consume_rate_eps",
        "flink_pending_records",
        "flink_source_position_total",
        "flink_source_read_rate_eps",
        "flink_source_native_rate_eps",
        "flink_kafka_consumer_rate_eps",
        "flink_commits_succeeded",
        "flink_commits_failed",
        "checkpoint_total",
        "checkpoint_completed",
        "checkpoint_failed",
        "checkpoint_in_progress",
        "latest_checkpoint_id",
        "latest_checkpoint_duration_ms",
        "latest_checkpoint_size_bytes",
        "latest_checkpoint_path",
        "latest_checkpoint_trigger_ms",
        "iceberg_last_checkpoint_duration_ms",
        "iceberg_last_commit_duration_ms",
        "iceberg_committed_data_files",
        "iceberg_committed_records",
        "iceberg_committed_bytes",
        "iceberg_seconds_since_last_commit",
        "ozone_data_files",
        "ozone_metadata_json_files",
        "ozone_manifest_files",
        "ozone_snapshot_files",
        "ozone_total_files",
        "ozone_logical_bytes",
    ]

    checkpoint_fields = [
        "timestamp_utc",
        "stage",
        "checkpoint_id",
        "duration_ms",
        "size_bytes",
        "trigger_timestamp_ms",
        "external_path",
    ]

    commit_fields = [
        "timestamp_utc",
        "stage",
        "last_commit_duration_ms",
        "committed_data_files",
        "committed_records",
        "committed_bytes",
        "seconds_since_last_successful_commit",
    ]

    simulator_state = SimulatorState()
    simulator_proc = None
    simulator_thread = None

    if not args.skip_simulator:
        simulator_cmd = [
            "docker",
            "compose",
            "--profile",
            "simulator",
            "run",
            "--rm",
            "-e",
            "PYTHONUNBUFFERED=1",
            "ami-simulator",
            "--mode",
            "ramp",
            "--stage-seconds",
            str(args.stage_seconds),
        ]

        print("\nStarting AMI RAMP simulator...")
        simulator_proc = subprocess.Popen(
            simulator_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        simulator_thread = threading.Thread(
            target=stream_simulator_output,
            args=(simulator_proc, simulator_log, simulator_state),
            daemon=True,
        )
        simulator_thread.start()
        max_duration = args.stage_seconds * 3 + args.drain_seconds + 60
    else:
        if args.collector_seconds <= 0:
            raise ValueError(
                "--collector-seconds > 0 is required with --skip-simulator"
            )
        simulator_state.stage = "collector-only"
        max_duration = args.collector_seconds

    samples = []
    checkpoint_events = []
    commit_events = []

    last_log_end = None
    last_committed = None
    last_rate_time = None

    last_source_position = None
    last_source_position_time = None

    last_checkpoint_id = None
    last_committed_records = None

    last_inventory = {}
    next_storage_sample = 0.0

    start_mono = time.monotonic()
    drain_start = None

    with (
        metrics_csv.open("w", encoding="utf-8", newline="") as metrics_f,
        checkpoints_csv.open("w", encoding="utf-8", newline="") as cp_f,
        commits_csv.open("w", encoding="utf-8", newline="") as commit_f,
    ):
        metrics_writer = csv.DictWriter(metrics_f, fieldnames=metric_fields)
        cp_writer = csv.DictWriter(cp_f, fieldnames=checkpoint_fields)
        commit_writer = csv.DictWriter(commit_f, fieldnames=commit_fields)

        metrics_writer.writeheader()
        cp_writer.writeheader()
        commit_writer.writeheader()

        while True:
            loop_started = time.monotonic()
            elapsed = loop_started - start_mono
            stage, target_rate, sim_finished = simulator_state.snapshot()

            if sim_finished and drain_start is None:
                drain_start = loop_started

            if args.skip_simulator and elapsed >= max_duration:
                break

            if (
                not args.skip_simulator
                and drain_start is not None
                and loop_started - drain_start >= args.drain_seconds
            ):
                break

            if elapsed >= max_duration:
                print("Collector safety timeout reached.")
                break

            row = {field: "" for field in metric_fields}
            row["timestamp_utc"] = now_iso()
            row["elapsed_s"] = round(elapsed, 3)
            row["stage"] = stage
            row["target_rate_eps"] = target_rate

            try:
                overview = get_json(f"{args.flink_url}/jobs/{job_id}")
                row["flink_job_state"] = overview.get("state", "")
            except Exception as exc:
                row["flink_job_state"] = f"ERROR:{type(exc).__name__}"

            try:
                offsets = kafka_group_offsets(args.topic, args.group)
                log_end_total = sum(x["log_end"] for x in offsets.values())
                committed_total = sum(x["current"] for x in offsets.values())
                lag_total = sum(x["lag"] for x in offsets.values())

                row["kafka_log_end_total"] = log_end_total
                row["kafka_committed_offset_total"] = committed_total
                row["kafka_lag_total"] = lag_total

                for p in (0, 1, 2):
                    if p in offsets:
                        row[f"kafka_p{p}_current_offset"] = offsets[p]["current"]
                        row[f"kafka_p{p}_log_end"] = offsets[p]["log_end"]
                        row[f"kafka_p{p}_lag"] = offsets[p]["lag"]

                now_rate = time.monotonic()
                if (
                    last_rate_time is not None
                    and last_log_end is not None
                    and now_rate > last_rate_time
                ):
                    dt = now_rate - last_rate_time
                    row["kafka_input_rate_eps"] = round(
                        (log_end_total - last_log_end) / dt, 2
                    )
                    row["kafka_committed_consume_rate_eps"] = round(
                        (committed_total - last_committed) / dt, 2
                    )

                last_log_end = log_end_total
                last_committed = committed_total
                last_rate_time = now_rate
            except Exception as exc:
                print(f"[WARN] Kafka offset sample failed: {exc}")

            try:
                pending = query_subtask_metric_sum(
                    args.flink_url,
                    job_id,
                    source_vertex["id"],
                    metric_map["flink_pending_records"],
                )
                if pending is not None:
                    row["flink_pending_records"] = int(max(0.0, pending))
            except Exception as exc:
                print(f"[WARN] Flink pendingRecords sample failed: {exc}")

            try:
                source_position = query_subtask_metric_ids_sum(
                    args.flink_url,
                    job_id,
                    source_vertex["id"],
                    metric_map["flink_current_offset_metrics"],
                )
                if source_position is not None:
                    row["flink_source_position_total"] = int(source_position)

                    source_now = time.monotonic()
                    if (
                        last_source_position is not None
                        and last_source_position_time is not None
                        and source_now > last_source_position_time
                    ):
                        row["flink_source_read_rate_eps"] = round(
                            (source_position - last_source_position)
                            / (source_now - last_source_position_time),
                            2,
                        )

                    last_source_position = source_position
                    last_source_position_time = source_now
            except Exception as exc:
                print(f"[WARN] Flink currentOffset sample failed: {exc}")

            try:
                source_rate = query_subtask_metric_sum(
                    args.flink_url,
                    job_id,
                    source_vertex["id"],
                    metric_map["flink_source_native_rate"],
                )
                if source_rate is not None:
                    row["flink_source_native_rate_eps"] = round(
                        source_rate, 2
                    )
            except Exception as exc:
                print(f"[WARN] Flink source-rate sample failed: {exc}")

            try:
                kafka_consumer_rate = query_subtask_metric_sum(
                    args.flink_url,
                    job_id,
                    source_vertex["id"],
                    metric_map["flink_kafka_consumer_rate"],
                )
                if kafka_consumer_rate is not None:
                    row["flink_kafka_consumer_rate_eps"] = round(
                        kafka_consumer_rate, 2
                    )
            except Exception as exc:
                print(f"[WARN] Kafka consumer-rate sample failed: {exc}")

            try:
                commits_succeeded = query_subtask_metric_sum(
                    args.flink_url,
                    job_id,
                    source_vertex["id"],
                    metric_map["flink_commits_succeeded"],
                )
                if commits_succeeded is not None:
                    row["flink_commits_succeeded"] = int(
                        commits_succeeded
                    )
            except Exception as exc:
                print(f"[WARN] KafkaSource commitsSucceeded failed: {exc}")

            try:
                commits_failed = query_subtask_metric_sum(
                    args.flink_url,
                    job_id,
                    source_vertex["id"],
                    metric_map["flink_commits_failed"],
                )
                if commits_failed is not None:
                    row["flink_commits_failed"] = int(commits_failed)
            except Exception as exc:
                print(f"[WARN] KafkaSource commitsFailed failed: {exc}")

            try:
                cp = checkpoint_stats(args.flink_url, job_id)
                row.update(cp)

                cp_id = cp.get("latest_checkpoint_id")
                if cp_id is not None and cp_id != last_checkpoint_id:
                    cp_event = {
                        "timestamp_utc": now_iso(),
                        "stage": stage,
                        "checkpoint_id": cp_id,
                        "duration_ms": cp.get("latest_checkpoint_duration_ms"),
                        "size_bytes": cp.get("latest_checkpoint_size_bytes"),
                        "trigger_timestamp_ms": cp.get("latest_checkpoint_trigger_ms"),
                        "external_path": cp.get("latest_checkpoint_path"),
                    }
                    cp_writer.writerow(cp_event)
                    cp_f.flush()
                    checkpoint_events.append(cp_event)
                    last_checkpoint_id = cp_id
            except Exception as exc:
                print(f"[WARN] Checkpoint sample failed: {exc}")

            iceberg_specs = [
                "iceberg_last_checkpoint_duration_ms",
                "iceberg_last_commit_duration_ms",
                "iceberg_committed_data_files",
                "iceberg_committed_records",
                "iceberg_committed_bytes",
                "iceberg_seconds_since_last_commit",
            ]

            for field in iceberg_specs:
                try:
                    value = query_subtask_metric_sum(
                        args.flink_url,
                        job_id,
                        committer_vertex["id"],
                        metric_map[field],
                    )
                    if value is not None:
                        row[field] = (
                            int(value)
                            if field
                            in {
                                "iceberg_committed_data_files",
                                "iceberg_committed_records",
                                "iceberg_committed_bytes",
                            }
                            else round(value, 3)
                        )
                except Exception as exc:
                    print(f"[WARN] Iceberg metric {field} failed: {exc}")

            committed_records = as_int(row.get("iceberg_committed_records"))
            if (
                committed_records is not None
                and last_committed_records is not None
                and committed_records != last_committed_records
            ):
                event = {
                    "timestamp_utc": now_iso(),
                    "stage": stage,
                    "last_commit_duration_ms": row.get(
                        "iceberg_last_commit_duration_ms", ""
                    ),
                    "committed_data_files": row.get(
                        "iceberg_committed_data_files", ""
                    ),
                    "committed_records": row.get(
                        "iceberg_committed_records", ""
                    ),
                    "committed_bytes": row.get(
                        "iceberg_committed_bytes", ""
                    ),
                    "seconds_since_last_successful_commit": row.get(
                        "iceberg_seconds_since_last_commit", ""
                    ),
                }
                commit_writer.writerow(event)
                commit_f.flush()
                commit_events.append(event)

            if committed_records is not None:
                last_committed_records = committed_records

            if elapsed >= next_storage_sample:
                try:
                    last_inventory = ozone_inventory(table_path)
                except Exception as exc:
                    print(f"[WARN] Ozone inventory failed: {exc}")
                next_storage_sample = elapsed + args.storage_sample_interval

            row.update(last_inventory)

            metrics_writer.writerow(row)
            metrics_f.flush()
            samples.append(dict(row))

            print(
                f"[BENCH] stage={stage:10s} "
                f"target={target_rate:7.0f}/s "
                f"actual={str(row.get('kafka_input_rate_eps','')):>8} "
                f"lag={str(row.get('kafka_lag_total','')):>8} "
                f"read={str(row.get('flink_source_read_rate_eps','')):>8} "
                f"native={str(row.get('flink_source_native_rate_eps','')):>8} "
                f"pending={str(row.get('flink_pending_records','')):>8} "
                f"cp={str(row.get('latest_checkpoint_duration_ms','')):>6}ms "
                f"commit={str(row.get('iceberg_last_commit_duration_ms','')):>6}ms"
            )

            sleep_for = args.sample_interval - (time.monotonic() - loop_started)
            if sleep_for > 0:
                time.sleep(sleep_for)

    if simulator_proc is not None:
        return_code = simulator_proc.wait(timeout=30)
        if simulator_thread is not None:
            simulator_thread.join(timeout=5)
        if return_code != 0:
            raise RuntimeError(
                f"AMI simulator exited with code {return_code}. "
                f"See {simulator_log}"
            )

    summary_path = run_dir / "stage_summary.csv"
    write_stage_summary(
        samples,
        checkpoint_events,
        commit_events,
        summary_path,
    )

    try:
        end_offsets = kafka_group_offsets(args.topic, args.group)
    except Exception:
        end_offsets = {}

    manifest["completed_at_utc"] = now_iso()
    manifest["kafka_offsets_end"] = end_offsets

    final_kafka_committed_lag = (
        sum(value.get("lag", 0) for value in end_offsets.values())
        if end_offsets
        else None
    )

    final_pending = None
    if samples:
        final_pending = as_int(
            samples[-1].get("flink_pending_records")
        )

    manifest["final_kafka_committed_lag"] = final_kafka_committed_lag
    manifest["final_flink_pending_records"] = final_pending

    manifest["live_source_drained"] = (
        final_pending == 0 if final_pending is not None else None
    )
    manifest["kafka_offsets_caught_up"] = (
        final_kafka_committed_lag == 0
        if final_kafka_committed_lag is not None
        else None
    )

    if final_pending is not None and final_kafka_committed_lag is not None:
        manifest["drain_complete"] = (
            final_pending == 0
            and final_kafka_committed_lag == 0
        )
        manifest["drain_complete_basis"] = (
            "flink_pending_records_and_kafka_committed_lag"
        )
    elif final_pending is not None:
        manifest["drain_complete"] = final_pending == 0
        manifest["drain_complete_basis"] = "flink_pending_records_only"
    else:
        manifest["drain_complete"] = False
        manifest["drain_complete_basis"] = "unavailable"

    (run_dir / "benchmark_manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print("\nBenchmark collection complete.")
    print(f"Run directory : {run_dir}")
    print(f"Metrics       : {metrics_csv}")
    print(f"Checkpoints   : {checkpoints_csv}")
    print(f"Iceberg commits: {commits_csv}")
    print(f"Stage summary : {summary_path}")
    print(f"Manifest      : {run_dir / 'benchmark_manifest.json'}")
    if not args.skip_simulator:
        print(f"Simulator log : {simulator_log}")

    if not manifest.get("drain_complete", False):
        print(
            "\nWARNING: benchmark ended before Flink KafkaSource "
            "fully drained."
        )
        print(
            "  final pendingRecords = "
            f"{manifest.get('final_flink_pending_records')}"
        )
        print(
            "  final Kafka committed lag = "
            f"{manifest.get('final_kafka_committed_lag')}"
        )


if __name__ == "__main__":
    main()
