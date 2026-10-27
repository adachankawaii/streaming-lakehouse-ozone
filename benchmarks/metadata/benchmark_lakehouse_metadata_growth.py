#!/usr/bin/env python3
"""
Lakehouse-context metadata/file-growth benchmark for Apache Ozone.

Purpose
-------
Keep the real write path running at a CONSTANT input rate while the existing
Iceberg Bronze table accumulates more Parquet/Iceberg metadata objects on Ozone:

    AMI simulator -> Kafka -> Flink -> Iceberg -> S3 Gateway -> Apache Ozone

The run is analytically split into equal growth stages. Unlike the existing
RAMP benchmark, the target rate does not change between stages. This makes it
possible to ask whether write-path latency/throughput degrades as file and
metadata counts grow.

No third-party Python packages are required.

Important interpretation notes
------------------------------
* The benchmark measures the Iceberg-on-Ozone lakehouse write path, not an
  isolated OM microbenchmark.
* Ozone namespace inventory is taken from the real Bronze table path.
* OM/SCM metadata directory sizes are incremental footprint proxies.
* DataNode /data/hdds footprint includes all cluster data, including Flink
  checkpoint objects, so use its DELTA as a cluster-level physical-footprint
  indicator, not as pure Bronze-table size.
* Kafka consumer-group lag is committed-offset lag. Flink pendingRecords is the
  preferred live-source backlog signal when exposed.
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import math
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
    p.add_argument("--stages", type=int, default=4)
    p.add_argument("--warmup-seconds", type=int, default=60)
    p.add_argument("--stage-seconds", type=int, default=300)
    p.add_argument("--target-rate", type=float, default=100.0)
    p.add_argument("--sample-interval", type=float, default=5.0)
    p.add_argument("--storage-sample-interval", type=float, default=30.0)
    p.add_argument("--drain-seconds", type=int, default=120)
    p.add_argument("--flink-url", default="http://localhost:8081")
    p.add_argument("--job-match", default=DEFAULT_JOB_MATCH)
    p.add_argument("--topic", default=DEFAULT_TOPIC)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--output-root", default="benchmarks/results")
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


def find_running_job(flink_url: str, name_match: str):
    overview = get_json(f"{flink_url}/jobs/overview")
    candidates = [
        j for j in overview.get("jobs", [])
        if j.get("state") in {"RUNNING", "RESTARTING"}
        and name_match in j.get("name", "")
    ]

    if not candidates:
        running = [
            f"{j.get('jid')} {j.get('state')} {j.get('name')}"
            for j in overview.get("jobs", [])
            if j.get("state") in {"RUNNING", "RESTARTING"}
        ]
        raise RuntimeError(
            f"No running Flink job matched {name_match!r}.\n"
            + "\n".join(running)
        )

    if len(candidates) > 1:
        raise RuntimeError(
            "More than one matching Bronze job is running. Cancel duplicates:\n"
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



def resolve_metric_map(
    flink_url: str,
    job_id: str,
    source_vertex_id: str,
    committer_vertex_id: str,
):
    """
    Resolve Flink/Iceberg metric IDs dynamically.

    Some operator metrics are registered lazily, so this function is safe to
    call again after workload starts.
    """
    source_metric_ids = list_metric_ids(
        flink_url, job_id, source_vertex_id
    )
    committer_metric_ids = list_metric_ids(
        flink_url, job_id, committer_vertex_id
    )

    return {
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
        "iceberg_last_commit_duration_ms": select_metric(
            committer_metric_ids, "lastCommitDurationMs"
        ),
        "iceberg_last_checkpoint_duration_ms": select_metric(
            committer_metric_ids, "lastCheckpointDurationMs"
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
            committer_metric_ids,
            "elapsedSecondsSinceLastSuccessfulCommit",
        ),
    }


def metric_map_complete(metric_map: dict):
    scalar_keys = (
        "flink_pending_records",
        "iceberg_last_commit_duration_ms",
        "iceberg_committed_data_files",
        "iceberg_committed_records",
        "iceberg_committed_bytes",
        "iceberg_seconds_since_last_commit",
    )
    return (
        all(metric_map.get(k) for k in scalar_keys)
        and bool(metric_map.get("flink_current_offset_metrics"))
    )



def query_subtask_metric_sum(
    flink_url: str,
    job_id: str,
    vertex_id: str,
    metric_id: str | None,
):
    if not metric_id:
        return None

    vertex = get_json(f"{flink_url}/jobs/{job_id}/vertices/{vertex_id}")
    parallelism = int(vertex.get("parallelism", 0))
    if parallelism <= 0:
        return None

    total = 0.0
    seen = False

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
        seen = True

    return total if seen else None


def query_subtask_metric_ids_sum(
    flink_url: str,
    job_id: str,
    vertex_id: str,
    metric_ids: list[str],
):
    if not metric_ids:
        return None

    total = 0.0
    seen = False

    for metric_id in metric_ids:
        value = query_subtask_metric_sum(
            flink_url, job_id, vertex_id, metric_id
        )
        if value is None:
            continue
        total += value
        seen = True

    return total if seen else None


def kafka_group_offsets(topic: str, group: str):
    output = run_cmd(
        [
            "docker", "compose", "exec", "-T", "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:19092",
            "--describe", "--group", group,
        ],
        timeout=30,
        check=False,
    )

    partitions = {}
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
            "Could not parse Kafka consumer-group offsets.\nRaw output:\n"
            + output
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
            "docker", "compose", "exec", "-T", "om",
            "ozone", "fs", "-ls", "-R", "ofs://om/s3v/warehouse",
        ],
        timeout=90,
    )

    roots = []
    marker = "/ami_meter_raw-"

    for line in output.splitlines():
        if marker not in line:
            continue
        path = line.split()[-1]
        idx = path.find(marker)
        if idx >= 0:
            tail = path[idx + len(marker):].split("/")[0]
            roots.append(path[: idx + len(marker)] + tail)

    roots = sorted(set(roots), key=len)
    return roots[0] if roots else None


def ozone_inventory(table_path: str | None):
    if not table_path:
        return {}

    output = run_cmd(
        [
            "docker", "compose", "exec", "-T", "om",
            "ozone", "fs", "-ls", "-R", table_path,
        ],
        timeout=90,
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

    result["ozone_iceberg_metadata_files"] = (
        result["ozone_metadata_json_files"]
        + result["ozone_manifest_files"]
        + result["ozone_snapshot_files"]
    )
    return result


def parse_mem_bytes(value: str | None):
    if not value:
        return None

    value = value.strip()
    m = re.match(r"([\d.]+)\s*([KMGTP]?i?B)", value, re.I)
    if not m:
        return None

    number = float(m.group(1))
    unit = m.group(2).upper()

    scale = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KIB": 1024,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
    }.get(unit)

    return int(number * scale) if scale else None


def ozone_resource_stats():
    output = run_cmd(
        [
            "docker", "stats", "--no-stream",
            "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
        ],
        timeout=30,
        check=False,
    )

    services = {}
    dn_cpus = []
    dn_mems = []

    for raw in output.splitlines():
        parts = raw.strip().split("|")
        if len(parts) != 3:
            continue

        name, cpu_text, mem_usage = parts
        cpu = as_float(cpu_text.strip().rstrip("%"))
        current_mem = mem_usage.split("/")[0].strip()
        mem_bytes = parse_mem_bytes(current_mem)

        service = None
        for candidate in ("om", "scm", "s3g"):
            if name.endswith(f"-{candidate}-1"):
                service = candidate
                break

        dn_match = re.search(r"-datanode([1-5])-1$", name)

        if service:
            services[f"{service}_cpu_pct"] = cpu
            services[f"{service}_memory_bytes"] = mem_bytes

        if dn_match:
            if cpu is not None:
                dn_cpus.append(cpu)
            if mem_bytes is not None:
                dn_mems.append(mem_bytes)

    services["datanodes_avg_cpu_pct"] = (
        statistics.mean(dn_cpus) if dn_cpus else None
    )
    services["datanodes_max_cpu_pct"] = max(dn_cpus) if dn_cpus else None
    services["datanodes_total_memory_bytes"] = (
        sum(dn_mems) if dn_mems else None
    )

    return services


def du_kib(service: str, path: str):
    output = run_cmd(
        [
            "docker", "compose", "exec", "-T", service,
            "bash", "-lc",
            f"du -sk -- '{path}' 2>/dev/null | awk '{{print $1}}'",
        ],
        timeout=60,
        check=False,
    ).strip()

    try:
        return int(output.splitlines()[-1]) * 1024
    except (ValueError, IndexError):
        return None


def cluster_footprint():
    """
    Capture incremental on-disk footprint proxies for this Ozone 2.2.0 layout.

    Main DB / Ratis paths observed in the running containers:
      OM  : /data/metadata/om.db,  /data/metadata/om.ratis
      SCM : /data/metadata/scm.db, /data/metadata/scm.ratis

    The total /data/metadata directory is also captured for context.
    """
    result = {
        "om_db_bytes": du_kib("om", "/data/metadata/om.db"),
        "om_ratis_bytes": du_kib("om", "/data/metadata/om.ratis"),
        "om_metadata_total_bytes": du_kib("om", "/data/metadata"),
        "scm_db_bytes": du_kib("scm", "/data/metadata/scm.db"),
        "scm_ratis_bytes": du_kib("scm", "/data/metadata/scm.ratis"),
        "scm_metadata_total_bytes": du_kib("scm", "/data/metadata"),
    }

    dn_total = 0
    dn_seen = False
    for i in range(1, 6):
        value = du_kib(f"datanode{i}", "/data/hdds")
        if value is not None:
            dn_total += value
            dn_seen = True

    result["datanode_hdds_bytes_total"] = (
        dn_total if dn_seen else None
    )
    return result


def bronze_count():
    """
    Query the real Iceberg Bronze table before/after the benchmark.
    """
    sql = """SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
USE CATALOG lakehouse;
USE bronze;
SELECT COUNT(*) AS total_rows FROM ami_meter_raw;
"""
    payload = base64.b64encode(sql.encode("utf-8")).decode("ascii")

    command = (
        f"echo '{payload}' | base64 -d > /tmp/metadata_growth_count.sql && "
        "/opt/flink/bin/sql-client.sh "
        "-i /opt/flink/conf/init.sql "
        "-f /tmp/metadata_growth_count.sql"
    )

    output = run_cmd(
        [
            "docker", "compose", "exec", "-T",
            "flink-jobmanager", "bash", "-lc", command,
        ],
        timeout=180,
    )

    matches = re.findall(
        r"^\|\s*([0-9]+)\s*\|\s*$",
        output,
        flags=re.MULTILINE,
    )
    if not matches:
        raise RuntimeError(
            "Could not parse Bronze COUNT(*) from Flink SQL output:\n"
            + output
        )

    return int(matches[-1])




class GrowthSimulatorState:
    def __init__(
        self,
        stage_seconds: int,
        stages: int,
        target_rate: float,
        warmup_seconds: int = 0,
    ):
        self.lock = threading.Lock()
        self.stage_seconds = stage_seconds
        self.stages = stages
        self.target_rate = target_rate
        self.warmup_seconds = warmup_seconds
        self.workload_started_mono = None
        self.finished = False

    def update_from_line(self, line: str):
        if re.search(r"Stage:\s*custom\s*\|", line):
            with self.lock:
                if self.workload_started_mono is None:
                    self.workload_started_mono = time.monotonic()

    def mark_finished(self):
        with self.lock:
            self.finished = True

    def snapshot(self):
        with self.lock:
            started = self.workload_started_mono
            finished = self.finished

        if started is None:
            return "startup", 0.0, None, finished

        workload_elapsed = max(0.0, time.monotonic() - started)

        if finished:
            return "drain", 0.0, workload_elapsed, True

        if workload_elapsed < self.warmup_seconds:
            return (
                "warmup",
                self.target_rate,
                workload_elapsed,
                False,
            )

        analytical_elapsed = workload_elapsed - self.warmup_seconds
        idx = min(
            int(analytical_elapsed // self.stage_seconds),
            self.stages - 1,
        )
        return (
            f"growth-{idx + 1}",
            self.target_rate,
            workload_elapsed,
            False,
        )


def stream_simulator_output(proc, log_file: Path, state: GrowthSimulatorState):
    with log_file.open("w", encoding="utf-8", newline="\n") as f:
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            f.write(line)
            f.flush()
            state.update_from_line(line)
    state.mark_finished()


def stage_number(stage: str):
    m = re.fullmatch(r"growth-(\d+)", stage)
    return int(m.group(1)) if m else None


def first_numeric(rows, key):
    for r in rows:
        v = as_float(r.get(key))
        if v is not None:
            return v
    return None


def last_numeric(rows, key):
    for r in reversed(rows):
        v = as_float(r.get(key))
        if v is not None:
            return v
    return None


def delta_metric(rows, key):
    a = first_numeric(rows, key)
    b = last_numeric(rows, key)
    if a is None or b is None:
        return None
    return b - a


def summarize_stage(
    stage: str,
    samples: list[dict],
    checkpoints: list[dict],
    commits: list[dict],
    storage_samples: list[dict],
):
    rows = [r for r in samples if r.get("stage") == stage]
    cps = [r for r in checkpoints if r.get("stage") == stage]
    cms = [r for r in commits if r.get("stage") == stage]
    sts = [r for r in storage_samples if r.get("stage") == stage]

    if not rows:
        return None

    def vals(key):
        return [
            as_float(r.get(key))
            for r in rows
            if as_float(r.get(key)) is not None
        ]

    input_rates = vals("kafka_input_rate_eps")
    flink_rates = vals("flink_source_read_rate_eps")
    lags = vals("kafka_lag_total")
    pending = vals("flink_pending_records")

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

    row = {
        "stage": stage,
        "samples": len(rows),
        "target_rate_eps": rows[-1].get("target_rate_eps", ""),
        "observed_stage_seconds": round(
            max(as_float(r.get("workload_elapsed_s"), 0) for r in rows)
            - min(as_float(r.get("workload_elapsed_s"), 0) for r in rows),
            2,
        ),
        "avg_kafka_input_rate_eps": round(statistics.mean(input_rates), 2)
        if input_rates else "",
        "p95_kafka_input_rate_eps": round(percentile(input_rates, 0.95), 2)
        if input_rates else "",
        "avg_flink_source_read_rate_eps": round(statistics.mean(flink_rates), 2)
        if flink_rates else "",
        "max_kafka_lag": int(max(lags)) if lags else "",
        "end_kafka_lag": int(lags[-1]) if lags else "",
        "max_flink_pending_records": int(max(pending)) if pending else "",
        "end_flink_pending_records": int(pending[-1]) if pending else "",
        "checkpoint_count": len(cps),
        "checkpoint_duration_p50_ms": round(percentile(cp_durations, 0.50), 2)
        if cp_durations else "",
        "checkpoint_duration_p95_ms": round(percentile(cp_durations, 0.95), 2)
        if cp_durations else "",
        "iceberg_commit_events": len(cms),
        "iceberg_commit_duration_p50_ms": round(percentile(commit_durations, 0.50), 2)
        if commit_durations else "",
        "iceberg_commit_duration_p95_ms": round(percentile(commit_durations, 0.95), 2)
        if commit_durations else "",
        "kafka_log_end_delta": int(delta_metric(rows, "kafka_log_end_total"))
        if delta_metric(rows, "kafka_log_end_total") is not None else "",
        "iceberg_committed_records_delta": int(delta_metric(rows, "iceberg_committed_records"))
        if delta_metric(rows, "iceberg_committed_records") is not None else "",
        "iceberg_committed_data_files_delta": int(delta_metric(rows, "iceberg_committed_data_files"))
        if delta_metric(rows, "iceberg_committed_data_files") is not None else "",
    }

    for key in (
        "ozone_data_files",
        "ozone_iceberg_metadata_files",
        "ozone_total_files",
        "ozone_logical_bytes",
        "om_db_bytes",
        "om_ratis_bytes",
        "om_metadata_total_bytes",
        "scm_db_bytes",
        "scm_ratis_bytes",
        "scm_metadata_total_bytes",
        "datanode_hdds_bytes_total",
    ):
        start = first_numeric(sts, key)
        end = last_numeric(sts, key)
        row[f"{key}_start"] = int(start) if start is not None else ""
        row[f"{key}_end"] = int(end) if end is not None else ""
        row[f"{key}_delta"] = int(end - start) if start is not None and end is not None else ""

    for service in ("om", "scm", "s3g"):
        cpu_values = vals(f"{service}_cpu_pct")
        mem_values = vals(f"{service}_memory_bytes")
        row[f"{service}_avg_cpu_pct"] = (
            round(statistics.mean(cpu_values), 2) if cpu_values else ""
        )
        row[f"{service}_max_cpu_pct"] = (
            round(max(cpu_values), 2) if cpu_values else ""
        )
        row[f"{service}_max_memory_bytes"] = (
            int(max(mem_values)) if mem_values else ""
        )

    dn_avg = vals("datanodes_avg_cpu_pct")
    dn_max = vals("datanodes_max_cpu_pct")
    row["datanodes_avg_cpu_pct"] = (
        round(statistics.mean(dn_avg), 2) if dn_avg else ""
    )
    row["datanodes_peak_cpu_pct"] = (
        round(max(dn_max), 2) if dn_max else ""
    )

    return row


def main():
    args = parse_args()

    if args.stages < 2:
        raise ValueError("--stages must be >= 2")
    if args.warmup_seconds < 0:
        raise ValueError("--warmup-seconds must be >= 0")
    if args.stage_seconds <= 0:
        raise ValueError("--stage-seconds must be > 0")
    if args.target_rate <= 0:
        raise ValueError("--target-rate must be > 0")
    if args.sample_interval <= 0:
        raise ValueError("--sample-interval must be > 0")
    if args.storage_sample_interval <= 0:
        raise ValueError("--storage-sample-interval must be > 0")

    repo_root = Path.cwd()
    run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = (
        repo_root
        / args.output_root
        / f"{run_stamp}-lakehouse-metadata-growth"
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    print("Finding running Bronze Flink job...")
    job = find_running_job(args.flink_url, args.job_match)
    job_id = job["jid"]
    print(f"Job ID: {job_id}")
    print(f"Job   : {job['name']}")

    source_vertex, committer_vertex = find_vertices(args.flink_url, job_id)

    metric_map = resolve_metric_map(
        args.flink_url,
        job_id,
        source_vertex["id"],
        committer_vertex["id"],
    )

    print("Resolved metrics:")
    for k, v in metric_map.items():
        if isinstance(v, list):
            print(f"  {k}: {len(v)} metric(s)")
        else:
            print(f"  {k}: {v or '<not exposed>'}")

    table_path = discover_ozone_table_path()
    if not table_path:
        raise RuntimeError(
            "Could not discover ami_meter_raw table path under "
            "ofs://om/s3v/warehouse"
        )
    print(f"Ozone table path: {table_path}")

    start_offsets = kafka_group_offsets(args.topic, args.group)
    start_lag = sum(x["lag"] for x in start_offsets.values())
    if start_lag != 0:
        raise RuntimeError(
            f"Benchmark requires Kafka committed lag=0, found {start_lag}."
        )

    initial_pending = query_subtask_metric_sum(
        args.flink_url,
        job_id,
        source_vertex["id"],
        metric_map["flink_pending_records"],
    )
    if initial_pending is not None and initial_pending > 0:
        raise RuntimeError(
            "Benchmark requires clean Flink source start, but "
            f"pendingRecords={int(initial_pending)}."
        )

    print("Reading Bronze baseline COUNT(*)...")
    baseline_bronze_rows = bronze_count()
    print(f"Bronze baseline rows: {baseline_bronze_rows}")

    baseline_inventory = ozone_inventory(table_path)
    baseline_footprint = cluster_footprint()

    manifest = {
        "run_id": run_dir.name,
        "collector_version": "v5-warmup-separated",
        "created_at_utc": now_iso(),
        "experiment": (
            "Constant-rate lakehouse metadata/file growth on Apache Ozone"
        ),
        "architecture_under_test": (
            "AMI simulator -> Kafka -> Flink -> Iceberg/Lakekeeper "
            "-> Ozone S3 Gateway -> OM/SCM/DataNodes"
        ),
        "job_id": job_id,
        "job_name": job["name"],
        "topic": args.topic,
        "consumer_group": args.group,
        "stages": args.stages,
        "warmup_seconds": args.warmup_seconds,
        "stage_seconds": args.stage_seconds,
        "target_rate_eps": args.target_rate,
        "total_workload_seconds": (
            args.warmup_seconds + args.stages * args.stage_seconds
        ),
        "analytical_workload_seconds": args.stages * args.stage_seconds,
        "sample_interval_seconds": args.sample_interval,
        "storage_sample_interval_seconds": args.storage_sample_interval,
        "drain_seconds": args.drain_seconds,
        "ozone_table_path": table_path,
        "metric_map": metric_map,
        "kafka_offsets_start": start_offsets,
        "baseline_bronze_rows": baseline_bronze_rows,
        "baseline_inventory": baseline_inventory,
        "baseline_cluster_footprint": baseline_footprint,
        "notes": {
            "primary_question": (
                "At constant input rate, does write-path performance degrade "
                "as Iceberg/Ozone file and metadata counts grow?"
            ),
            "warmup": (
                "A constant-rate warm-up interval is excluded from growth-1 "
                "through growth-N stage summaries so startup/cold-cache effects "
                "do not bias the first analytical stage."
            ),
            "kafka_lag": (
                "Consumer-group committed-offset lag. It is not the "
                "authoritative live Flink backlog."
            ),
            "flink_pending_records": (
                "Primary live-source backlog signal when exposed."
            ),
            "ozone_inventory": (
                "Real Bronze table namespace from ozone fs -ls -R."
            ),
            "om_scm_metadata_bytes": (
                "OM DB=/data/metadata/om.db, OM Ratis=/data/metadata/om.ratis; "
                "SCM DB=/data/metadata/scm.db, SCM Ratis=/data/metadata/scm.ratis. "
                "These are incremental filesystem footprint proxies; LSM/WAL/"
                "compaction means byte growth need not be perfectly linear."
            ),
            "datanode_hdds_bytes_total": (
                "Total /data/hdds footprint across five DataNodes. Includes "
                "all cluster data, including checkpoint objects."
            ),
            "interpretation_limit": (
                "Latency changes can come from Iceberg metadata/catalog, S3G "
                "and Ozone together. Use OM/SCM/S3G resource metrics and "
                "namespace growth to reason about where pressure appears."
            ),
        },
    }

    manifest_path = run_dir / "benchmark_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    metrics_csv = run_dir / "metrics.csv"
    checkpoints_csv = run_dir / "checkpoints.csv"
    commits_csv = run_dir / "iceberg_commits.csv"
    storage_csv = run_dir / "storage_growth.csv"
    resource_csv = run_dir / "ozone_resources.csv"
    simulator_log = run_dir / "simulator.log"
    summary_csv = run_dir / "stage_summary.csv"

    metric_fields = [
        "timestamp_utc",
        "elapsed_s",
        "workload_elapsed_s",
        "stage",
        "target_rate_eps",
        "flink_job_state",
        "kafka_log_end_total",
        "kafka_committed_offset_total",
        "kafka_lag_total",
        "kafka_input_rate_eps",
        "kafka_committed_consume_rate_eps",
        "flink_pending_records",
        "flink_source_position_total",
        "flink_source_read_rate_eps",
        "checkpoint_total",
        "checkpoint_completed",
        "checkpoint_failed",
        "checkpoint_in_progress",
        "latest_checkpoint_id",
        "latest_checkpoint_duration_ms",
        "latest_checkpoint_size_bytes",
        "latest_checkpoint_path",
        "latest_checkpoint_trigger_ms",
        "iceberg_last_commit_duration_ms",
        "iceberg_last_checkpoint_duration_ms",
        "iceberg_committed_data_files",
        "iceberg_committed_records",
        "iceberg_committed_bytes",
        "iceberg_seconds_since_last_commit",
        "ozone_data_files",
        "ozone_metadata_json_files",
        "ozone_manifest_files",
        "ozone_snapshot_files",
        "ozone_iceberg_metadata_files",
        "ozone_total_files",
        "ozone_logical_bytes",
        "om_db_bytes",
        "om_ratis_bytes",
        "om_metadata_total_bytes",
        "scm_db_bytes",
        "scm_ratis_bytes",
        "scm_metadata_total_bytes",
        "datanode_hdds_bytes_total",
        "om_cpu_pct",
        "om_memory_bytes",
        "scm_cpu_pct",
        "scm_memory_bytes",
        "s3g_cpu_pct",
        "s3g_memory_bytes",
        "datanodes_avg_cpu_pct",
        "datanodes_max_cpu_pct",
        "datanodes_total_memory_bytes",
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

    storage_fields = [
        "timestamp_utc",
        "elapsed_s",
        "workload_elapsed_s",
        "stage",
        "ozone_data_files",
        "ozone_metadata_json_files",
        "ozone_manifest_files",
        "ozone_snapshot_files",
        "ozone_iceberg_metadata_files",
        "ozone_total_files",
        "ozone_logical_bytes",
        "om_db_bytes",
        "om_ratis_bytes",
        "om_metadata_total_bytes",
        "scm_db_bytes",
        "scm_ratis_bytes",
        "scm_metadata_total_bytes",
        "datanode_hdds_bytes_total",
    ]

    resource_fields = [
        "timestamp_utc",
        "elapsed_s",
        "workload_elapsed_s",
        "stage",
        "om_cpu_pct",
        "om_memory_bytes",
        "scm_cpu_pct",
        "scm_memory_bytes",
        "s3g_cpu_pct",
        "s3g_memory_bytes",
        "datanodes_avg_cpu_pct",
        "datanodes_max_cpu_pct",
        "datanodes_total_memory_bytes",
    ]

    footprint_fields = {
        "om_db_bytes",
        "om_ratis_bytes",
        "om_metadata_total_bytes",
        "scm_db_bytes",
        "scm_ratis_bytes",
        "scm_metadata_total_bytes",
        "datanode_hdds_bytes_total",
    }
    missing_storage_fields = footprint_fields.difference(storage_fields)
    missing_metric_fields = footprint_fields.difference(metric_fields)
    if missing_storage_fields or missing_metric_fields:
        raise RuntimeError(
            "Collector schema mismatch before benchmark start. "
            f"Missing from storage_fields={sorted(missing_storage_fields)}; "
            f"missing from metric_fields={sorted(missing_metric_fields)}"
        )

    simulator_state = GrowthSimulatorState(
        args.stage_seconds,
        args.stages,
        args.target_rate,
        warmup_seconds=args.warmup_seconds,
    )

    total_duration = (
        args.warmup_seconds + args.stages * args.stage_seconds
    )
    simulator_cmd = [
        "docker", "compose", "--profile", "simulator",
        "run", "--rm",
        "-e", "PYTHONUNBUFFERED=1",
        "ami-simulator",
        "--mode", "custom",
        "--target-rate", str(args.target_rate),
        "--duration", str(total_duration),
    ]

    print("\nStarting constant-rate AMI simulator...")
    print(
        f"Target={args.target_rate:.2f}/s, "
        f"duration={total_duration}s, "
        f"warmup={args.warmup_seconds}s, "
        f"analytical stages={args.stages} x {args.stage_seconds}s"
    )

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

    samples = []
    checkpoint_events = []
    commit_events = []
    storage_samples = []

    last_log_end = None
    last_committed = None
    last_rate_time = None

    last_source_position = None
    last_source_position_time = None

    last_checkpoint_id = None
    last_committed_records = None

    last_inventory = dict(baseline_inventory)
    last_footprint = dict(baseline_footprint)
    last_resources = ozone_resource_stats()

    next_storage_sample = 0.0
    next_metric_refresh = 0.0
    previous_stage = None
    start_mono = time.monotonic()
    drain_start = None
    safety_timeout = total_duration + args.drain_seconds + 180

    with (
        metrics_csv.open("w", encoding="utf-8", newline="") as metrics_f,
        checkpoints_csv.open("w", encoding="utf-8", newline="") as cp_f,
        commits_csv.open("w", encoding="utf-8", newline="") as commit_f,
        storage_csv.open("w", encoding="utf-8", newline="") as storage_f,
        resource_csv.open("w", encoding="utf-8", newline="") as resource_f,
    ):
        metrics_writer = csv.DictWriter(metrics_f, fieldnames=metric_fields)
        cp_writer = csv.DictWriter(cp_f, fieldnames=checkpoint_fields)
        commit_writer = csv.DictWriter(commit_f, fieldnames=commit_fields)
        storage_writer = csv.DictWriter(storage_f, fieldnames=storage_fields)
        resource_writer = csv.DictWriter(resource_f, fieldnames=resource_fields)

        metrics_writer.writeheader()
        cp_writer.writeheader()
        commit_writer.writeheader()
        storage_writer.writeheader()
        resource_writer.writeheader()

        while True:
            loop_started = time.monotonic()
            elapsed = loop_started - start_mono
            stage, target_rate, workload_elapsed, sim_finished = (
                simulator_state.snapshot()
            )

            if sim_finished and drain_start is None:
                drain_start = loop_started

            if (
                drain_start is not None
                and loop_started - drain_start >= args.drain_seconds
            ):
                break

            if elapsed >= safety_timeout:
                print("[WARN] Collector safety timeout reached.")
                break

            if (
                elapsed >= next_metric_refresh
                or not metric_map_complete(metric_map)
            ):
                try:
                    refreshed = resolve_metric_map(
                        args.flink_url,
                        job_id,
                        source_vertex["id"],
                        committer_vertex["id"],
                    )
                    if refreshed != metric_map:
                        metric_map = refreshed
                        manifest["metric_map"] = metric_map
                        manifest_path.write_text(
                            json.dumps(manifest, indent=2),
                            encoding="utf-8",
                        )
                        print("[INFO] Refreshed Flink/Iceberg metric IDs:")
                        for k, v in metric_map.items():
                            if isinstance(v, list):
                                print(f"  {k}: {len(v)} metric(s)")
                            else:
                                print(f"  {k}: {v or '<not exposed>'}")
                except Exception as exc:
                    print(f"[WARN] Metric-ID refresh failed: {exc}")

                next_metric_refresh = elapsed + 15.0

            row = {field: "" for field in metric_fields}
            row["timestamp_utc"] = now_iso()
            row["elapsed_s"] = round(elapsed, 3)
            row["workload_elapsed_s"] = (
                round(workload_elapsed, 3)
                if workload_elapsed is not None else ""
            )
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
                print(f"[WARN] Kafka sample failed: {exc}")

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
                print(f"[WARN] pendingRecords failed: {exc}")

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
                print(f"[WARN] Flink currentOffset failed: {exc}")

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

            for field in (
                "iceberg_last_commit_duration_ms",
                "iceberg_last_checkpoint_duration_ms",
                "iceberg_committed_data_files",
                "iceberg_committed_records",
                "iceberg_committed_bytes",
                "iceberg_seconds_since_last_commit",
            ):
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
                            if field in {
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

            # Sample Ozone server resource usage every main loop.
            try:
                last_resources = ozone_resource_stats()
            except Exception as exc:
                print(f"[WARN] docker stats failed: {exc}")

            row.update(last_resources)

            resource_row = {
                k: row.get(k, "")
                for k in resource_fields
            }
            resource_writer.writerow(resource_row)
            resource_f.flush()

            # Force storage/metadata inventory at stage transitions and at the
            # configured lower frequency to reduce observer overhead.
            stage_changed = stage != previous_stage
            should_storage_sample = (
                elapsed >= next_storage_sample or stage_changed
            )

            if should_storage_sample:
                try:
                    last_inventory = ozone_inventory(table_path)
                except Exception as exc:
                    print(f"[WARN] Ozone table inventory failed: {exc}")

                try:
                    last_footprint = cluster_footprint()
                except Exception as exc:
                    print(f"[WARN] Ozone cluster footprint failed: {exc}")

                storage_row = {
                    "timestamp_utc": now_iso(),
                    "elapsed_s": round(elapsed, 3),
                    "workload_elapsed_s": (
                        round(workload_elapsed, 3)
                        if workload_elapsed is not None else ""
                    ),
                    "stage": stage,
                }
                storage_row.update(last_inventory)
                storage_row.update(last_footprint)

                storage_writer.writerow(storage_row)
                storage_f.flush()
                storage_samples.append(dict(storage_row))

                next_storage_sample = elapsed + args.storage_sample_interval

            row.update(last_inventory)
            row.update(last_footprint)

            metrics_writer.writerow(row)
            metrics_f.flush()
            samples.append(dict(row))

            print(
                f"[GROWTH] stage={stage:9s} "
                f"target={target_rate:6.0f}/s "
                f"actual={str(row.get('kafka_input_rate_eps','')):>7} "
                f"lag={str(row.get('kafka_lag_total','')):>6} "
                f"pending={str(row.get('flink_pending_records','')):>6} "
                f"cp={str(row.get('latest_checkpoint_duration_ms','')):>5}ms "
                f"commit={str(row.get('iceberg_last_commit_duration_ms','')):>5}ms "
                f"dataFiles={str(row.get('ozone_data_files','')):>4} "
                f"metaFiles={str(row.get('ozone_iceberg_metadata_files','')):>4} "
                f"OMcpu={str(row.get('om_cpu_pct','')):>6}% "
                f"S3Gcpu={str(row.get('s3g_cpu_pct','')):>6}%"
            )

            previous_stage = stage

            sleep_for = args.sample_interval - (
                time.monotonic() - loop_started
            )
            if sleep_for > 0:
                time.sleep(sleep_for)

    return_code = simulator_proc.wait(timeout=30)
    simulator_thread.join(timeout=5)
    if return_code != 0:
        raise RuntimeError(
            f"AMI simulator exited with code {return_code}. "
            f"See {simulator_log}"
        )

    # Final, post-drain snapshot.
    final_inventory = ozone_inventory(table_path)
    final_footprint = cluster_footprint()
    final_offsets = kafka_group_offsets(args.topic, args.group)

    print("Reading final Bronze COUNT(*)...")
    final_bronze_rows = bronze_count()
    print(f"Bronze final rows: {final_bronze_rows}")
    final_lag = sum(x["lag"] for x in final_offsets.values())

    try:
        final_pending = query_subtask_metric_sum(
            args.flink_url,
            job_id,
            source_vertex["id"],
            metric_map["flink_pending_records"],
        )
        final_pending = int(final_pending) if final_pending is not None else None
    except Exception:
        final_pending = None

    stage_summaries = []
    for i in range(1, args.stages + 1):
        stage = f"growth-{i}"
        summary = summarize_stage(
            stage,
            samples,
            checkpoint_events,
            commit_events,
            storage_samples,
        )
        if summary:
            summary["analytical_cumulative_target_records"] = int(
                args.target_rate * args.stage_seconds * i
            )
            stage_summaries.append(summary)

    if stage_summaries:
        fields = list(stage_summaries[0].keys())
        with summary_csv.open(
            "w", encoding="utf-8", newline=""
        ) as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(stage_summaries)

    start_log_end_total = sum(
        x["log_end"] for x in start_offsets.values()
    )
    final_log_end_total = sum(
        x["log_end"] for x in final_offsets.values()
    )
    kafka_log_end_delta = final_log_end_total - start_log_end_total
    bronze_row_delta = final_bronze_rows - baseline_bronze_rows

    manifest["completed_at_utc"] = now_iso()
    manifest["final_bronze_rows"] = final_bronze_rows
    manifest["bronze_row_delta"] = bronze_row_delta
    manifest["kafka_log_end_delta"] = kafka_log_end_delta
    manifest["bronze_delta_matches_kafka_log_end_delta"] = (
        bronze_row_delta == kafka_log_end_delta
    )
    manifest["kafka_offsets_end"] = final_offsets
    manifest["final_kafka_committed_lag"] = final_lag
    manifest["final_flink_pending_records"] = final_pending
    manifest["final_inventory"] = final_inventory
    manifest["final_cluster_footprint"] = final_footprint
    manifest["drain_complete"] = (
        final_lag == 0
        and (final_pending in {0, None})
    )
    manifest["baseline_to_final_growth"] = {
        key: (
            final_inventory.get(key) - baseline_inventory.get(key)
            if final_inventory.get(key) is not None
            and baseline_inventory.get(key) is not None
            else None
        )
        for key in set(baseline_inventory) | set(final_inventory)
    }
    manifest["baseline_to_final_cluster_growth"] = {
        key: (
            final_footprint.get(key) - baseline_footprint.get(key)
            if final_footprint.get(key) is not None
            and baseline_footprint.get(key) is not None
            else None
        )
        for key in set(baseline_footprint) | set(final_footprint)
    }

    manifest_path.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    print("\n============================================================")
    print("LAKEHOUSE METADATA/FILE GROWTH BENCHMARK COMPLETE")
    print("============================================================")
    print(f"Run directory       : {run_dir}")
    print(f"Stage summary       : {summary_csv}")
    print(f"Metrics             : {metrics_csv}")
    print(f"Checkpoints         : {checkpoints_csv}")
    print(f"Iceberg commits     : {commits_csv}")
    print(f"Storage growth      : {storage_csv}")
    print(f"Ozone resources     : {resource_csv}")
    print(f"Manifest            : {manifest_path}")
    print(f"Simulator log       : {simulator_log}")
    print(f"Bronze baseline rows: {baseline_bronze_rows}")
    print(f"Bronze final rows   : {final_bronze_rows}")
    print(f"Bronze row delta    : {bronze_row_delta}")
    print(f"Kafka log-end delta : {kafka_log_end_delta}")
    print(
        "Bronze/Kafka match   : "
        f"{manifest['bronze_delta_matches_kafka_log_end_delta']}"
    )
    print(f"Final Kafka lag     : {final_lag}")
    print(f"Final pendingRecords: {final_pending}")
    print(f"Drain complete      : {manifest['drain_complete']}")


if __name__ == "__main__":
    main()
