#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    from confluent_kafka import Producer
except ImportError:
    Producer = None  # Only needed for live publishing; dry-run still works.

VN_TZ = timezone(timedelta(hours=7))
STOP = False

@dataclass
class MeterState:
    meter_id: str
    usage_point_id: str
    sequence_no: int
    event_time: datetime
    energy_import_kwh_total: float
    base_power_kw: float
    profile_phase: float

@dataclass
class Counters:
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    malformed: int = 0
    partial: int = 0
    unsupported_schema: int = 0
    duplicate: int = 0
    late: int = 0
    sequence_gap: int = 0

def handle_signal(signum, frame):
    global STOP
    STOP = True

def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["baseline","ramp","burst","quality","custom"], default="baseline")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:19092"))
    p.add_argument("--topic", default=os.getenv("AMI_TOPIC","ami.meter.events"))
    p.add_argument("--meters", type=int, default=3000)
    p.add_argument("--business-interval-seconds", type=int, default=900)
    p.add_argument("--target-rate", type=float, default=None)
    p.add_argument("--duration", type=float, default=0)
    p.add_argument("--stage-seconds", type=float, default=60)
    p.add_argument("--burst-seconds", type=float, default=20)
    p.add_argument("--warmup-seconds", type=float, default=30)
    p.add_argument("--recovery-seconds", type=float, default=30)
    p.add_argument("--quality-rate", type=float, default=0.0)
    p.add_argument("--duplicate-rate", type=float, default=0.0)
    p.add_argument("--late-rate", type=float, default=0.0)
    p.add_argument("--sequence-gap-rate", type=float, default=0.0)
    p.add_argument("--event-time-start", default=None)
    p.add_argument("--seed", type=int, default=20260828)
    p.add_argument("--dry-run-events", type=int, default=0)
    return p

def validate_args(args):
    if args.meters <= 0:
        raise ValueError("--meters must be > 0")
    if args.business_interval_seconds <= 0:
        raise ValueError("--business-interval-seconds must be > 0")
    for name in ["quality_rate","duplicate_rate","late_rate","sequence_gap_rate"]:
        v = getattr(args, name)
        if not 0 <= v <= 1:
            raise ValueError(f"{name} must be in [0,1]")
    if args.mode == "custom" and (args.target_rate is None or args.target_rate <= 0):
        raise ValueError("--target-rate > 0 is required for custom mode")

def configure_mode(args):
    natural = args.meters / args.business_interval_seconds
    if args.mode == "baseline":
        return [("baseline", natural, args.duration)]
    if args.mode == "ramp":
        return [("ramp-1k",1000.0,args.stage_seconds),("ramp-2k",2000.0,args.stage_seconds),("ramp-5k",5000.0,args.stage_seconds)]
    if args.mode == "burst":
        return [("warmup-1k",1000.0,args.warmup_seconds),("burst-10k",10000.0,args.burst_seconds),("recovery-1k",1000.0,args.recovery_seconds)]
    if args.mode == "quality":
        args.quality_rate = args.quality_rate or 0.10
        args.duplicate_rate = args.duplicate_rate or 0.02
        args.late_rate = args.late_rate or 0.02
        args.sequence_gap_rate = args.sequence_gap_rate or 0.02
        return [("quality",100.0,args.duration if args.duration > 0 else 30.0)]
    return [("custom",float(args.target_rate),args.duration)]

def parse_iso_datetime(value):
    dt = datetime.fromisoformat(value.strip().replace("Z","+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=VN_TZ)
    return dt.astimezone(VN_TZ)

def expected_event_count(stages):
    if any(duration <= 0 for _,_,duration in stages):
        return None
    return max(1, math.ceil(sum(rate*duration for _,rate,duration in stages)))

def logical_timestamp_for_index(start, idx, meters, interval):
    cycle = idx // meters
    meter_idx = idx % meters
    spacing = interval / meters
    return start + timedelta(seconds=cycle*interval + meter_idx*spacing)

def plan_replay_window(args, stages):
    wall_start = datetime.now(VN_TZ).replace(microsecond=0)
    total = expected_event_count(stages)
    if args.event_time_start:
        start = parse_iso_datetime(args.event_time_start)
        end = None if total is None else logical_timestamp_for_index(start,total-1,args.meters,args.business_interval_seconds)
        return wall_start,start,end,total,"explicit"
    if total is None:
        start = wall_start - timedelta(seconds=args.business_interval_seconds)
        return wall_start,start,None,None,"rolling"
    cycles = max(1, math.ceil(total/args.meters))
    start = wall_start - timedelta(seconds=cycles*args.business_interval_seconds)
    end = logical_timestamp_for_index(start,total-1,args.meters,args.business_interval_seconds)
    return wall_start,start,end,total,"auto-backdated"

def create_meter_states(args, rng, replay_start):
    spacing = args.business_interval_seconds / args.meters
    states = []
    for i in range(args.meters):
        idx = i + 1
        states.append(MeterState(
            meter_id=f"M{idx:05d}",
            usage_point_id=f"UP{idx:05d}",
            sequence_no=0,
            event_time=replay_start + timedelta(seconds=i*spacing),
            energy_import_kwh_total=rng.uniform(800.0,24000.0),
            base_power_kw=rng.uniform(0.25,4.0),
            profile_phase=rng.uniform(-0.25,0.25),
        ))
    return states

def daily_load_factor(ts, phase):
    hour = ts.hour + ts.minute/60.0
    morning = math.exp(-((hour-7.5)**2)/5.5)
    evening = 1.35*math.exp(-((hour-19.5)**2)/7.0)
    return max(0.25,0.52+0.42*morning+0.58*evening+0.10*math.sin((hour/24.0)*2*math.pi+phase))

def make_valid_event(state,args,rng,counters):
    inc = 2 if rng.random() < args.sequence_gap_rate else 1
    if inc == 2:
        counters.sequence_gap += 1
    state.sequence_no += inc
    event_time = state.event_time
    state.event_time += timedelta(seconds=args.business_interval_seconds)
    if rng.random() < args.late_rate:
        event_time -= timedelta(seconds=args.business_interval_seconds)
        counters.late += 1
    factor = daily_load_factor(event_time,state.profile_phase)
    active = max(0.03,state.base_power_kw*factor*rng.uniform(0.92,1.08))
    pf = min(0.999,max(0.88,rng.gauss(0.975,0.012)))
    voltage = min(245.0,max(205.0,230.0-1.5*max(0.0,active/max(state.base_power_kw,0.1)-1.0)+rng.gauss(0.0,1.3)))
    current = (active*1000.0)/max(voltage*pf,1.0)
    angle = math.acos(pf)
    reactive = active*math.tan(angle)
    apparent = active/pf
    state.energy_import_kwh_total += active*(args.business_interval_seconds/3600.0)
    event = {
        "schema_version":"1.0",
        "event_id":f"ami-{uuid.uuid4()}",
        "event_type":"READING",
        "meter_id":state.meter_id,
        "usage_point_id":state.usage_point_id,
        "event_time":event_time.isoformat(timespec="milliseconds"),
        "sequence_no":state.sequence_no,
        "interval_seconds":args.business_interval_seconds,
        "phase_count":1,
        "voltage_l1_v":round(voltage,2),
        "current_l1_a":round(current,3),
        "active_power_kw":round(active,4),
        "reactive_power_kvar":round(reactive,4),
        "apparent_power_kva":round(apparent,4),
        "energy_import_kwh_total":round(state.energy_import_kwh_total,4),
        "frequency_hz":round(rng.gauss(50.0,0.015),3),
        "power_factor":round(pf,4),
        "meter_status":"NORMAL",
        "quality_code":"GOOD",
    }
    return state.meter_id, json.dumps(event,separators=(",",":")), "OK"

def apply_quality_fault(key,payload,status,args,rng,counters):
    if rng.random() >= args.quality_rate:
        return key,payload,status
    fault = rng.choice(["MALFORMED","PARTIAL","UNSUPPORTED_SCHEMA"])
    if fault == "MALFORMED":
        counters.malformed += 1
        return key,"{this-is-not-json",fault
    obj = json.loads(payload)
    if fault == "PARTIAL":
        counters.partial += 1
        obj.pop("usage_point_id",None)
    else:
        counters.unsupported_schema += 1
        obj["schema_version"] = "2.0"
        obj["firmware_version"] = "2.0-simulator"
    return key,json.dumps(obj,separators=(",",":")),fault

def delivery_callback(counters):
    def cb(err,msg):
        if err is None:
            counters.delivered += 1
        else:
            counters.failed += 1
    return cb

def print_clock_plan(args, wall_start, replay_start, replay_end, total, mode):
    natural = args.meters/args.business_interval_seconds
    print("\n"+"="*72)
    print("AMI CLOCK PLAN")
    print("="*72)
    print(f"Logical meters          : {args.meters:,}")
    print(f"Business interval       : {args.business_interval_seconds}s")
    print(f"Natural business rate   : {natural:,.2f} events/s")
    print(f"Wall-clock run start    : {wall_start.isoformat()}")
    print(f"Business replay start   : {replay_start.isoformat()}")
    print(f"Replay planning mode    : {mode}")
    if total is not None:
        print(f"Planned emitted records : {total:,}")
    print(f"Estimated replay end    : {replay_end.isoformat() if replay_end else 'unbounded'}")
    print("Latency rule            : use kafka_timestamp -> ingest_time")
    print("Do NOT use              : ingest_time - event_time_source for accelerated replay")
    print("="*72)

def run_stage(producer,states,cursor,name,rate,duration,args,rng,counters,dry=None):
    natural = args.meters/args.business_interval_seconds
    print(f"\nStage: {name} | target={rate:,.2f}/s | acceleration={rate/natural:,.1f}x")
    start=time.monotonic()
    next_batch=start
    batch_size=max(1,min(1000,int(rate/20.0)))
    batch_interval=batch_size/rate
    cb=delivery_callback(counters)
    last_valid_event = None
    while not STOP:
        now=time.monotonic()
        if duration>0 and now-start>=duration: break
        if dry is not None and dry[0]<=0: break
        if now<next_batch:
            time.sleep(min(next_batch-now,0.01)); continue
        for _ in range(batch_size):
            if dry is not None and dry[0]<=0: break
            is_duplicate = False
            if last_valid_event is not None and rng.random() < args.duplicate_rate:
                # Duplicates consume an emission slot, preserving requested EPS.
                # Re-send the exact same event_id and payload as the original.
                key, payload = last_valid_event
                status = "OK"  # Bronze parsing cannot identify duplicates by itself.
                counters.duplicate += 1
                is_duplicate = True
            else:
                state=states[cursor]; cursor=(cursor+1)%len(states)
                key,payload,status=make_valid_event(state,args,rng,counters)
                key,payload,status=apply_quality_fault(key,payload,status,args,rng,counters)
                if status == "OK":
                    last_valid_event = (key, payload)
            if producer is None:
                print(json.dumps({"key":key,"expected_parse_status":status,"duplicate":is_duplicate,"value":payload},ensure_ascii=False))
                dry[0]-=1
            else:
                while True:
                    try:
                        producer.produce(args.topic,key=key.encode(),value=payload.encode(),on_delivery=cb)
                        counters.attempted+=1
                        break
                    except BufferError:
                        producer.poll(0.01)
                producer.poll(0)
        next_batch += batch_interval
    return cursor

def main():
    args=build_parser().parse_args()
    validate_args(args)
    signal.signal(signal.SIGINT,handle_signal)
    signal.signal(signal.SIGTERM,handle_signal)
    rng=random.Random(args.seed)
    stages=configure_mode(args)
    wall_start,replay_start,replay_end,total,plan_mode=plan_replay_window(args,stages)
    print_clock_plan(args,wall_start,replay_start,replay_end,total,plan_mode)
    states=create_meter_states(args,rng,replay_start)
    counters=Counters()
    cursor=0
    if args.dry_run_events>0:
        remaining=[args.dry_run_events]
        for name,rate,duration in stages:
            cursor=run_stage(None,states,cursor,name,rate,duration,args,rng,counters,remaining)
            if remaining[0]<=0: break
        return 0
    conf={
        "bootstrap.servers":args.bootstrap_servers,
        "client.id":f"ami-simulator-{os.getpid()}",
        "acks":"all",
        "compression.type":"lz4",
        "linger.ms":5,
        "batch.num.messages":10000,
        "queue.buffering.max.messages":500000,
        "message.timeout.ms":30000,
    }
    print(f"\nConnecting to Kafka: {args.bootstrap_servers}\nTopic: {args.topic}")
    if Producer is None:
        raise RuntimeError("confluent-kafka is required for Kafka publishing; use --dry-run-events for offline tests")
    producer=Producer(conf)
    try:
        for name,rate,duration in stages:
            if STOP: break
            cursor=run_stage(producer,states,cursor,name,rate,duration,args,rng,counters)
    finally:
        print("\nFlushing Kafka producer...")
        remaining=producer.flush(30)
        if remaining:
            print(f"WARNING: {remaining} message(s) still queued")
        print(f"Final: attempted={counters.attempted:,}, delivered={counters.delivered:,}, failed={counters.failed:,}")
    return 0 if counters.failed==0 else 2

if __name__=="__main__":
    raise SystemExit(main())
