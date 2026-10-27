import json
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:19092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "meter-telemetry"
)

NUM_METERS = int(
    os.getenv("NUM_METERS", "100")
)

NUM_TRANSFORMERS = int(
    os.getenv("NUM_TRANSFORMERS", "10")
)

EVENTS_PER_SECOND = float(
    os.getenv("EVENTS_PER_SECOND", "20")
)


producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,

        # Producer identity
        "client.id": "smart-meter-simulator",

        # Reliability
        "acks": "all",
        "enable.idempotence": True,

        # Small batching without adding large latency
        "linger.ms": 10,

        # Fail relatively quickly in development
        "delivery.timeout.ms": 30000,
    }
)


running = True


def stop_handler(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


# ---------------------------------------------------------
# Deterministic meter topology
# ---------------------------------------------------------

meters = []

for i in range(1, NUM_METERS + 1):

    meter_id = f"METER_{i:05d}"

    transformer_index = ((i - 1) % NUM_TRANSFORMERS) + 1
    transformer_id = f"TR_{transformer_index:03d}"

    meters.append(
        {
            "meter_id": meter_id,
            "transformer_id": transformer_id,

            # Give each meter slightly different operating behavior
            "base_voltage": random.uniform(226.0, 233.0),
            "base_current": random.uniform(3.0, 20.0),

            "sequence_no": 0,
        }
    )


def utc_now():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def create_event(meter):

    meter["sequence_no"] += 1

    voltage = random.gauss(
        meter["base_voltage"],
        1.2
    )

    current = max(
        0.1,
        random.gauss(
            meter["base_current"],
            1.0
        )
    )

    power_factor = min(
        1.0,
        max(
            0.75,
            random.gauss(0.95, 0.02)
        )
    )

    frequency = random.gauss(
        50.0,
        0.03
    )

    # Approximate single-phase active power
    power_kw = (
        voltage
        * current
        * power_factor
        / 1000.0
    )

    return {
        "schema_version": 1,

        "event_id": str(uuid.uuid4()),

        "meter_id": meter["meter_id"],
        "transformer_id": meter["transformer_id"],

        "event_time": utc_now(),

        "sequence_no": meter["sequence_no"],

        "voltage": round(voltage, 2),
        "current": round(current, 2),
        "power_kw": round(power_kw, 3),
        "frequency_hz": round(frequency, 3),
        "power_factor": round(power_factor, 3),

        "status": "OK",
    }


delivery_errors = 0


def delivery_report(err, msg):
    global delivery_errors

    if err is not None:
        delivery_errors += 1

        print(
            f"[DELIVERY ERROR] "
            f"topic={msg.topic()} "
            f"error={err}"
        )


print("========================================")
print("Smart Meter Simulator")
print("========================================")
print(f"Kafka:        {BOOTSTRAP_SERVERS}")
print(f"Topic:        {TOPIC}")
print(f"Meters:       {NUM_METERS}")
print(f"Transformers: {NUM_TRANSFORMERS}")
print(f"Target rate:  {EVENTS_PER_SECOND} events/s")
print("========================================")


event_count = 0
start_time = time.monotonic()

meter_index = 0

interval = 1.0 / EVENTS_PER_SECOND


try:

    while running:

        loop_start = time.monotonic()

        meter = meters[meter_index]

        meter_index = (
            meter_index + 1
        ) % len(meters)

        event = create_event(meter)

        key = event["meter_id"]

        producer.produce(
            topic=TOPIC,

            # Kafka partition key
            key=key.encode("utf-8"),

            value=json.dumps(
                event,
                separators=(",", ":")
            ).encode("utf-8"),

            on_delivery=delivery_report
        )

        # Executes delivery callbacks
        producer.poll(0)

        event_count += 1

        if event_count % 100 == 0:

            elapsed = time.monotonic() - start_time

            actual_rate = (
                event_count / elapsed
                if elapsed > 0
                else 0
            )

            print(
                f"[STATS] "
                f"events={event_count} "
                f"rate={actual_rate:.2f}/s "
                f"delivery_errors={delivery_errors}"
            )

        elapsed_loop = time.monotonic() - loop_start

        sleep_time = interval - elapsed_loop

        if sleep_time > 0:
            time.sleep(sleep_time)


finally:

    print("Stopping producer...")

    remaining = producer.flush(10)

    print(
        f"Producer stopped. "
        f"events={event_count}, "
        f"undelivered={remaining}, "
        f"delivery_errors={delivery_errors}"
    )