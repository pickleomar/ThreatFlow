"""
Synthetic security-event producer -> Kafka topic 'security-events'.

Generates realistic authentication/network events at ~20 events/sec and
DELIBERATELY injects anomalies so the Spark detector has something to find:
  - bursts of failed logins from a single IP (brute-force simulation)
  - traffic from blacklisted IPs

Run: python producer/producer.py
"""
import json
import os
import time
import random

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Inside Docker -> "kafka:9094"; on host -> "localhost:9092"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "security-events"

# A small pool of "normal" internal IPs
NORMAL_IPS = [f"192.168.0.{i}" for i in range(1, 51)]
# Known-bad IPs (any event from these is suspicious)
BLACKLIST = ["10.66.66.6", "45.137.21.9", "185.220.101.1"]
ACTIONS = ["login_ok", "login_fail", "connect", "port_scan"]
PORTS = [22, 80, 443, 3389, 8080, 1337, 4444]


def make_normal_event():
    return {
        "ip": random.choice(NORMAL_IPS),
        "action": random.choices(ACTIONS, weights=[60, 20, 18, 2])[0],
        "port": random.choice(PORTS),
        "ts": time.time(),
    }


def make_bruteforce_burst(target_ip, n=12):
    """A single IP hammering failed logins -> should trigger an alert."""
    events = []
    for _ in range(n):
        events.append({
            "ip": target_ip,
            "action": "login_fail",
            "port": 22,
            "ts": time.time(),
        })
    return events


def make_blacklist_event():
    return {
        "ip": random.choice(BLACKLIST),
        "action": random.choice(["connect", "login_fail", "port_scan"]),
        "port": random.choice(PORTS),
        "ts": time.time(),
    }


def make_producer():
    """Retry until Kafka is reachable (it may still be starting up)."""
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/30), retrying in 3s...")
            time.sleep(3)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP} after 30 attempts")


def main():
    producer = make_producer()
    print(f"[producer] sending to topic '{TOPIC}' at {KAFKA_BOOTSTRAP} ... Ctrl+C to stop")

    tick = 0
    try:
        while True:
            # Normal background traffic
            producer.send(TOPIC, make_normal_event())

            # Every ~150 ticks, inject a brute-force burst
            if tick % 150 == 0 and tick > 0:
                target = random.choice(NORMAL_IPS)
                print(f"[producer] >>> injecting brute-force burst on {target}")
                for ev in make_bruteforce_burst(target):
                    producer.send(TOPIC, ev)

            # Every ~80 ticks, inject a blacklisted-IP event
            if tick % 80 == 0 and tick > 0:
                ev = make_blacklist_event()
                print(f"[producer] >>> injecting blacklist event from {ev['ip']}")
                producer.send(TOPIC, ev)

            tick += 1
            time.sleep(0.05)  # ~20 events/sec
    except KeyboardInterrupt:
        print("\n[producer] stopping...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
