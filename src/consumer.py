import sys
import os
import json
from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report

broker = "localhost:9092"
topic = "temperatures"
srcdir = "/src"


def load_state(filename: str) -> dict:
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return {"offset": 0}


def atomic_write(filename: str, data: dict) -> None:
    tmp = filename + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.rename(tmp, filename)


def update_stats(stats: dict, station_id: str, report: Report) -> None:
    if station_id not in stats:
        stats[station_id] = {
            "count": 0,
            "sum": 0.0,
            "avg": 0.0,
            "start": report.date,
            "end": report.date,
        }

    s = stats[station_id]
    s["count"] += 1
    s["sum"] += report.degrees
    s["avg"] = s["sum"] / s["count"]

    if report.date < s["start"]:
        s["start"] = report.date
    if report.date > s["end"]:
        s["end"] = report.date


def main():
    partitions = sys.argv[1:]
    if not partitions:
        print("Usage: python3 consumer.py <partition> [partition ...]")
        sys.exit(1)

    partitions = [int(p) for p in partitions]

    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        enable_auto_commit=False,
        auto_offset_reset="earliest"
    )

    tps = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(tps)

    for p in partitions:
        filename = f"{srcdir}/partition-{p}.json"
        state = load_state(filename)
        offset = state.get("offset", 0)
        consumer.seek(TopicPartition(topic, p), offset)
        print(f"Partition {p}: seeking to offset {offset}")

    print(f"Starting consumer for partitions: {partitions}")

    while True:
        batch = consumer.poll(timeout_ms=1000)

        if not batch:
            print("No messages received in this poll")
            continue

        for tp, messages in batch.items():
            partition = tp.partition
            filename = f"{srcdir}/partition-{partition}.json"

            state = load_state(filename)
            offset = state.get("offset", 0)

            stats = {k: v for k, v in state.items() if k != "offset"}

            for msg in messages:
                report = Report()
                report.ParseFromString(msg.value)

                update_stats(stats, report.station_id, report)
                offset = msg.offset + 1

            data_to_save = {"offset": offset}
            data_to_save.update(stats)
            atomic_write(filename, data_to_save)

            print(
                f"Updated partition-{partition}.json "
                f"offset={offset}, stations={list(stats.keys())}"
            )


if __name__ == "__main__":
    main()
