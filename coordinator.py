import argparse
import csv
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from itertools import islice
from pathlib import Path

from kazoo.exceptions import BadVersionError, NodeExistsError

from common import ROOT, atomic_json, check_transaction, connect, encode, log
from pairs import count_pairs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    base = "/hw4/" + args.run
    directory = ROOT / "results" / args.run
    logfile = ROOT / "logs" / args.run / (args.name + ".jsonl")
    zk = connect()
    zk.add_listener(lambda state: log(logfile, "session", state=str(state)))
    log(logfile, "started", name=args.name, pid=os.getpid(), requested_timeout=10,
        negotiated_timeout_ms=zk._session_timeout, session_id=hex(zk.client_id[0]))
    try:
        while True:
            pointer, _ = zk.get(base + "/result")
            if json.loads(pointer):
                log(logfile, "already_done")
                return
            _, epoch_stat = zk.get(base + "/epoch")
            transaction = zk.transaction()
            transaction.create(base + "/leader", encode({"name": args.name}), ephemeral=True)
            transaction.set_data(base + "/epoch", str(epoch_stat.version + 1).encode(),
                                 version=epoch_stat.version)
            try:
                check_transaction(transaction.commit())
                epoch = epoch_stat.version + 1
                break
            except (NodeExistsError, BadVersionError):
                time.sleep(0.1)
        log(logfile, "leader", name=args.name, epoch=epoch)
        checkpoint, _ = zk.get(base + "/checkpoint")
        checkpoint = json.loads(checkpoint)
        counts = Counter()
        cursor = selected = without_skills = 0
        if checkpoint:
            state = json.loads((ROOT / checkpoint["file"]).read_text())
            cursor, selected, without_skills = state["cursor"], state["selected"], state["without_skills"]
            counts.update({tuple(skills): number for skills, number in state["counts"]})
        log(logfile, "restore", cursor=cursor, selected=selected, pairs=len(counts))
        csv.field_size_limit(10_000_000)
        with args.input.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            for _ in range(cursor):
                next(reader)
            while True:
                rows = list(islice(reader, 10_000))
                if not rows:
                    break
                part, count, missing = count_pairs(rows)
                counts.update(part)
                cursor += len(rows)
                selected += count
                without_skills += missing
                time.sleep(0.5)
                filename = directory / f"checkpoint-{epoch}-{cursor}.json"
                state = {"cursor": cursor, "selected": selected, "without_skills": without_skills,
                         "counts": [[list(skills), count] for skills, count in sorted(counts.items())]}
                atomic_json(filename, state)
                transaction = zk.transaction()
                transaction.check(base + "/epoch", version=epoch)
                transaction.set_data(base + "/checkpoint", encode({"file": str(filename.relative_to(ROOT))}))
                try:
                    check_transaction(transaction.commit())
                    log(logfile, "checkpoint", epoch=epoch, cursor=cursor, selected=selected)
                except BadVersionError:
                    log(logfile, "stale_checkpoint", epoch=epoch, cursor=cursor)
        result = directory / (args.name + "-pairs.csv")
        with result.open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["skill_1", "skill_2", "vacancies"])
            for skills, number in sorted(counts.items()):
                writer.writerow([*skills, number])
        checksum = hashlib.sha256(result.read_bytes()).hexdigest()
        baseline = hashlib.sha256((ROOT / "baseline/hw3_pairs.csv").read_bytes()).hexdigest()
        log(logfile, "computed", epoch=epoch, source_rows=cursor, selected=selected,
            without_skills=without_skills, unique_pairs=len(counts), occurrences=sum(counts.values()),
            sha256=checksum, matches_hw3=checksum == baseline)
        if checksum != baseline:
            raise RuntimeError("Результат не совпал с ДЗ 3")
        message = {"coordinator": args.name, "epoch": epoch,
                   "file": str(result.relative_to(ROOT)), "sha256": checksum}
        request = urllib.request.Request("http://127.0.0.1:8765", encode(message),
                                         {"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                reply = json.load(response)
            log(logfile, "записал результат", **message, **reply)
        except urllib.error.HTTPError as error:
            reply = json.load(error)
            if reply.get("error") != "BadVersionError":
                raise
            log(logfile, "BadVersionError", **message, **reply)
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
