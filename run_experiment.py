import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from common import ROOT, atomic_json, connect, encode, log


def events(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def wait_for(test, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = test()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError("Не дождались события")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--fencing", type=int, choices=[0, 1], default=int(os.getenv("FENCING", "0")))
    parser.add_argument("--input", type=Path, default=Path.home() / "Downloads/it_vacancies_2024.csv")
    args = parser.parse_args()
    directory = ROOT / "logs" / args.run
    if directory.exists():
        raise RuntimeError("Для нового опыта нужен новый --run; старые логи сохраняются")
    directory.mkdir(parents=True)
    zk = connect()
    base = "/hw4/" + args.run
    if zk.exists(base):
        raise RuntimeError("Такой опыт уже есть в ZooKeeper")
    zk.ensure_path(base)
    for name, value in [("epoch", b"0"), ("checkpoint", b"{}"), ("result", b"{}"), ("writes", b"")]:
        zk.create(base + "/" + name, value)
    processes = []
    handles = []
    paused = None
    experiment_log = directory / "experiment.jsonl"

    def start(name, command):
        handle = (directory / (name + ".stdout.log")).open("w")
        handles.append(handle)
        process = subprocess.Popen([sys.executable, "-u", *command], cwd=ROOT,
                                   stdout=handle, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    try:
        start("receiver", ["receiver.py", "--run", args.run, "--fencing", str(args.fencing)])
        wait_for(lambda: events(directory / "receiver.jsonl"))
        a = start("A", ["coordinator.py", "--run", args.run, "--name", "A", "--input", str(args.input)])
        wait_for(lambda: any(e["event"] == "leader" for e in events(directory / "A.jsonl")))
        b = start("B", ["coordinator.py", "--run", args.run, "--name", "B", "--input", str(args.input)])
        wait_for(lambda: any(e["event"] == "started" for e in events(directory / "B.jsonl")))
        wait_for(lambda: any(e["event"] == "checkpoint" and e["cursor"] >= 50_000
                            for e in events(directory / "A.jsonl")))
        os.kill(a.pid, signal.SIGSTOP)
        paused = a
        stopped_at = time.monotonic()
        checkpoint = json.loads(zk.get(base + "/checkpoint")[0])
        state = json.loads((ROOT / checkpoint["file"]).read_text())
        log(experiment_log, "SIGSTOP", pid=a.pid, checkpoint_cursor=state["cursor"],
            signal_monotonic=stopped_at)
        leadership = wait_for(lambda: next((e for e in events(directory / "B.jsonl")
                                            if e["event"] == "leader"), None), timeout=30)
        log(experiment_log, "failover", seconds=leadership["monotonic"] - stopped_at,
            new_leader="B", epoch=leadership["epoch"])
        wait_for(lambda: any(e["event"] == "записал результат" for e in events(directory / "B.jsonl")), timeout=30)
        assert b.wait(timeout=5) == 0
        time.sleep(max(0, 40 - (time.monotonic() - stopped_at)))
        os.kill(a.pid, signal.SIGCONT)
        paused = None
        log(experiment_log, "SIGCONT", pid=a.pid, pause_seconds=time.monotonic() - stopped_at)
        assert a.wait(timeout=60) == 0
        pointer, stat = zk.get(base + "/result")
        writes = [json.loads(zk.get(base + "/writes/" + key)[0])
                  for key in zk.get_children(base + "/writes")]
        all_events = {name: events(directory / (name + ".jsonl")) for name in ("A", "B")}
        restored = next(e for e in all_events["B"] if e["event"] == "restore")
        computed = [next(e for e in all_events[name] if e["event"] == "computed") for name in ("A", "B")]
        assert all(e["matches_hw3"] for e in computed)
        assert restored["cursor"] == state["cursor"] > 0
        assert len(writes) == (1 if args.fencing else 2)
        assert stat.version == len(writes)
        assert json.loads(pointer)["coordinator"] == ("B" if args.fencing else "A")
        assert sum(e["event"] == "BadVersionError" for e in all_events["A"]) == args.fencing
        summary = {"run": args.run, "fencing": args.fencing,
                   "failover_seconds": leadership["monotonic"] - stopped_at,
                   "restored_cursor": restored["cursor"], "restored_selected": restored["selected"],
                   "pause_seconds": next(e for e in events(experiment_log) if e["event"] == "SIGCONT")["pause_seconds"],
                   "pointer_version": stat.version, "pointer": json.loads(pointer),
                   "writes": writes, "matches_hw3": True, "sha256": computed[0]["sha256"],
                   "computed": computed}
        atomic_json(ROOT / "evidence" / (args.run + ".json"), summary)
        log(experiment_log, "PASS", **summary)
    finally:
        if paused:
            os.kill(paused.pid, signal.SIGCONT)
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for handle in handles:
            handle.close()
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
