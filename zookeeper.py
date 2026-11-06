import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tarfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def download(url, path):
    if not path.exists():
        urllib.request.urlretrieve(url, path)


def unpack(archive, directory):
    directory.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive) as tar:
        for item in tar.getmembers():
            parts = Path(item.name).parts[1:]
            if parts:
                item.name = str(Path(*parts))
                tar.extract(item, directory, filter="data")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["setup", "start", "stop", "status", "cli"])
    parser.add_argument("nodes", nargs="*", type=int, choices=[1, 2, 3])
    args = parser.parse_args()
    os.chdir(ROOT)
    java = ROOT / ".runtime/java17/Contents/Home"
    if args.command == "setup":
        downloads = ROOT / "downloads"
        downloads.mkdir(exist_ok=True)
        archive = downloads / "apache-zookeeper-3.9.6-bin.tar.gz"
        url = "https://archive.apache.org/dist/zookeeper/zookeeper-3.9.6/" + archive.name
        download(url, archive)
        checksum = urllib.request.urlopen(url + ".sha512").read().decode()
        assert hashlib.sha512(archive.read_bytes()).hexdigest() in checksum
        for node in (1, 2, 3):
            unpack(archive, ROOT / f".runtime/zk{node}")
        if not (java / "bin/java").exists():
            archive = downloads / "java17.tar.gz"
            download("https://api.adoptium.net/v3/binary/latest/17/ga/mac/aarch64/jdk/hotspot/normal/eclipse", archive)
            unpack(archive, ROOT / ".runtime/java17")
        subprocess.run([sys.executable, "-m", "venv", ".venv"], check=True)
        subprocess.run([".venv/bin/python", "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        return
    os.environ["JAVA_HOME"] = str(java)
    os.environ["PATH"] = str(java / "bin") + os.pathsep + os.environ["PATH"]
    os.environ["SERVER_JVMFLAGS"] = "-Xms64m -Xmx256m -Dzookeeper.nio.numWorkerThreads=4 -Dzookeeper.nio.numSelectorThreads=1"
    if args.command == "cli":
        node = (args.nodes or [1])[0]
        sys.exit(subprocess.call([".runtime/zk1/bin/zkCli.sh", "-server", f"127.0.0.1:{2180 + node}"]))
    for node in args.nodes or [1, 2, 3]:
        logs = ROOT / f"logs/servers/zk{node}"
        logs.mkdir(parents=True, exist_ok=True)
        os.environ["ZOO_LOG_DIR"] = str(logs)
        data = ROOT / f".runtime/zk{node}/data"
        data.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / f"conf/zk{node}/myid", data / "myid")
        print(f"ZooKeeper {node}: {args.command}", flush=True)
        result = subprocess.run([f".runtime/zk{node}/bin/zkServer.sh", args.command,
                                 str(ROOT / f"conf/zk{node}/zoo.cfg")])
        if result.returncode:
            sys.exit(result.returncode)


if __name__ == "__main__":
    main()
