import argparse
import json
import os
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

from kazoo.exceptions import BadVersionError

from common import ROOT, check_transaction, connect, encode, log


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--fencing", type=int, choices=[0, 1], default=int(os.getenv("FENCING", "0")))
    args = parser.parse_args()
    zk = connect()
    base = "/hw4/" + args.run
    logfile = ROOT / "logs" / args.run / "receiver.jsonl"

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *unused):
            pass

        def do_POST(self):
            try:
                message = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                transaction = zk.transaction()
                if args.fencing:
                    transaction.check(base + "/epoch", version=message["epoch"])
                transaction.set_data(base + "/result", encode(message))
                transaction.create(base + "/writes/" + uuid.uuid4().hex, encode(message))
                check_transaction(transaction.commit())
                _, stat = zk.get(base + "/result")
                response = {"accepted": True, "pointer_version": stat.version}
                log(logfile, "accepted", fencing=args.fencing, **message, **response)
                status = 200
            except BadVersionError:
                response = {"accepted": False, "error": "BadVersionError"}
                log(logfile, "rejected", fencing=args.fencing, **message, **response)
                status = 409
            except Exception as error:
                response = {"accepted": False, "error": type(error).__name__}
                log(logfile, "error", detail=str(error))
                status = 500
            data = encode(response)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    log(logfile, "ready", fencing=args.fencing)
    try:
        HTTPServer(("127.0.0.1", 8765), Handler).serve_forever()
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()
