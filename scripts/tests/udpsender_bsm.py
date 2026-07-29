#!/usr/bin/env python3
"""Send BSMs to the ODE BSM UDP receiver (port 46800).

For high-rate load testing, prefer udpsender_loadtest.py — this script's default
1 ms sleep + print loop typically tops out around ~600 Hz on Windows.
"""

import argparse
import os
import socket
import time

UDP_IP = os.getenv("DOCKER_HOST_IP") or "127.0.0.1"
UDP_PORT = 46800
MESSAGE = (
    "0022e12d18466c65c1493800000e00e4616183e85a8f0100c000038081bc001480b8494c4c"
    "950cd8cde6e9651116579f22a424dd78fffff00761e4fd7eb7d07f7fff80005f11d1020214"
    "c1c0ffc7c016aff4017a0ff65403b0fd204c20ffccc04f8fe40c420ffe6404cefe60e9a101"
    "33408fcfde1438103ab4138f00e1eec1048ec160103e237410445c171104e26bc103dc415"
    "4305c2c84103b1c1c8f0a82f42103f34262d1123198103dac25fb12034ce10381c259f120"
    "38ca103574251b10e3b2210324c23ad0f23d8efffe0000209340d10000004264bf00"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="ODE BSM UDP sender")
    parser.add_argument("--host", default=UDP_IP)
    parser.add_argument("--port", type=int, default=UDP_PORT)
    parser.add_argument(
        "--interval",
        type=float,
        default=0.001,
        help="Seconds between sends (0 = blast; use udpsender_loadtest.py for serious load)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print every send (strongly recommended above ~100 Hz)",
    )
    args = parser.parse_args()

    payload = bytes.fromhex(MESSAGE)
    print(f"UDP target IP: {args.host}")
    print(f"UDP target port: {args.port}")
    print(f"interval={args.interval}s quiet={args.quiet} payload_bytes={len(payload)}")
    if args.interval <= 0.001 and not args.quiet:
        print(
            "WARNING: printing every packet will dominate CPU. "
            "Use --quiet or scripts/tests/udpsender_loadtest.py"
        )

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sent = 0
    start = time.perf_counter()
    last_report = start
    try:
        while True:
            sock.sendto(payload, (args.host, args.port))
            sent += 1
            if not args.quiet:
                print("sending BSM")
            if args.interval > 0:
                time.sleep(args.interval)
            now = time.perf_counter()
            if now - last_report >= 1.0:
                print(f"send_hz={sent / (now - start):,.0f} sent={sent:,}", flush=True)
                last_report = now
    except KeyboardInterrupt:
        elapsed = max(time.perf_counter() - start, 1e-9)
        print(f"stopped avg_hz={sent / elapsed:,.0f} sent={sent:,}")


if __name__ == "__main__":
    main()
