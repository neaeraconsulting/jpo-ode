#!/usr/bin/env python3
"""High-rate UDP load generator for ODE receivers.

The stock udpsender_*.py scripts sleep + print every packet, which typically
tops out around ~500-800 msg/s on Windows. This script is meant for finding
the ODE decode/publish ceiling.

Examples:
  # Blast BSMs as fast as one process can send (no sleep)
  python udpsender_loadtest.py --port 46800 --rate 0

  # Target ~2000 Hz with progress reports
  python udpsender_loadtest.py --port 46800 --rate 2000

  # Multi-process blast (use when a single sender saturates first)
  python udpsender_loadtest.py --port 46800 --rate 0 --workers 4

  # Generic receiver (44990) with BSM payload
  python udpsender_loadtest.py --port 44990 --rate 1000

Env:
  DOCKER_HOST_IP  default target host if --host is omitted
"""

from __future__ import annotations

import argparse
import os
import socket
import time
from multiprocessing import Process, Value
from typing import Optional

# Same payload as udpsender_bsm.py (1609.3 wrapper + BSM)
DEFAULT_BSM_HEX = (
    "0022e12d18466c65c1493800000e00e4616183e85a8f0100c000038081bc001480b8494c4c"
    "950cd8cde6e9651116579f22a424dd78fffff00761e4fd7eb7d07f7fff80005f11d1020214"
    "c1c0ffc7c016aff4017a0ff65403b0fd204c20ffccc04f8fe40c420ffe6404cefe60e9a101"
    "33408fcfde1438103ab4138f00e1eec1048ec160103e237410445c171104e26bc103dc415"
    "4305c2c84103b1c1c8f0a82f42103f34262d1123198103dac25fb12034ce10381c259f120"
    "38ca103574251b10e3b2210324c23ad0f23d8efffe0000209340d10000004264bf00"
)

PORTS = {
    "bsm": 46800,
    "generic": 44990,
    "tim": 47900,
    "map": 46753,
    "spat": 44910,
}


def send_loop(
    host: str,
    port: int,
    payload: bytes,
    rate: float,
    duration: float,
    report_every: float,
    counter: Optional[Value],
    worker_id: int,
) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Larger send buffer helps when blasting faster than the NIC/kernel drains.
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
    except OSError:
        pass

    interval = 0.0 if rate <= 0 else 1.0 / rate
    next_send = time.perf_counter()
    start = next_send
    last_report = start
    sent = 0
    last_sent = 0

    try:
        while True:
            now = time.perf_counter()
            if duration > 0 and (now - start) >= duration:
                break

            sock.sendto(payload, (host, port))
            sent += 1
            if counter is not None:
                with counter.get_lock():
                    counter.value += 1

            if interval > 0:
                next_send += interval
                sleep_for = next_send - time.perf_counter()
                if sleep_for > 0:
                    time.sleep(sleep_for)
                elif sleep_for < -0.25:
                    # Fell far behind; resync to avoid spiral after pauses.
                    next_send = time.perf_counter()

            if report_every > 0 and worker_id == 0:
                now = time.perf_counter()
                elapsed_report = now - last_report
                if elapsed_report >= report_every:
                    total = counter.value if counter is not None else sent
                    delta = total - last_sent
                    hz = delta / elapsed_report
                    overall = total / (now - start)
                    print(
                        f"[loadtest] send_hz={hz:,.0f} overall_hz={overall:,.0f} "
                        f"sent={total:,} elapsed={now - start:,.1f}s",
                        flush=True,
                    )
                    last_report = now
                    last_sent = total
    finally:
        sock.close()
        if worker_id == 0:
            elapsed = max(time.perf_counter() - start, 1e-9)
            total = counter.value if counter is not None else sent
            print(
                f"[loadtest] done sent={total:,} avg_hz={total / elapsed:,.0f} "
                f"elapsed={elapsed:,.1f}s",
                flush=True,
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="ODE UDP load tester")
    parser.add_argument(
        "--host",
        default=os.getenv("DOCKER_HOST_IP") or "127.0.0.1",
        help="ODE host (default: DOCKER_HOST_IP or 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="UDP port (default from --receiver, else 46800 BSM)",
    )
    parser.add_argument(
        "--receiver",
        choices=sorted(PORTS.keys()),
        default="bsm",
        help="Named receiver port shortcut",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Target send rate in Hz (0 = blast as fast as possible)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Seconds to run (0 = until Ctrl+C)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of sender processes",
    )
    parser.add_argument(
        "--report-every",
        type=float,
        default=1.0,
        help="Progress report interval seconds (0 disables)",
    )
    parser.add_argument(
        "--hex-file",
        default=None,
        help="Optional file containing hex payload (whitespace ignored)",
    )
    args = parser.parse_args()

    port = args.port if args.port is not None else PORTS[args.receiver]
    if args.hex_file:
        with open(args.hex_file, "r", encoding="utf-8") as f:
            payload = bytes.fromhex("".join(f.read().split()))
    else:
        payload = bytes.fromhex(DEFAULT_BSM_HEX)

    per_worker_rate = args.rate / args.workers if args.rate > 0 else 0.0
    print(
        f"[loadtest] target={args.host}:{port} payload_bytes={len(payload)} "
        f"rate={args.rate:g}Hz workers={args.workers} "
        f"per_worker={per_worker_rate:g}Hz duration={args.duration:g}s",
        flush=True,
    )
    print(
        "[loadtest] Prometheus tips:\n"
        "  sum(rate(kafka_produced_rsu_messages_total{topic=\"topic.OdeBsmJson\"}[$__rate_interval]))\n"
        "  sum(rate(ode_ffmlib_decode_total_seconds_count[$__rate_interval]))\n"
        "  avg(rate(ode_ffmlib_decode_stage_seconds_sum[$__rate_interval]) / "
        "rate(ode_ffmlib_decode_stage_seconds_count[$__rate_interval])) by (stage)",
        flush=True,
    )

    if args.workers <= 1:
        send_loop(
            args.host,
            port,
            payload,
            args.rate,
            args.duration,
            args.report_every,
            None,
            0,
        )
        return

    counter = Value("Q", 0)
    procs = []
    for i in range(args.workers):
        p = Process(
            target=send_loop,
            args=(
                args.host,
                port,
                payload,
                per_worker_rate,
                args.duration,
                args.report_every,
                counter,
                i,
            ),
        )
        p.start()
        procs.append(p)
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()


if __name__ == "__main__":
    # Required on Windows for multiprocessing
    main()
