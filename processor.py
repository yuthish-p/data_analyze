
import argparse
import csv
import json
import multiprocessing as mp
import re
from datetime import datetime
from pathlib import Path
import signal
import sys


log_pattern = re.compile(
    r'(?P<ip>\S+) - - \[(?P<time>.*?)\] "(?P<method>\S+) (?P<endpoint>\S+) \S+" (?P<status>\d{3}) \d+'
)

def parse_line(line: str):
    m = log_pattern.match(line)
    if not m:
        return None, None, None
    endpoint = m.group("endpoint")
    status = m.group("status")
    ts = datetime.strptime(m.group("time").split()[0], "%d/%b/%Y:%H:%M:%S")
    minute = ts.strftime("%Y-%m-%d %H:%M")
    return endpoint, status, minute


def worker(filepath: str, start: int, end: int, queue: mp.Queue, stop_event: mp.Event):
    endpoint_counts = {}
    status_counts = {}
    rpm_counts = {}

    with open(filepath, "r") as f:
        f.seek(start)
        if start > 0:
            f.readline()  
        pos = f.tell()
        while pos < end and not stop_event.is_set():
            line = f.readline()
            if not line:
                break
            endpoint, status, minute = parse_line(line)
            if endpoint:
                endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
                status_counts[status] = status_counts.get(status, 0) + 1
                rpm_counts[minute] = rpm_counts.get(minute, 0) + 1
            pos = f.tell()

    queue.put((endpoint_counts, status_counts, rpm_counts))


def aggregate(queue: mp.Queue, num_chunks: int):
    agg_endpoints, agg_status, agg_rpm = {}, {}, {}
    for _ in range(num_chunks):
        e, s, r_counts = queue.get()
        for k, v in e.items():
            agg_endpoints[k] = agg_endpoints.get(k, 0) + v
        for k, v in s.items():
            agg_status[k] = agg_status.get(k, 0) + v
        for k, v in r_counts.items():
            agg_rpm[k] = agg_rpm.get(k, 0) + v
    return agg_endpoints, agg_status, agg_rpm

def single_process(filepath: str):
    endpoint_counts = {}
    status_counts = {}
    rpm_counts = {}
    try:
        with open(filepath, "r") as f:
            for line in f:
                endpoint, status, minute = parse_line(line)
                if endpoint:
                    endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
                    status_counts[status] = status_counts.get(status, 0) + 1
                    rpm_counts[minute] = rpm_counts.get(minute, 0) + 1
        return endpoint_counts, status_counts, rpm_counts
    except KeyboardInterrupt:
        print("Process interrupted .")
        


def multi_process(filepath: str, workers: int):
    size = Path(filepath).stat().st_size
    chunk_size = size // workers
    queue = mp.Queue()
    stop_event = mp.Event()
    processes = []

    def signal_handler(sig, frame):
        stop_event.set()
        for p in processes:
            p.terminate()
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)

    try:    
        for i in range(workers):
            start = i * chunk_size
            end = size if i == workers - 1 else (i + 1) * chunk_size
            p = mp.Process(target=worker, args=(filepath, start, end, queue, stop_event))
            processes.append(p)
            p.start()

        agg_endpoints, agg_status, agg_rpm = aggregate(queue, len(processes))

        for p in processes:
            p.join()

        return agg_endpoints, agg_status, agg_rpm

    except KeyboardInterrupt:
        print("Process interrupted ")
        stop_event.set()
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        return {}, {}, {}  

def print_top_endpoints_csv(endpoint_counts: dict, top_n: int):
    writer = csv.writer(sys.stdout)
    writer.writerow(["endpoint", "count"])
    for ep, count in sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]:
        writer.writerow([ep, count])

def write_rpm_csv(rpm_counts: dict, out_file: str):
    with open(out_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["minute", "count"])
        for minute, count in sorted(rpm_counts.items()):
            writer.writerow([minute, count])

def print_status_json(status_counts: dict):
    print(json.dumps(status_counts, indent=2))



def main():
    parser = argparse.ArgumentParser(description="analyzer")
    parser.add_argument("filepath", help="Path to log file")
    parser.add_argument("--mode", choices=["single", "multi"], default="multi")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--rpm-out", required=True, help="Output CSV for RPM series")
    args = parser.parse_args()

    if args.mode == "single":
        endpoints, status, rpm = single_process(args.filepath)
    else:
        endpoints, status, rpm = multi_process(args.filepath, args.workers)

    print_top_endpoints_csv(endpoints, args.top_n)
    write_rpm_csv(rpm, args.rpm_out)
    print_status_json(status)

if __name__ == "__main__":
    main()
