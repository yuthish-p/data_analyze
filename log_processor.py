import re
from pathlib import Path
import json
from datetime import datetime
import multiprocessing as mp
from config import r


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


def worker(filepath: str, queue: mp.Queue, start: int, end: int):
    
    endpoint_counts = {}
    status_counts = {}
    rpm_counts = {}

    with open(filepath, "r") as f:
        f.seek(start)
        if start > 0:
            f.readline()  
        pos = f.tell()
        print(f"pos=>{pos}")
        while pos < end:
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



def analyze(job_id: str, filepath: str, mode: str = "multi", workers: int = 4, top_n: int = 10):
    try:
        r.hset(job_id, mapping={"status": "processing", "progress": "0"})

        size = Path(filepath).stat().st_size
        chunk_size = size // workers

        queue = mp.Queue()
        processes = []
        for i in range(workers):
            start = i * chunk_size
            
            if i == workers - 1:
                end = size
            else:
                end =(i + 1) * chunk_size
                
            p = mp.Process(target=worker, args=(filepath, queue, start, end))
            processes.append(p)
            p.start()

        agg_endpoints, agg_status, agg_rpm = {}, {}, {}
        done = 0
        while done < workers:
            e, s, r_counts = queue.get()
            for k, v in e.items():
                agg_endpoints[k] = agg_endpoints.get(k, 0) + v
            for k, v in s.items():
                agg_status[k] = agg_status.get(k, 0) + v
            for k, v in r_counts.items():
                agg_rpm[k] = agg_rpm.get(k, 0) + v
            done += 1
            r.hset(job_id, "progress", f"{int(done/workers*100)}")

        for p in processes:
            p.join()

        top_endpoints = sorted(agg_endpoints.items(), key=lambda x: x[1], reverse=True)[:top_n]
        result = {
            "top_endpoints": [{"endpoint": ep, "count": c} for ep, c in top_endpoints],
            "status_distribution": agg_status,
            "rpm_series": [{"minute": m, "count": c} for m, c in sorted(agg_rpm.items())],
        }

        r.hset(job_id, mapping={
            "status": "done",
            "result": json.dumps(result),
            "progress": "100"
        })

    except Exception as e:
        r.hset(job_id, mapping={"status": "failed", "error": str(e)})
