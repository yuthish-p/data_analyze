from pathlib import Path
import redis


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


TMP_DIR = Path("/up_files") if Path("/up_files").exists() else Path.cwd() / "up_files"
TMP_DIR.mkdir(parents=True, exist_ok=True)


