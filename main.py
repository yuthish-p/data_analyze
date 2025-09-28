from fastapi import FastAPI, UploadFile, Form, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
import uuid
import json

from config import r,TMP_DIR
from log_processor import analyze


app = FastAPI(title="Log Analyzer API", version="1.0")

@app.post("/upload")
async def upload_file(
    file: UploadFile,
    background_tasks: BackgroundTasks,
    mode: str = Form("multi"),
    workers: int = Form(4),
    top_n: int = Form(10),
):
    job_id = str(uuid.uuid4())
    filepath = TMP_DIR / f"{job_id}.log"
    
    print(file.filename)
    print(filepath)

    try:
        contents = await file.read()       
        with open(filepath, "wb") as f:
            f.write(contents)              
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot save file: {e}")
    finally:
        await file.close()
        
    r.hset(job_id, mapping={
        "status": "pending",
        "filepath": str(filepath),
        "result": ""
    })

    background_tasks.add_task(analyze, job_id, str(filepath), mode, workers, top_n)

    return {"job_id": job_id, "message": "job started"}


@app.get("/status/{job_id}")
def check_status(job_id: str):
    job = r.hgetall(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": job.get("status"), "progress": job.get("progress")}


@app.get("/result/{job_id}")
def get_result(job_id: str):
    job = r.hgetall(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("status") == "done":
        return json.loads(job["result"])
    elif job.get("status") == "failed":
        raise HTTPException(status_code=500, detail=f"Job f: {job.get('error')}")
    else:
        return JSONResponse(
            status_code=202,
            content={"message": "processing", "status": job.get("status")},
        )
