run the api 

using the redh to store the job_ids

pip install -r requirements.txt

uvicorn main:app --reload

api-endpoints
/upload
    file
    mode
    workers
    top_n

/status/{job_id}
/result/{job_id}

for the script 

python3 processor.py <filepath> --mode <mode [single|multi]> --workers <core> --top-n <count> --rpm-out <outputcsvname>


python3 processor.py /temp_logs.txt --mode multi --workers 2 --top-n 10 --rpm-out rpm.csv
