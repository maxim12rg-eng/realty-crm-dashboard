FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r dwh_requirements.txt

CMD ["python", "etl_full_v6.py"]
