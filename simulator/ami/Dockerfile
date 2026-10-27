FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ami_simulator.py .

ENTRYPOINT ["python", "-u", "/app/ami_simulator.py"]
CMD ["--mode", "baseline"]
