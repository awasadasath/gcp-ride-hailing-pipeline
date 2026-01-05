FROM python:3.11

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
COPY key.json .
COPY python/uber_simulator.py .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "uber_simulator.py"]