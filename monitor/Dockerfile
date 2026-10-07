FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir cbor2

COPY monitor.py /app/monitor.py

RUN mkdir -p /data

EXPOSE 8080

CMD ["python", "-u", "/app/monitor.py"]
