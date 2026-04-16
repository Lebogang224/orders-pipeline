# Optional — for fully containerised runs.
# Default usage is: docker compose up -d (just DB) + python main.py run (on host)
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py", "run"]
