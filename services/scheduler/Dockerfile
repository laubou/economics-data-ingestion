FROM python:3.11-slim

WORKDIR /app

# Install shared package first — layer cached until packages/ changes
COPY packages/ packages/
RUN pip install --no-cache-dir -e packages/

# Copy all services the scheduler orchestrates
COPY services/downloader/ services/downloader/
COPY services/producer/   services/producer/
COPY services/scheduler/  services/scheduler/

CMD ["python", "services/scheduler/app.py"]
