FROM python:3.11-slim

WORKDIR /app

# Install shared package first (layer cached until packages/ changes)
COPY packages/ /app/packages/
RUN pip install --no-cache-dir /app/packages/

COPY services/downloader/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/downloader/app.py .

CMD ["python", "app.py"]
