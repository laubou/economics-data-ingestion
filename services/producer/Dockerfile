FROM python:3.11-slim

WORKDIR /app

COPY packages/ /app/packages/
RUN pip install --no-cache-dir /app/packages/

COPY services/producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/producer/app.py .

CMD ["python", "app.py"]
