FROM python:3.11-slim

WORKDIR /app

COPY packages/ /app/packages/
RUN pip install --no-cache-dir /app/packages/

COPY services/transformer_silver/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/transformer_silver/app.py .

CMD ["python", "app.py"]
