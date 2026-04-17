FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY harris_scraper.py .
COPY server.py .
COPY start.sh .
RUN chmod +x start.sh

EXPOSE 8080

ENTRYPOINT ["sh", "start.sh"]
