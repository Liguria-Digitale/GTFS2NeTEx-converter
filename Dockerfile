FROM python:3.13-alpine
WORKDIR /app
COPY requirements.txt .
COPY *.py .
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "GTFS2NeTEx-converter.py"]