# Use the official Python slim image
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
VOLUME ["/app"]

RUN pip install --upgrade pip
RUN pip install --no-cache-dir google-api-python-client
RUN pip install --no-cache-dir google-auth
RUN pip install --no-cache-dir google-auth-oauthlib>=0.4.1
RUN pip install --no-cache-dir schedule
RUN pip install --no-cache-dir paho-mqtt
RUN pip install --no-cache-dir ha-mqtt-discoverable
RUN pip install --no-cache-dir bs4
RUN pip install --no-cache-dir Pillow

CMD ["python", "YoutubeStreamCreator.py"]
