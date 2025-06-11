# Dockerfile

FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    wget https://github.com/jellyfin/jellyfin-ffmpeg/releases/download/v7.1.1-4/jellyfin-ffmpeg7_7.1.1-4-bookworm_amd64.deb && \
    apt-get install -y ./jellyfin-ffmpeg7_7.1.1-4-bookworm_amd64.deb && \
    cp /usr/lib/jellyfin-ffmpeg/ffmpeg /usr/bin/ffmpeg && \
    # Clean up to keep the image small
    apt-get purge -y --auto-remove wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm jellyfin-ffmpeg7_7.1.1-4-bookworm_amd64.deb

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

ENV PYTHONPATH="/app/src"

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]