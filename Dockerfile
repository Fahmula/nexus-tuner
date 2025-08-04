FROM ubuntu:24.04 AS builder-image

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install --no-install-recommends -y software-properties-common && add-apt-repository ppa:deadsnakes/ppa && apt update
RUN apt install --no-install-recommends -y python3.13 python3.13-venv python3.13-dev build-essential
RUN apt clean && rm -rf /var/lib/apt/lists/*

RUN python3.13 -m venv /app/venv
ENV PATH="/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir setuptools
RUN pip install --no-cache-dir -r requirements.txt

FROM ubuntu:24.04 AS runner-image
RUN apt update && apt install --no-install-recommends -y software-properties-common && add-apt-repository ppa:deadsnakes/ppa && apt update
RUN apt install --no-install-recommends -y python3.13
RUN apt install --no-install-recommends -y iputils-ping curl ffmpeg && apt clean && rm -rf /var/lib/apt/lists/*

RUN useradd app
COPY --from=builder-image /app/venv /app/venv

USER app
WORKDIR /app
COPY app.py /app
COPY nexus_tuner /app/nexus_tuner
COPY templates /app/templates
COPY entrypoint.sh /app
COPY VERSION /app

ENV NEXUS_CONFIG_DIR=/config
ENV NEXUS_PORT=4040

ENV LANG=C.UTF-8
ENV PYTHONPATH=/app/nexus_tuner
ENV PYTHONUNBUFFERED=1

ENV VIRTUAL_ENV=/app/venv
ENV PATH="/app/venv/bin:$PATH"

ENTRYPOINT ["/app/entrypoint.sh"]