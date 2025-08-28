FROM ubuntu:24.04 AS base-image

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install --no-install-recommends -y software-properties-common && add-apt-repository ppa:deadsnakes/ppa && apt update
RUN apt install --no-install-recommends -y python3.13
ENV PATH="/app/venv/bin:$PATH"

FROM base-image AS builder-image

ARG DEBIAN_FRONTEND=noninteractive

RUN apt install --no-install-recommends -y python3.13-venv python3.13-dev build-essential
RUN python3.13 -m venv /app/venv
COPY requirements.txt .
RUN pip install --no-cache-dir setuptools
RUN pip install --no-cache-dir -r requirements.txt

# Build FFmpeg from source: https://trac.ffmpeg.org/wiki/CompilationGuide/Ubuntu
RUN apt update -qq && apt install --no-install-recommends -y autoconf \
    automake build-essential cmake git-core libass-dev libfreetype6-dev \
    libgnutls28-dev libmp3lame-dev libtool libvorbis-dev libunistring-dev \
    meson ninja-build pkg-config texinfo wget yasm zlib1g-dev nasm \
    libx264-dev libx265-dev libnuma-dev libvpx-dev libopus-dev \
    libdav1d-dev librist-dev libssl-dev && apt clean && rm -rf /var/lib/apt/lists/*
RUN mkdir -p "$HOME/ffmpeg_sources"
RUN cd "$HOME/ffmpeg_sources" && \
    git -C aom pull 2> /dev/null || git clone --depth 1 https://aomedia.googlesource.com/aom && \
    mkdir -p aom_build && \
    cd aom_build && \
    PATH="$HOME/bin:$PATH" cmake -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" -DENABLE_TESTS=OFF -DENABLE_NASM=on ../aom && \
    PATH="$HOME/bin:$PATH" make -j$(nproc) && \
    make install
RUN cd "$HOME/ffmpeg_sources" && \
    git -C SVT-AV1 pull 2> /dev/null || git clone https://gitlab.com/AOMediaCodec/SVT-AV1.git && \
    mkdir -p SVT-AV1/build && \
    cd SVT-AV1/build && \
    PATH="$HOME/bin:$PATH" cmake -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" -DCMAKE_BUILD_TYPE=Release -DBUILD_DEC=OFF -DBUILD_SHARED_LIBS=OFF .. && \
    PATH="$HOME/bin:$PATH" make -j$(nproc) && \
    make install
RUN cd "$HOME/ffmpeg_sources" && \
    git clone 'https://github.com/Netflix/vmaf' 'vmaf-master' && \
    mkdir -p 'vmaf-master/libvmaf/build' && \
    cd 'vmaf-master/libvmaf/build' && \
    meson setup -Denable_tests=false -Denable_docs=false --buildtype=release --default-library=static '../' --prefix "$HOME/ffmpeg_build" --bindir="$HOME/bin" --libdir="$HOME/ffmpeg_build/lib" && \
    ninja && \
    ninja install
RUN cd "$HOME/ffmpeg_sources" && \
    git -C srt pull 2> /dev/null || git clone --depth 1 https://github.com/Haivision/srt.git && \
    mkdir -p srt/build && \
    cd srt/build && \
    cmake -DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" -DENABLE_SHARED=OFF -DCMAKE_BUILD_TYPE=Release .. && \
    make -j$(nproc) && \
    make install
RUN mkdir -p "$HOME/bin"
RUN cd "$HOME/ffmpeg_sources" && \
    wget -O ffmpeg-8.0.tar.bz2 https://ffmpeg.org/releases/ffmpeg-8.0.tar.bz2 && \
    tar xjvf ffmpeg-8.0.tar.bz2 && \
    cd ffmpeg-8.0 && \
    PATH="$HOME/bin:$PATH" PKG_CONFIG_PATH="$HOME/ffmpeg_build/lib/pkgconfig" ./configure \
    --prefix="$HOME/ffmpeg_build" \
    --pkg-config-flags="--static" \
    --extra-cflags="-I$HOME/ffmpeg_build/include" \
    --extra-ldflags="-L$HOME/ffmpeg_build/lib" \
    --extra-libs="-lpthread -lm" \
    --ld="g++" \
    --bindir="$HOME/bin" \
    --enable-gpl --enable-gnutls --enable-libaom --enable-libass \
    --enable-libfreetype --enable-libmp3lame --enable-libopus \
    --enable-libsvtav1 --enable-libdav1d --enable-libvorbis --enable-libvpx \
    --enable-libx264 --enable-libx265 --enable-libsrt --enable-librist && \
    PATH="$HOME/bin:$PATH" make -j$(nproc) && \
    make install && \
    hash -r

FROM base-image AS runner-image

RUN apt install --no-install-recommends -y iputils-ping curl

# FFmpeg binaries and libraries
COPY --from=builder-image /root/bin/ /usr/bin/
RUN apt install --no-install-recommends -y libass9 libmp3lame0 libvorbis0a \
    libvorbisenc2 libvpx9 libdav1d7 libopus0 libx264-164 libx265-199 librist4
    
RUN apt install --no-install-recommends -y vlc && apt clean && rm -rf /var/lib/apt/lists/*

RUN userdel -r ubuntu
RUN useradd app
COPY --from=builder-image /app/venv /app/venv

USER app
WORKDIR /app
COPY app.py /app
COPY nexus_tuner /app/nexus_tuner
COPY templates /app/templates
COPY public /app/public
COPY entrypoint.sh /app
COPY VERSION /app

ENV LANG=C.UTF-8
ENV PYTHONPATH=/app/nexus_tuner
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=/app/venv

ENV NEXUS_CONFIG_DIR=/config
ENV NEXUS_PORT=4040

ENTRYPOINT ["/app/entrypoint.sh"]