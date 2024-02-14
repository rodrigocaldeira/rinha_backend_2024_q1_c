# A Dockerfile for the file main.c using clang
# Usage: docker build -t clang .
#        docker run -it --rm clang
#        docker run -it --rm -v $(pwd):/mnt clang
#        docker run -it --rm -v $(pwd):/mnt clang /mnt/main.c
#        docker run -it --rm -v $(pwd):/mnt clang /mnt/main.c -o /mnt/main
#        docker run -it --rm -v $(pwd):/mnt clang /mnt/main.c -o /mnt/main -lm
#        docker run -it --rm -v $(pwd):/mnt clang /mnt/main.c -o /mnt/main -lm -fsanitize=address

ARG BUILDER_IMAGE="ubuntu:22.04"

FROM ${BUILDER_IMAGE} as builder

RUN apt-get update && apt-get -y --no-install-recommends install \
    build-essential \
    clang-15 libcjson-dev \
    libmicrohttpd12 libmicrohttpd-dev \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV C_INCLUDE_PATH="/usr/include/cjson:/usr/include/microhttpd:/usr/include/postgresql"

WORKDIR /app

COPY main.c .

RUN clang-15 -O2 -o main main.c -lpq -lcjson -lmicrohttpd

CMD ["/app/main"]

