# docker run -v /path/to/files:/mnt -v /path/to/gcp/credentials.json:/app/credentials.json -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials.json -it p2j /mnt/<PARQUET> /mnt/<JSON>

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

ADD . /app

WORKDIR /app
RUN uv sync --frozen

ENTRYPOINT ["uv", "run", "parquet2json"]