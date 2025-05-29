ARG python_version=3.13

FROM python:${python_version}-alpine AS build

RUN apk add --no-cache build-base linux-headers libffi-dev libressl-dev cargo uv

ENV UV_TOOL_DIR=/app
RUN uv tool install --compile-bytecode rnmon

FROM python:${python_version}-alpine
ARG python_version

# Only copy the necessary files from the build stage, to improve layer efficiency
COPY --from=build /app/rnmon /app

RUN mkdir /config

RUN addgroup -S app --gid 1000 && adduser -S app --uid 1000 -G app
RUN chown -R app:app /config /app

USER app:app

VOLUME ["/config"]

ENV PYTHONUNBUFFERED=1

WORKDIR /app/bin
ENTRYPOINT ["./python", "rnmon", "--rns-config", "/config/reticulum", "--config", "/config/scraping.yaml"]
