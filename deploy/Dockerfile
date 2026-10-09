# The exchange: the gateway (engine, journal, web page) and the bots, in one image.
FROM rust:1.89-slim-bookworm AS build
WORKDIR /src
COPY . .
# On a small machine, --build-arg JOBS=2 keeps the build from taking all of it.
ARG JOBS
RUN cargo build --release --locked -p gateway --bin gateway --bin bots ${JOBS:+--jobs $JOBS}

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 exchange \
    && mkdir -p /data \
    && chown exchange /data
COPY --from=build /src/target/release/gateway /src/target/release/bots /usr/local/bin/
USER exchange
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["gateway"]
