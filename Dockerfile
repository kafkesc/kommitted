ARG BASE_IMG=rust
ARG BASE_IMG_VERSION=1.72.1-slim-bookworm

FROM $BASE_IMG:$BASE_IMG_VERSION
ARG BASE_IMG
ARG BASE_IMG_VERSION
ARG KOMMITTED_VERSION

LABEL base.image=$BASE_IMG:$BASE_IMG_VERSION \
    name=kommitted \
    version=$KOMMITTED_VERSION \
    description="Measure Kafka Consumer Offset Lag and Time Lag" \
    repository="https://github.com/kafkesc/kommitted" \
    homepage="https://crates.io/crates/kommitted" \
    license="MIT OR Apache-2.0"

ENV BUILD_DEPS "tcl-dev libssl-dev libsasl2-dev"

# Setup
RUN \
	apt update && \
	apt install -y ${BUILD_DEPS}

# Build
RUN \
	cargo install --version "${KOMMITTED_VERSION#v}" kommitted

# Cleanup
RUN \
    rm -rf /usr/local/cargo/registry && \
    apt remove --purge -y ${BUILD_DEPS} && \
	apt autoremove -y && \
	apt autoclean -y && \
	rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["kommitted"]