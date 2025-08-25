# See https://containers.dev/guide/dockerfile for guidance on building dev container images.

# Use official Node.js base image
FROM mcr.microsoft.com/devcontainers/javascript-node:22
ARG GO_VERSION=1.24.1

RUN set -eux; \
    ARCH=$(uname -m); \
    if [ "$ARCH" = "x86_64" ]; then \
        GO_URL="https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        GO_URL="https://go.dev/dl/go${GO_VERSION}.linux-arm64.tar.gz"; \
    else \
        echo "Unsupported architecture: $ARCH"; exit 1; \
    fi; \
    curl -OL "$GO_URL"; \
    rm -rf /usr/local/go; \
    tar -C /usr/local -xzf $(basename "$GO_URL"); \
    rm $(basename "$GO_URL")
ENV PATH="/usr/local/go/bin:$PATH"

# Install mage for building go projects
RUN GOBIN=/usr/local/go/bin go install github.com/magefile/mage@latest

# Set up a non-root user (optional)
ARG USERNAME=vscode
RUN useradd -m $USERNAME || true
USER $USERNAME

WORKDIR /workspace
