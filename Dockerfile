# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/langolier ./cmd/langolier

# Data directory pre-created with the runtime uid. Docker seeds a fresh
# named/anonymous volume from this path on first mount, but only if it is
# non-empty — hence the .keep file — so the volume ends up owned by uid 65532
# (the scratch image has no shell to chown a volume at runtime).
RUN install -d -m 0700 -o 65532 -g 65532 /data-skel \
 && install -m 0600 -o 65532 -g 65532 /dev/null /data-skel/.keep

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/langolier /langolier
COPY --from=build --chown=65532:65532 /data-skel /data

# Time-zone data is embedded via the time/tzdata import.
VOLUME ["/data"]
ENV DATA_DIR=/data
USER 65532:65532

ENTRYPOINT ["/langolier"]
