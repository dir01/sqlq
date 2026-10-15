# Build-time networking is allowed; the resulting VM has no external NIC.
FROM golang:1.27-bookworm AS build
WORKDIR /src/library
COPY go.mod go.sum ./
COPY tests/chaos/go.mod tests/chaos/go.sum ./tests/chaos/
WORKDIR /src/library/tests/chaos
RUN go mod download
WORKDIR /src/library
COPY *.go ./
WORKDIR /src/library/tests/chaos
COPY tests/chaos/internal ./internal
COPY tests/chaos/cmd ./cmd
ARG CHECK_PROFILE=standard
RUN go vet ./... \
    && case "$CHECK_PROFILE" in \
      standard) check_flag='' ;; \
      race) check_flag='-race' ;; \
      asan) check_flag='-asan' ;; \
      *) exit 1 ;; \
    esac \
    && export GOEXPERIMENT=cgocheck2 CGO_ENABLED=1 \
    && go build $check_flag -gcflags=all=-d=checkptr=2 -trimpath -o /sqlq-chaos ./cmd/sqlq-chaos \
    && go -C /src/library test -c $check_flag -gcflags=all=-d=checkptr=2 -trimpath -o /sqlq-readiness.test .

FROM debian:bookworm-slim
ARG TARGETARCH
ARG CHECK_PROFILE=standard
RUN case "$TARGETARCH" in arm64|amd64) ;; *) exit 1 ;; esac \
    && apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    "linux-image-$TARGETARCH" postgresql-15 iproute2 util-linux cpio gzip coreutils ca-certificates libasan8 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /sqlq-chaos /usr/local/bin/sqlq-chaos
COPY --from=build /sqlq-readiness.test /usr/local/bin/sqlq-readiness.test
COPY tests/chaos/vm/init /init
RUN chmod 755 /init && mkdir -p /vm /proc /sys /dev /run /tmp /data \
    && (test -e /dev/console || mknod /dev/console c 5 1) \
    && cp /boot/vmlinuz-* /vm/kernel \
    && printf '{"architecture":"%s","check_profile":"%s"}\n' "$TARGETARCH" "$CHECK_PROFILE" > /vm/image.json \
    && rm -rf /boot /var/lib/postgresql/15/main \
    && (find . -xdev -path ./vm -prune -o -path ./proc -print -prune -o -path ./sys -print -prune \
       -o -path ./dev -prune -o -print; printf './dev\n./dev/console\n./dev/null\n') \
       | cpio --quiet -o -H newc | gzip -1 > /vm/initramfs.gz
