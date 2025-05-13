FROM alpine:3.21 AS build
ARG VERSION=1.24.1

# Install build dependencies
RUN apk add --no-cache autoconf autoconf-doc automake curl gcc git libc-dev libevent-dev libtool make openssl-dev pandoc pkgconfig

# Download and extract pgbouncer
RUN curl -sS -o /pgbouncer.tar.gz -L https://pgbouncer.github.io/downloads/files/$VERSION/pgbouncer-$VERSION.tar.gz && \
  tar -xzf /pgbouncer.tar.gz && mv /pgbouncer-$VERSION /pgbouncer

# Build pgbouncer
RUN cd /pgbouncer && ./configure --prefix=/usr && make

FROM alpine:3.21

RUN apk add --no-cache busybox libevent postgresql-client && \
  mkdir -p /etc/pgbouncer /var/log/pgbouncer /var/run/pgbouncer && \
  chown -R postgres /var/log/pgbouncer /var/run/pgbouncer /etc/pgbouncer

# Copy pgbouncer binary
COPY --from=build /pgbouncer/pgbouncer /usr/bin

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create a virtual environment and install Python using uv
ENV VIRTUAL_ENV=/opt/venv
RUN uv venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Create a working directory for the package
WORKDIR /app

# Install dependencies using uv
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --active --no-install-project

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --active

RUN chmod +x /app/bootstrap.sh

EXPOSE 5432
ENTRYPOINT ["/app/bootstrap.sh"]
CMD ["/usr/bin/pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
