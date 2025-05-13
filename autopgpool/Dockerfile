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

# Install Python and required dependencies
RUN apk add --no-cache python3 py3-pip busybox libevent postgresql-client && \
  mkdir -p /etc/pgbouncer /var/log/pgbouncer /var/run/pgbouncer && \
  touch /etc/pgbouncer/userlist.txt && \
  chown -R postgres /var/log/pgbouncer /var/run/pgbouncer /etc/pgbouncer

# Copy pgbouncer binary and example configs
COPY --from=build /pgbouncer/pgbouncer /usr/bin
COPY --from=build /pgbouncer/etc/pgbouncer.ini /etc/pgbouncer/pgbouncer.ini.example
COPY --from=build /pgbouncer/etc/userlist.txt /etc/pgbouncer/userlist.txt.example

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create a virtual environment and install Python using uv
ENV VIRTUAL_ENV=/opt/venv
RUN uv venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Create a working directory for the package
WORKDIR /app

# Copy the autopg package
COPY . .

# Install the package
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --active

# Create bootstrap script
RUN echo '#!/bin/bash\n\
\n\
echo "Running Autopg for PgBouncer..."\n\
\n\
autopg build-config --pg-path /etc/pgbouncer\n\
\n\
echo "Booting PgBouncer..."\n\
\n\
exec "$@"\n' > /app/bootstrap.sh && \
chmod +x /app/bootstrap.sh

EXPOSE 5432
USER postgres
ENTRYPOINT ["/app/bootstrap.sh"]
CMD ["/usr/bin/pgbouncer", "/etc/pgbouncer/pgbouncer.ini"]
