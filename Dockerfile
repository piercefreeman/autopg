FROM postgres:16

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Create a virtual environment and install Python using uv
ENV VIRTUAL_ENV=/opt/venv
RUN uv venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Create a working directory for the package
WORKDIR /app

# Copy the package files
COPY . .

# Install dependencies using uv
RUN uv sync --active
RUN chmod +x /app/bootstrap.sh

# Keep the original postgres entrypoint
ENTRYPOINT ["/app/bootstrap.sh"]
