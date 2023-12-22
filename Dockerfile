# Use Python image
FROM python:3.8-slim-buster

# Set the working directory in the container
WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir psycopg2-binary faker redis

# Copy PostgreSQL Faker script
COPY postgresFaker.py /app/postgresFaker.py

# Copy Redis Faker script
COPY redisFaker.py /app/redisFaker.py

COPY db_benchmark.py /app/db_benchmark.py

# Run both PostgreSQL and Redis Faker scripts
CMD ["sh", "-c", "python postgresFaker.py & python redisFaker.py"]
