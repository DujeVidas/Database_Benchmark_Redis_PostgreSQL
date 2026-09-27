# Database Benchmarking with PostgreSQL and Redis

A benchmarking project comparing the performance characteristics of **PostgreSQL** and **Redis** under different database workloads.

The project uses Python to generate synthetic test data, execute automated benchmarks, collect performance metrics, and export the results to Excel for further analysis. The database environment is containerized using Docker Compose.

## Technologies

- Python
- PostgreSQL
- Redis
- Docker & Docker Compose
- Pandas
- psycopg2
- redis-py
- Faker
- OpenPyXL
- ThreadPoolExecutor

## Benchmark Scenarios

The benchmark evaluates PostgreSQL and Redis under several different workload scenarios:

- **Read/Write Workloads** – measures read and write throughput in operations per second
- **Key Access Frequency** – measures response times under repeated data access
- **Concurrent Operations** – executes read and write operations concurrently
- **Row Load Testing** – measures response time while querying different amounts of data
- **Read-Heavy Transactions** – evaluates performance under repeated read operations
- **Write-Heavy Transactions** – evaluates performance under repeated insert operations
- **Transactional Operations** – performs combinations of read, insert, and update operations

## Performance Metrics

The benchmark collects and compares:

- Read throughput (ops/sec)
- Write throughput (ops/sec)
- Response time
- Row query time
- Concurrent operation time
- Read-heavy transaction time
- Write-heavy transaction time

Both **average and median values** are calculated from the collected measurements.

## How It Works

1. PostgreSQL and Redis are started in a Docker environment.
2. `postgresFaker.py` and `redisFaker.py` populate the databases with synthetic user data generated using Faker.
3. `db_benchmark.py` executes the benchmark scenarios.
4. Execution times and throughput metrics are collected.
5. Pandas is used to process the measurements and calculate averages and medians.
6. Results are automatically exported to an Excel workbook containing separate sheets for each benchmark scenario.

By default, the database population scripts generate **1,000 records** for each database. The number of benchmark iterations can be configured in `db_benchmark.py`.

## Results

The experiments demonstrate the different performance characteristics of an in-memory key-value store and a relational database.

Under the tested workloads, **Redis generally achieved higher throughput and lower response times**, particularly for frequent data access and concurrent operations.

However, raw performance is only one factor when selecting a database. PostgreSQL provides relational data modeling, SQL querying, structured transactions, and persistent storage, while Redis is particularly suitable for scenarios requiring very fast data access, caching, and in-memory operations.

The benchmark therefore illustrates how database selection depends on the application's **data model, workload, persistence requirements, query complexity, and transactional requirements**.

## Full Report

A detailed experimental report is available in:

**[Report_Eng.pdf](./Report_Eng.pdf)**

The report contains the complete methodology, system architecture, benchmark scenarios, performance graphs, result analysis, and conclusions.

## Running the Benchmark

### Prerequisites

Make sure you have installed:

- Docker
- Docker Compose
- Python 3

Clone the repository:

```bash
git clone https://github.com/DujeVidas/Database_Benchmark_Redis_PostgreSQL.git
cd Database_Benchmark_Redis_PostgreSQL
```

Install the required Python dependencies:

```bash
pip install redis psycopg2 pandas faker openpyxl psutil
```

Start the benchmarking environment:

```bash
docker-compose up --build
```

This will create and start the containers required for PostgreSQL, Redis, and the supporting environment.

Once the databases are running, execute:

```bash
python db_benchmark.py
```

The benchmark results will be exported to an Excel file:

```text
database_metrics_<NUM_ITERATIONS>.xlsx
```

When finished, stop the environment with:

```bash
docker-compose down
```

## Generated Results

The generated Excel workbook contains separate worksheets for:

- averages and medians
- Redis and PostgreSQL read/write throughput
- key access frequency
- row-load response times
- concurrent operation times
- read-heavy transactions
- write-heavy transactions
- transactional operations

This makes it possible to inspect both individual benchmark runs and aggregated performance metrics.

## Project Structure

```text
Database_Benchmark_Redis_PostgreSQL/
├── db_benchmark.py
├── postgresFaker.py
├── redisFaker.py
├── init.sql
├── Dockerfile
├── docker-compose.yml
├── Report_Eng.pdf
└── sheets/
    └── database_metrics_*.xlsx
```

## Key Takeaways

This project provided practical experience with:

- PostgreSQL and Redis
- Database performance benchmarking
- Python database integration
- Concurrent database operations
- Performance measurement and analysis
- Synthetic test-data generation
- Pandas data processing
- Docker and Docker Compose
- Automated Excel report generation

It also demonstrates how different database architectures can perform very differently depending on the workload and why database technology should be selected according to application requirements rather than raw performance alone.

## Author

**Duje Vidas**  
University Master of Informatics
