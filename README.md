# skein

A mother **proxy** with little ducklings **worker** running queries.

A mother has HTTP api endpoint to send queries.
Queries are queued and executed by little ducklings.

Plan:
 - priority queue for queries (there's already a priority in the request)
 - separate queues for separate contexts (user / system)
 - max concurrent queries per context (user / system)
 - add a frontend to send queries
 - fully logging the utilized resources (now, there's profiling enabled but it's effect on performance are not clear)
 - support duckings sizes

# requirements

you need `mprocs` and `golang` installed.

# how to run?

the easiest way is to run `mprocs` in the main directory, it will start the proxy and the worker

check `benchmark_test.go` and `send_query.sh` to see how to send queries to the proxy

# datasets

```shell
mkdir -p datasets/taxi
cd datasets/taxi
wget https://github.com/duckdb/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet
wget https://github.com/duckdb/duckdb-data/releases/download/v1.0/taxi_2019_05.parquet
wget https://github.com/duckdb/duckdb-data/releases/download/v1.0/taxi_2019_06.parquet
```

Download taxi_*.parquet from: https://github.com/duckdb/duckdb-data/releases
