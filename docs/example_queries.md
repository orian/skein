```sql
SELECT count(*)
   FROM 'taxi_2019_04.parquet'
   WHERE pickup_at BETWEEN '2019-04-15' AND '2019-04-20';
```

```sql
SELECT count(*)
   FROM 'taxi_2019_04.parquet'
   WHERE pickup_at > '2019-06-30'
```

```sql
SELECT passenger_count, count(*)
    FROM 'taxi_2019_04.parquet'
    GROUP BY passenger_count
```