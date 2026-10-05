## Requirements

- Java 17
- Apache Maven 3.9.6
- Docker version 24.0.7

## Execution steps

1) docker rmi -f edge-sqlite-db:1.0.0
2) mvn clean package
3) docker build -f Dockerfile . -t edge-sqlite-db:1.0.0
4) docker compose -f docker-kafka-compose.yml up
5) docker compose -f docker-compose.yml up
6) docker compose -f docker-client-compose.yaml up
7) docker compose -f docker-second-client-compose.yaml u

## Queries

The list of analytical SQL queries designed for benchmarking and evaluation are categorizes below. The queries are grouped
based on their characteristics such as selectivity, joins, and filtering complexity.
Queries 1.1 to 2.3 are originated from the SSB benchmark while the others were developed as part of this study.

### 1.1 Revenue by Year (Low Selectivity)
```sql
SELECT
    SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM
    lineorder AS lo,
    date_table AS d
WHERE
    lo.lo_orderdate = d.d_datekey
    AND d.d_year = 1993
    AND lo.lo_discount BETWEEN 1 AND 3
    AND lo.lo_quantity < 25;
```

### 1.2 Revenue by Year-Month (Medium Selectivity)
```sql
SELECT
    SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM
    lineorder AS lo,
    date_table AS d
WHERE
    lo.lo_orderdate = d.d_datekey
    AND d.d_yearmonthnum = 199401
    AND lo.lo_discount BETWEEN 4 AND 6
    AND lo.lo_quantity BETWEEN 26 AND 35;
```

### 1.3 Revenue by Week (High Selectivity)
```sql
SELECT
    SUM(lo.lo_extendedprice * lo.lo_discount) AS revenue
FROM
    lineorder AS lo,
    date_table AS d
WHERE
    lo.lo_orderdate = d.d_datekey
    AND d.d_year = 1994
    AND d.d_weeknuminyear = 6
    AND lo.lo_discount BETWEEN 5 AND 7
    AND lo.lo_quantity BETWEEN 26 AND 35;
```

---


### 2.1 Revenue by Product Category & Supplier Region
```sql
SELECT
    SUM(lo.lo_revenue) AS revenue
FROM
    lineorder AS lo,
    date_table AS d,
    part AS p,
    supplier AS s
WHERE
    lo.lo_orderdate = d.d_datekey
    AND lo.lo_partkey = p.p_partkey
    AND lo.lo_suppkey = s.s_suppkey
    AND p.p_category = 'MFGR#12'
    AND s.s_region = 'AMERICA'
    AND d.d_year = 1997;
```

### 2.2 Revenue by Brand & Supplier Region
```sql
SELECT
    SUM(lo.lo_revenue) AS revenue
FROM
    lineorder AS lo,
    date_table AS d,
    part AS p,
    supplier AS s
WHERE
    lo.lo_orderdate = d.d_datekey
    AND lo.lo_partkey = p.p_partkey
    AND lo.lo_suppkey = s.s_suppkey
    AND p.p_brand = 'MFGR#2221'
    AND s.s_region = 'ASIA'
    AND d.d_year = 1998;
```

### 2.3 Revenue by Brand in Europe
```sql
SELECT
    SUM(lo.lo_revenue) AS revenue
FROM
    lineorder AS lo,
    date_table AS d,
    part AS p,
    supplier AS s
WHERE
    lo.lo_orderdate = d.d_datekey
    AND lo.lo_partkey = p.p_partkey
    AND lo.lo_suppkey = s.s_suppkey
    AND p.p_brand = 'MFGR#2239'
    AND s.s_region = 'EUROPE'
    AND d.d_year = 1997;
```

---

### 1 Orders with Customer & Date Filtering
```sql
SELECT
    lo.lo_orderkey AS order_key,
    c.c_name AS customer_name,
    d.d_year AS order_year
FROM
    lineorder AS lo,
    customer AS c,
    date_table AS d
WHERE
    lo.lo_custkey = c.c_custkey
    AND lo.lo_orderdate = d.d_datekey
    AND d.d_year = 1997
    AND c.c_region = 'EUROPE';
```

### 2 Supplier–Part Join
```sql
SELECT
    lo.lo_orderkey AS order_key,
    p.p_partkey AS part_key,
    s.s_name AS supplier_name
FROM
    lineorder AS lo,
    part AS p,
    supplier AS s
WHERE
    lo.lo_partkey = p.p_partkey
    AND lo.lo_suppkey = s.s_suppkey
    AND p.p_category = 'MFGR#12'
    AND s.s_region = 'ASIA';
```

### 3 Customer–Supplier Join
```sql
SELECT
    lo.lo_orderkey AS order_key,
    c.c_city AS customer_city,
    s.s_city AS supplier_city
FROM
    lineorder AS lo,
    customer AS c,
    supplier AS s
WHERE
    lo.lo_custkey = c.c_custkey
    AND lo.lo_suppkey = s.s_suppkey
    AND c.c_region = 'ASIA'
    AND s.s_region = 'ASIA';
```

---

### 4. Join All Tables with Part Brand and Supplier Region Filtering
```sql
SELECT
    lo.lo_orderkey AS order_key,
    c.c_name AS customer_name,
    s.s_name AS supplier_name,
    p.p_brand AS part_brand,
    d.d_date AS order_date
FROM
    lineorder AS lo,
    customer AS c,
    supplier AS s,
    part AS p,
    date_table AS d
WHERE
    lo.lo_custkey = c.c_custkey
    AND lo.lo_suppkey = s.s_suppkey
    AND lo.lo_partkey = p.p_partkey
    AND lo.lo_orderdate = d.d_datekey
    AND d.d_year = 1998
    AND p.p_brand = 'MFGR#2239'
    AND s.s_region = 'EUROPE';
```

---

### 5. Join All Tables with Part Brand and Customer Region Filtering.
```sql
SELECT
    lo.lo_orderkey AS order_key,
    c.c_name AS customer_name,
    c.c_region AS customer_region,
    s.s_name AS supplier_name,
    s.s_region AS supplier_region,
    p.p_brand AS part_brand,
    p.p_category AS part_category,
    d.d_date AS order_date,
    d.d_year AS order_year
FROM
    lineorder AS lo,
    customer AS c,
    supplier AS s,
    part AS p,
    date_table AS d
WHERE
    lo.lo_custkey = c.c_custkey
    AND lo.lo_suppkey = s.s_suppkey
    AND lo.lo_partkey = p.p_partkey
    AND lo.lo_orderdate = d.d_datekey
    AND p.p_brand = 'MFGR#2221'
    AND c.c_region = 'AMERICA';
```
### 6. Join All Tables with Supplier Region, Part Brand, Order Quantity and Date Range Filtering.
```sql
SELECT
    lo.lo_orderkey AS order_key,
    c.c_name AS customer_name,
    s.s_name AS supplier_name,
    p.p_brand AS part_brand,
    d.d_date AS order_date
FROM
    lineorder AS lo,
    customer AS c,
    supplier AS s,
    part AS p,
    date_table AS d
WHERE
    lo.lo_custkey = c.c_custkey
    AND lo.lo_suppkey = s.s_suppkey
    AND lo.lo_partkey = p.p_partkey
    AND lo.lo_orderdate = d.d_datekey
    AND lo.lo_quantity <= 19
    AND p.p_brand = 'MFGR#3110'
    AND d.d_year BETWEEN 1997 AND 1998
    AND s.s_region = 'ASIA';
```
