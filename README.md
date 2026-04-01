[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

<!-- What does this pipeline do? --
This pipeline is a data tool for Amman Digital Market. It takes raw sales data from a database and turns it into a clean summary report.

It performs 4 main steps:

1. Extracts: It connects to PostgreSQL and pulls tables for Customers, Products, and Orders.

2. Cleans: It removes "bad" data, like canceled orders or orders with quantities over 100.

3. Calculates: It joins the tables to find out how much each customer spent and their preferred category.

4. Saves: It creates a final file called customer_analytics.csv to show the results
>

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

<!-- What does customer_analytics.csv contain?

Customer ID & Name: Who is the buyer?

Total Orders: How many successful orders did they make?

Total Revenue: Total money spent by this customer.

Top Category: Which product category they bought the most.
 -->

## Quality Checks

<!-- What validations are performed and why? 

We perform these checks to make sure the data is 100% correct:

Filter Cancelled: We remove cancelled orders so they don't count as sales.

Filter Quantities: We remove any order with more than 100 items to avoid "Data Entry" mistakes.

Check Nulls: The code stops and shows an error if a Customer ID is missing.

Positive Totals: We make sure all prices and revenues are more than zero.-->

---

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
