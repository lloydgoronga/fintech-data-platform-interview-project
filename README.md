# Fintech Data Platform - Interview Project

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)
![Redis](https://img.shields.io/badge/redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white)

This project is a comprehensive data engineering solution designed to establish a robust data platform for a new fintech initiative. It demonstrates a complete lifecycle from data ingestion and ETL to data warehousing, real-time analytics, and governance.

## Architecture

The platform is built on a modern, containerized architecture that separates concerns and ensures reproducibility.

![Data Platform Architecture](./docs/architecture.png)

### Key Components:
* **Data Sources:** A PostgreSQL database (`transactional_db`) simulating a production OLTP system and a MongoDB database for semi-structured merchant data.
* **Batch ETL Pipeline:** A Python script using `pandas` for transformation, orchestrated to extract data from sources, apply business logic and quality checks, and load it into the data warehouse.
* **Data Warehouse:** A PostgreSQL database (`dwh`) structured with a Star Schema optimized for analytical queries.
* **Real-Time Stream:** A Redis Pub/Sub channel (`financial_transactions`) for streaming data.
* **Stream Processing:** A Python consumer that listens to the real-time stream and applies a fraud-detection rule.
* **Containerization:** All services are fully containerized with Docker and managed by Docker Compose for a consistent, one-command setup.

## Tech Stack
- **Language:** Python 3.10
- **Databases:** PostgreSQL (Source & DWH), MongoDB (Source)
- **Streaming:** Redis (Pub/Sub)
- **ETL/Transformation:** Pandas, SQLAlchemy
- **Containerization:** Docker, Docker Compose
- **Configuration:** python-dotenv

## Getting Started

These instructions will get you a copy of the project up and running on your local machine.

### Prerequisites

- [Git](https://git-scm.com/)
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

### Setup & Execution

1.  **Clone the repository:**
    ```sh
    git clone [https://github.com/YourUsername/fintech-data-platform-interview-project.git](https://github.com/YourUsername/fintech-data-platform-interview-project.git)
    cd fintech-data-platform-interview-project
    ```

2.  **Start all services with Docker Compose:**
    (Ensure Docker Desktop is running)
    ```sh
    docker-compose up -d
    ```

3.  **Set up the Python environment and install dependencies:**
    ```sh
    # Create a virtual environment
    python -m venv venv
    # Activate it (for Windows Command Prompt)
    .\venv\Scripts\activate
    # Install required packages
    pip install -r requirements.txt
    ```

4.  **Run the Batch ETL Pipeline:**
    This will populate the data warehouse.
    ```sh
    python etl/main_pipeline.py
    ```

5.  **Run the Real-Time Analytics System:**
    (You will need two separate terminals for this)

    * **In Terminal 1, start the producer:**
        ```sh
        python streaming/producer.py
        ```
    * **In Terminal 2, start the consumer:**
        ```sh
        python streaming/consumer.py
        ```
    You will now see transactions being produced and consumed, with alerts for any transaction over $2000.

## Data Warehouse Schema

The data warehouse uses a Star Schema, which is ideal for analytical queries. It consists of one central fact table and three dimension tables.

### Sample Analytical Queries

You can connect to the data warehouse using any SQL client (like DBeaver or pgAdmin) with the credentials from the `.env` file (Host: `localhost`, Port: `5434`).

**1. Total Spending per Customer:**
```sql
SELECT c.first_name, SUM(f.amount) as total_spent
FROM fact_transactions f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.first_name
ORDER BY total_spent DESC;
```

**2. Total Sales by Merchant Category:**
```sql
SELECT m.category, SUM(f.amount) as total_sales
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_key = m.merchant_key
GROUP BY m.category
ORDER BY total_sales DESC;
```

