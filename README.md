# Global E-Commerce Sales Data Pipeline

## Project Overview

This project implements an end-to-end, batch ETL pipeline for global e-commerce sales data. Raw CSV data is ingested, processed, and enriched using a scalable Spark job, loaded into a staging database, and then published to a cloud data warehouse for analytics. The entire workflow is automated and orchestrated with Apache Airflow and fully containerized using Docker for reproducible local development. The final, clean data is exposed via Power BI for business intelligence dashboards.

## Tech Stack

- **Orchestration:** Apache Airflow
- **Data Transformation:** Apache Spark (PySpark)
- **Data Warehouse:** Snowflake
- **Staging Database:** PostgreSQL
- **Containerization:** Docker & Docker Compose
- **Language:** Python
- **Libraries:** pandas, SQLAlchemy, pytest
- **BI & Visualization:** Power BI

## Architecture Diagram

The pipeline follows a standard ETL architecture, moving data from a source file through transformation and staging layers to a final analytics warehouse.

![Architecture Diagram](docs/architecture.png)
*(Self-note: You can create a simple diagram using a tool like [draw.io](https://app.diagrams.net/) or Miro and save the image in your `docs/` folder as `architecture.png`)*

## Key Features

- **End-to-End Automation:** The entire pipeline is orchestrated by an Airflow DAG, running on a daily schedule.
- **Scalable Transformations:** Utilizes Spark for distributed data processing, enabling the pipeline to scale to much larger datasets.
- **Data Quality Checks:** The Spark job includes basic cleaning and validation steps, such as removing nulls and filtering invalid transactions.
- **Staging & Analytics Layers:** Employs a robust two-layer database architecture with PostgreSQL for staging and Snowflake for analytics, which is a common industry best practice.
- **Containerized Environment:** All services (Airflow, Spark, PostgreSQL) are defined in Docker Compose, allowing for a one-command setup (`docker-compose up`).
- **Unit Tested:** Includes unit tests with `pytest` to ensure code reliability and maintainability.

## Project Showcase

### Airflow DAG Run

Here is a screenshot of a successful pipeline run in the Airflow UI, showing the dependencies and flow of tasks.

![Airflow DAG](docs/airflow_dag_run.png)
*(Self-note: Take a screenshot of your successful DAG run in the Airflow graph view and save it in `docs/`)*

### Power BI Dashboard

A business intelligence dashboard was created in Power BI, connecting directly to the Snowflake data warehouse to provide key insights.

![Power BI Dashboard](docs/powerbi_dashboard.png)
*(Self-note: Take a screenshot of the simple dashboard you created and save it in `docs/`)*

## Local Setup

### Prerequisites

- Docker Desktop installed and running.
- Python 3.9+ and a virtual environment tool (e.g., `venv`).
- A Snowflake account (free trial is sufficient).

### Instructions

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-link>
    cd global-ecommerce-pipeline
    ```

2.  **Set Up Python Environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Configure Snowflake Credentials:**
    Add your Snowflake credentials to `docker/docker-compose.yml` under the environment variables for the `airflow-webserver` and `airflow-scheduler` services.
    ```yaml
    environment:
      - SNOWFLAKE_USER=YOUR_USERNAME
      - SNOWFLAKE_PASSWORD=YOUR_PASSWORD
      - SNOWFLAKE_ACCOUNT=YOUR_ACCOUNT_IDENTIFIER
    ```

4.  **Start All Services:**
    ```bash
    # From the 'docker/' directory
    cd docker
    docker-compose up -d
    ```

5.  **Access Airflow & Trigger the DAG:**
    - Open the Airflow UI at `http://localhost:8080`.
    - Log in with username `airflow` and password `airflow`.
    - Un-pause the `ecommerce_data_pipeline` DAG and trigger a manual run.

## Running Tests

To run the unit tests, execute the following command from the project root directory:
```bash
pytest