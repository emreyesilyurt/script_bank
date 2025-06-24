# Prioritization Scoring ETL

This project contains the data pipeline and scoring logic for ranking electronic components based on demand, pricing, inventory, and other key metadata fields.

## Project Structure

- `data/` – For storing local test samples and outputs.
- `scripts/` – Main ETL and scoring scripts.
- `sql/` – BigQuery SQL queries used to extract data.
- `notebooks/` – Jupyter notebooks for exploration and debugging.
- `config/` – Configuration files and weights for scoring.
- `tests/` – Unit tests for feature logic and ETL pipeline.
- `docs/` – Documentation and visualizations for model design and evaluation.

## Workflow

1. **Extract**: Load test batches from BigQuery using SQL in the `sql/` folder.
2. **Transform**: Parse and normalize features in Python (`scripts/etl_priority_model.py`).
3. **Score**: Apply a weighted formula to calculate priority scores.
4. **Evaluate**: Use notebooks for analysis, and adjust weights/config as needed.
5. **Scale**: Once tested, scale using BigQuery batch jobs or cloud orchestration.

## Author

Emre Can Yesilyurt – Data Engineer at EETech
