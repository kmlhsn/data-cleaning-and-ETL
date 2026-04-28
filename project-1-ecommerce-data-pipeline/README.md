# E-Commerce Data Pipeline

This project focuses on creating an ETL pipeline for E-Commerce data. The project consists of several components as follows:

## Structure
- `src/pipeline.py`: Main orchestrator coordinating all ETL stages
- `src/data_loader.py`: DataLoader class for CSV/Parquet loading with schema validation
- `src/cleaner.py`: DataCleaner class handling duplicates, missing values, outlier detection
- `src/transformer.py`: DataTransformer class for feature engineering and datetime parsing
- `src/validator.py`: DataValidator class for data quality checks
- `src/ai_enricher.py`: AIEnricher class with fraud detection and customer segmentation
- `config/config.yaml`: Configuration file with data paths, required columns, transformation rules
- `requirements.txt`: Dependencies
- `README.md`: Project documentation

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python src/pipeline.py
```

## Dependencies

- pandas
- numpy
- scikit-learn
- pyyaml
- python-dotenv
