import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
import json
from datetime import datetime

from src.data_loader import DataLoader
from src.cleaner import DataCleaner
from src.transformer import DataTransformer
from src.validator import DataValidator
from src.ai_enricher import AIEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EcommercePipeline:
    """
    Main orchestrator for the E-Commerce ETL pipeline.
    Coordinates all modules: loading, cleaning, transformation, validation, and enrichment.
    """
    
    def __init__(self, config_path: str):
        """
        Initialize pipeline with configuration
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.loader = DataLoader(config_path)
        self.cleaner = DataCleaner(self.loader.config)
        self.transformer = DataTransformer(self.loader.config)
        self.validator = DataValidator(self.loader.config)
        self.enricher = AIEnricher(self.loader.config)
        
        self.config = self.loader.config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_path = Path(self.config['data']['output']['path'])
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.execution_report = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': None,
            'stages': {}
        }
    
    def run(self, input_file: str) -> pd.DataFrame:
        """
        Execute complete ETL pipeline
        
        Args:
            input_file: Name of input CSV file
            
        Returns:
            Processed and enriched DataFrame
        """
        self.logger.info("=" * 80)
        self.logger.info("Starting E-Commerce ETL Pipeline")
        self.logger.info("=" * 80)
        
        self.execution_report['start_time'] = datetime.now().isoformat()
        start_time = datetime.now()
        
        try:
            # Stage 1: Load Data
            self.logger.info("\n[STAGE 1] Loading Data...")
            df = self._load_stage(input_file)
            
            # Stage 2: Clean Data
            self.logger.info("\n[STAGE 2] Cleaning Data...")
            df = self._clean_stage(df)
            
            # Stage 3: Transform Data
            self.logger.info("\n[STAGE 3] Transforming Data...")
            df = self._transform_stage(df)
            
            # Stage 4: Validate Data
            self.logger.info("\n[STAGE 4] Validating Data...")
            df = self._validate_stage(df)
            
            # Stage 5: AI Enrichment
            self.logger.info("\n[STAGE 5] AI Enrichment...")
            df = self._enrich_stage(df)
            
            self.execution_report['end_time'] = datetime.now().isoformat()
            self.execution_report['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            self.logger.info("\n" + "=" * 80)
            self.logger.info("Pipeline Execution SUCCESSFUL")
            self.logger.info("=" * 80)
            
            return df
        
        except Exception as e:
            self.logger.error(f"\nPipeline Execution FAILED: {str(e)}", exc_info=True)
            self.execution_report['error'] = str(e)
            raise
    
    def _load_stage(self, input_file: str) -> pd.DataFrame:
        """Load and validate input data"""
        df = self.loader.load_csv(input_file)
        self.loader.validate_schema(df, self.config['validation']['required_columns'])
        
        self.execution_report['stages']['load'] = {
            'rows': len(df),
            'columns': len(df.columns),
            'memory_mb': df.memory_usage(deep=True).sum() / 1024**2
        }
        
        self.logger.info(f"Loaded: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def _clean_stage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize data"""
        df = self.cleaner.clean(df)
        report = self.cleaner.get_cleaning_report()
        
        self.execution_report['stages']['clean'] = report
        self.execution_report['stages']['clean']['rows_after'] = len(df)
        
        self.logger.info(f"Cleaned: {report}")
        return df
    
    def _transform_stage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and engineer features"""
        df = self.transformer.transform(df)
        report = self.transformer.get_transformation_report()
        
        self.execution_report['stages']['transform'] = {
            'transformations_applied': report,
            'new_columns': len(df.columns)
        }
        
        self.logger.info(f"Transformed: {len(report)} operations applied")
        return df
    
    def _validate_stage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality"""
        is_valid, report = self.validator.validate(
            df, 
            strict_mode=self.config['validation'].get('strict_mode', False)
        )
        
        self.execution_report['stages']['validate'] = report
        
        if not is_valid:
            self.logger.warning(f"Validation issues: {report['errors']}")
        
        return df
    
    def _enrich_stage(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply AI enrichment"""
        df = self.enricher.enrich(df)
        report = self.enricher.get_enrichment_report()
        
        self.execution_report['stages']['enrich'] = report
        
        self.logger.info(f"Enrichment report: {report}")
        return df
    
    def export_results(self, output_file: str, format: str = 'parquet') -> None:
        """
        Export processed data to file
        
        Args:
            output_file: Output filename
            format: Output format ('parquet', 'csv', 'json')
        """
        # This will be called after run()
        pass
    
    def get_execution_report(self) -> Dict:
        """Return complete execution report"""
        return self.execution_report
    
    def save_execution_report(self, filename: str = 'execution_report.json') -> None:
        """Save execution report to JSON file"""
        report_path = self.output_path / filename
        
        with open(report_path, 'w') as f:
            json.dump(self.execution_report, f, indent=2, default=str)
        
        self.logger.info(f"Execution report saved: {report_path}")
