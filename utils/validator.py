"""Data validation utilities."""

import pandas as pd
import logging
from typing import Dict, List, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Results of data validation."""
    is_valid: bool
    total_rows: int
    valid_rows: int
    issues: List[Dict[str, Any]]
    quality_score: float

class DataValidator:
    """Simple data validator for the module."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
    
    def validate_batch(self, df: pd.DataFrame, batch_id: str = None) -> ValidationResult:
        """Validate a batch of data."""
        logger.info(f"Validating batch {batch_id} with {len(df)} rows")
        
        issues = []
        
        # Basic structure validation
        if len(df) == 0:
            issues.append({
                'type': 'empty_dataframe',
                'severity': 'critical',
                'message': 'Dataframe is empty'
            })
        
        # Check for required columns
        required_columns = ['pn']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            issues.append({
                'type': 'missing_columns',
                'severity': 'critical',
                'message': f'Missing required columns: {missing_columns}'
            })
        
        # Calculate quality score
        critical_issues = [i for i in issues if i.get('severity') == 'critical']
        quality_score = 100.0 - (len(critical_issues) * 25)
        
        result = ValidationResult(
            is_valid=len(critical_issues) == 0,
            total_rows=len(df),
            valid_rows=len(df) if len(critical_issues) == 0 else 0,
            issues=issues,
            quality_score=max(0.0, quality_score)
        )
        
        logger.info(f"Validation complete: Quality Score {result.quality_score}/100")
        return result