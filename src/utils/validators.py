import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Results of data validation."""
    is_valid: bool
    total_rows: int
    valid_rows: int
    issues: List[Dict[str, Any]]
    field_coverage: Dict[str, Dict[str, Any]]
    quality_score: float

class DataValidator:
    """Comprehensive data validation for scoring pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.quality_thresholds = config.get('quality_thresholds', {})
        self.required_coverage = self.quality_thresholds.get('required_coverage', {})
    
    def validate_batch(self, df: pd.DataFrame, batch_id: str = None) -> ValidationResult:
        """Validate a batch of data comprehensively."""
        logger.info(f"Validating batch {batch_id} with {len(df)} rows")
        
        issues = []
        field_coverage = {}
        
        # Basic structure validation
        issues.extend(self._validate_structure(df))
        
        # Field-level validation
        field_coverage = self._calculate_field_coverage(df)
        issues.extend(self._validate_field_coverage(field_coverage))
        
        # Data quality validation
        issues.extend(self._validate_data_quality(df))
        
        # Business rule validation
        issues.extend(self._validate_business_rules(df))
        
        # Calculate metrics
        critical_issues = [i for i in issues if i.get('severity') == 'critical']
        valid_rows = len(df) - sum(i.get('affected_rows', 0) for i in critical_issues)
        
        quality_score = self._calculate_quality_score(df, issues, field_coverage)
        
        result = ValidationResult(
            is_valid=len(critical_issues) == 0,
            total_rows=len(df),
            valid_rows=max(0, valid_rows),
            issues=issues,
            field_coverage=field_coverage,
            quality_score=quality_score
        )
        
        self._log_validation_results(result, batch_id)
        return result
    
    def _validate_structure(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Validate basic data structure."""
        issues = []
        
        # Check for empty dataframe
        if len(df) == 0:
            issues.append({
                'type': 'empty_dataframe',
                'severity': 'critical',
                'message': 'Dataframe is empty',
                'affected_rows': 0
            })
        
        # Check for required columns
        required_columns = ['pn', 'inventory']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            issues.append({
                'type': 'missing_columns',
                'severity': 'critical',
                'message': f'Missing required columns: {missing_columns}',
                'affected_rows': len(df)
            })
        
        return issues
    
    def _calculate_field_coverage(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Calculate field coverage statistics."""
        coverage = {}
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            coverage[col] = {
                'present': True,
                'null_count': int(null_count),
                'coverage_pct': float((len(df) - null_count) / len(df) * 100),
                'unique_values': int(df[col].nunique()),
                'data_type': str(df[col].dtype)
            }
        
        return coverage
    
    def _validate_field_coverage(self, field_coverage: Dict) -> List[Dict[str, Any]]:
        """Validate field coverage against requirements."""
        issues = []
        
        for field, required_pct in self.required_coverage.items():
            if field in field_coverage:
                actual_pct = field_coverage[field]['coverage_pct']
                if actual_pct < required_pct:
                    issues.append({
                        'type': 'insufficient_coverage',
                        'field': field,
                        'severity': 'warning',
                        'message': f'{field} coverage {actual_pct:.1f}% below required {required_pct}%',
                        'expected': required_pct,
                        'actual': actual_pct
                    })
        
        return issues
    
    def _validate_data_quality(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Validate data quality against thresholds."""
        issues = []
        
        # Validate numeric ranges
        numeric_fields = {
            'inventory': (self.quality_thresholds.get('min_inventory', 0),
                         self.quality_thresholds.get('max_inventory', 1000000)),
            'first_price': (self.quality_thresholds.get('min_price', 0.01),
                           self.quality_thresholds.get('max_price', 100000)),
            'moq': (self.quality_thresholds.get('min_moq', 1),
                   self.quality_thresholds.get('max_moq', 100000)),
            'leadtime_weeks': (self.quality_thresholds.get('min_leadtime_weeks', 0),
                              self.quality_thresholds.get('max_leadtime_weeks', 52))
        }
        
        for field, (min_val, max_val) in numeric_fields.items():
            if field in df.columns:
                out_of_range = df[
                    (df[field].notnull()) & 
                    ((df[field] < min_val) | (df[field] > max_val))
                ]
                
                if len(out_of_range) > 0:
                    issues.append({
                        'type': 'out_of_range',
                        'field': field,
                        'severity': 'warning',
                        'message': f'{len(out_of_range)} {field} values out of range [{min_val}, {max_val}]',
                        'affected_rows': len(out_of_range),
                        'examples': out_of_range[field].head(5).tolist()
                    })
        
        return issues
    
    def _validate_business_rules(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Validate business-specific rules."""
        issues = []
        
        # Check for components with zero inventory and long lead times
        if all(col in df.columns for col in ['inventory', 'leadtime_weeks']):
            unavailable = df[(df['inventory'] == 0) & (df['leadtime_weeks'] > 12)]
            if len(unavailable) > 0:
                issues.append({
                    'type': 'unavailable_components',
                    'severity': 'info',
                    'message': f'{len(unavailable)} components are unavailable (no stock, >12 week leadtime)',
                    'affected_rows': len(unavailable)
                })
        
        # Check for missing price data on in-stock items
        if all(col in df.columns for col in ['inventory', 'first_price']):
            missing_price = df[(df['inventory'] > 0) & (df['first_price'].isnull())]
            if len(missing_price) > 0:
                issues.append({
                    'type': 'missing_price_data',
                    'severity': 'warning',
                    'message': f'{len(missing_price)} in-stock components missing price data',
                    'affected_rows': len(missing_price)
                })
        
        return issues
    
    def _calculate_quality_score(
        self, 
        df: pd.DataFrame, 
        issues: List[Dict], 
        field_coverage: Dict
    ) -> float:
        """Calculate overall data quality score (0-100)."""
        
        # Start with perfect score
        score = 100.0
        
        # Deduct points for issues
        for issue in issues:
            if issue['severity'] == 'critical':
                score -= 25
            elif issue['severity'] == 'warning':
                score -= 10
            elif issue['severity'] == 'info':
                score -= 2
        
        # Adjust for field coverage
        total_coverage = sum(fc['coverage_pct'] for fc in field_coverage.values()) / len(field_coverage)
        coverage_penalty = max(0, (95 - total_coverage) * 0.5)  # Penalty if under 95% average coverage
        score -= coverage_penalty
        
        return max(0.0, min(100.0, score))
    
    def _log_validation_results(self, result: ValidationResult, batch_id: str = None):
        """Log validation results."""
        logger.info(f"Validation complete for batch {batch_id}: "
                   f"Quality Score: {result.quality_score:.1f}/100, "
                   f"Valid Rows: {result.valid_rows}/{result.total_rows}")
        
        if result.issues:
            for issue in result.issues:
                level = logging.ERROR if issue['severity'] == 'critical' else logging.WARNING
                logger.log(level, f"Validation issue: {issue['message']}")
