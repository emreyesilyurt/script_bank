"""Simplified part scorer for module usage."""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class PartScorer:
    """Main part scoring class - simplified for module usage."""
    
    def __init__(self, config: Dict = None):
        """Initialize scorer with configuration."""
        from ..config.settings import get_default_config
        
        self.config = config or get_default_config()
        self.weights = self.config.get('weights', {})
        self.feature_config = self.config.get('features', {})
    
    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate priority scores for parts dataframe."""
        logger.info(f"Scoring {len(df)} parts")
        
        # Feature engineering
        df = self._engineer_features(df)
        
        # Calculate base scores
        df['base_score'] = self._calculate_base_score(df)
        
        # Apply business boosts
        df['priority_score'] = self._apply_boosts(df)
        
        # Normalize to 0-100
        df['priority_score'] = self._normalize_scores(df['priority_score'])
        
        return df.sort_values('priority_score', ascending=False)
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create scoring features."""
        from ..core.feature_engineer import FeatureEngineer
        
        engineer = FeatureEngineer(self.feature_config)
        return engineer.transform(df)
    
    def _calculate_base_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate weighted base score."""
        score = pd.Series(0.0, index=df.index)
        
        for feature, weight in self.weights.items():
            if feature in df.columns:
                score += df[feature].fillna(0) * weight
        
        return score
    
    def _apply_boosts(self, df: pd.DataFrame) -> pd.Series:
        """Apply business rule boosts."""
        score = df['base_score'].copy()
        
        # Example boosts
        if 'inventory' in df.columns and 'moq' in df.columns:
            ample_stock = df['inventory'] >= 10 * df['moq'] 
            score[ample_stock] *= 1.1
        
        if 'leadtime_weeks' in df.columns:
            immediate = df['leadtime_weeks'] == 0
            score[immediate] *= 1.15
            
        return score
    
    def _normalize_scores(self, scores: pd.Series) -> pd.Series:
        """Normalize scores to 0-100 range."""
        if scores.max() == scores.min():
            return pd.Series(50.0, index=scores.index)
        
        # Min-max normalization
        min_score = scores.min()
        max_score = scores.max()
        normalized = (scores - min_score) / (max_score - min_score) * 100
        
        return normalized.round(2)