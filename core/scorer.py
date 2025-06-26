"""Main part scoring functionality."""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from sklearn.preprocessing import RobustScaler, MinMaxScaler

logger = logging.getLogger(__name__)

class PartScorer:
    """Main part scoring class for prioritizing electronic components."""
    
    def __init__(self, config: Dict = None):
        """Initialize scorer with configuration.
        
        Args:
            config: Configuration dictionary with weights and features
        """
        from ..config.settings import get_default_config
        
        self.config = config or get_default_config()
        self.weights = self.config.get('weights', {})
        self.feature_config = self.config.get('features', {})
        
        # Initialize scalers
        self.robust_scaler = RobustScaler()
        self.final_scaler = MinMaxScaler(feature_range=(1, 100))
    
    def calculate_scores(self, df: pd.DataFrame, normalize=True) -> pd.DataFrame:
        """Calculate priority scores for parts dataframe.
        
        Args:
            df: Input dataframe with part data
            normalize: Whether to normalize scores to 0-100 range
            
        Returns:
            DataFrame with scoring features and priority_score added
        """
        if len(df) == 0:
            # Handle empty dataframe
            empty_df = df.copy()
            empty_df['priority_score'] = pd.Series(dtype=float)
            empty_df['score_percentile'] = pd.Series(dtype=float)
            empty_df['base_score'] = pd.Series(dtype=float)
            return empty_df
            
        logger.info(f"Calculating scores for {len(df)} parts")
        
        # Create working copy
        result_df = df.copy()
        
        # Step 1: Engineer features
        result_df = self._engineer_features(result_df)
        
        # Step 2: Calculate base weighted score
        result_df['base_score'] = self._calculate_base_score(result_df)
        
        # Step 3: Apply business rule boosts
        result_df['boosted_score'] = self._apply_boosts(result_df)
        
        # Step 4: Normalize scores
        if normalize:
            result_df['priority_score'] = self._normalize_scores(result_df['boosted_score'])
        else:
            result_df['priority_score'] = result_df['boosted_score']
        
        # Step 5: Add percentile ranking
        result_df['score_percentile'] = result_df['priority_score'].rank(pct=True) * 100
        
        logger.info(f"Scoring complete. Mean score: {result_df['priority_score'].mean():.2f}")
        
        return result_df.sort_values('priority_score', ascending=False)
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create and transform features for scoring."""
        from ..core.feature_engineer import FeatureEngineer
        
        engineer = FeatureEngineer(self.feature_config)
        return engineer.transform(df)
    
    def _calculate_base_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate weighted base score."""
        base_score = pd.Series(0.0, index=df.index)
        
        for feature, weight in self.weights.items():
            if feature in df.columns:
                feature_values = df[feature].fillna(0)
                base_score += feature_values * weight
                logger.debug(f"Added {feature} with weight {weight}")
            else:
                logger.warning(f"Feature {feature} not found in dataframe")
        
        # Zero out completely unavailable items
        if all(col in df.columns for col in ['inventory', 'leadtime_weeks']):
            unavailable = (df['inventory'] == 0) & (df['leadtime_weeks'] > 12)
            base_score[unavailable] = 0
        
        return base_score
    
    def _apply_boosts(self, df: pd.DataFrame) -> pd.Series:
        """Apply business rule boosts to base scores."""
        boosted_score = df['base_score'].copy()
        
        # Predefined boost conditions
        boost_conditions = [
            {
                'name': 'ample_stock',
                'condition': lambda df: (df.get('inventory', 0) >= 10 * df.get('moq', 1)),
                'multiplier': 1.1
            },
            {
                'name': 'immediate_ship',
                'condition': lambda df: (df.get('leadtime_weeks', 999) == 0),
                'multiplier': 1.15
            },
            {
                'name': 'authorized_source',
                'condition': lambda df: (df.get('source_type', '') == 'Authorized'),
                'multiplier': 1.05
            },
            {
                'name': 'high_demand',
                'condition': lambda df: (df.get('demand_all_time', 0) > 100),
                'multiplier': 1.08
            }
        ]
        
        # Apply boost conditions
        for boost in boost_conditions:
            try:
                mask = boost['condition'](df)
                if hasattr(mask, 'sum') and mask.sum() > 0:
                    boosted_score[mask] *= boost['multiplier']
                    logger.info(f"Applied {boost['name']} boost to {mask.sum()} parts")
            except Exception as e:
                logger.warning(f"Could not apply boost {boost['name']}: {e}")
        
        return boosted_score
    
    def _normalize_scores(self, scores: pd.Series) -> pd.Series:
        """Normalize scores to 0-100 range."""
        if len(scores) == 0 or scores.max() == scores.min():
            return pd.Series(50.0, index=scores.index)
        
        # Separate zero and non-zero scores
        non_zero_mask = scores > 0
        
        if non_zero_mask.sum() == 0:
            return pd.Series(0.0, index=scores.index)
        
        # Normalize non-zero scores to 1-100 range
        normalized = scores.copy()
        non_zero_scores = scores[non_zero_mask].values.reshape(-1, 1)
        
        try:
            scaled_scores = self.final_scaler.fit_transform(non_zero_scores)
            normalized[non_zero_mask] = scaled_scores.flatten()
        except Exception as e:
            logger.error(f"Error in normalization: {e}")
            # Fallback to simple min-max
            min_val, max_val = non_zero_scores.min(), non_zero_scores.max()
            normalized[non_zero_mask] = 1 + (scores[non_zero_mask] - min_val) / (max_val - min_val) * 99
        
        return normalized.round(2)