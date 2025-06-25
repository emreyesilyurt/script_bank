import numpy as np
import pandas as pd
import json
import logging
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ComponentScorer:
    # Calculates priority scores for components
    
    def __init__(self, weights_path: Optional[str] = None):
        self.weights = self._load_weights(weights_path)
        self.boost_config = self._load_boost_config()
    
    def _load_weights(self, weights_path: Optional[str] = None) -> Dict[str, float]:
        # Load scoring weights from config
        if weights_path is None:
            weights_path = Path(__file__).parent.parent / "config" / "weights.json"
        
        with open(weights_path, 'r') as f:
            return json.load(f)
    
    def _load_boost_config(self) -> Dict:
        # Load boost configuration
        config_path = Path(__file__).parent.parent / "config" / "feature_config.json"
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        return config.get('boost_conditions', {})
    
    def calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        # Calculate priority scores for all components
        df = df.copy()
        
        # Apply tiered filtering
        df = self._apply_tiered_filtering(df)
        
        # Calculate base score
        df['base_score'] = self._calculate_base_score(df)
        
        # Apply business boosts
        df['boosted_score'] = self._apply_boosts(df)
        
        # Normalize to 0-100
        df['priority_score'] = self._normalize_scores(df['boosted_score'])
        
        logger.info(f"Calculated scores for {len(df)} components")
        return df
    
    def _apply_tiered_filtering(self, df: pd.DataFrame) -> pd.DataFrame:
        # Apply pre-filtering based on availability
        # Mark unavailable items
        if all(col in df.columns for col in ['inventory', 'leadtime_weeks']):
            df['is_available'] = ~((df['inventory'] == 0) & (df['leadtime_weeks'] > 12))
        else:
            df['is_available'] = True
        
        logger.info(f"Filtered out {(~df['is_available']).sum()} unavailable items")
        return df
    
    def _calculate_base_score(self, df: pd.DataFrame) -> pd.Series:
        # Calculate weighted base score
        base_score = pd.Series(0.0, index=df.index)
        
        for feature, weight in self.weights.items():
            if feature in df.columns:
                base_score += df[feature].fillna(0) * weight
                logger.debug(f"Added {feature} with weight {weight}")
            else:
                logger.warning(f"Feature {feature} not found in dataframe")
        
        # Zero out unavailable items
        base_score[~df['is_available']] = 0
        
        return base_score
    
    def _apply_boosts(self, df: pd.DataFrame) -> pd.Series:
        # Apply multiplicative boosts based on business rules
        boosted_score = df['base_score'].copy()
        
        # Apply each boost condition
        for boost_name, boost_config in self.boost_config.items():
            condition = boost_config['condition']
            multiplier = boost_config['multiplier']
            
            # Evaluate condition
            try:
                mask = df.eval(condition)
                boost_count = mask.sum()
                if boost_count > 0:
                    boosted_score[mask] *= multiplier
                    logger.info(f"Applied {boost_name} boost to {boost_count} items")
            except Exception as e:
                logger.warning(f"Could not apply boost {boost_name}: {e}")
        
        return boosted_score
    
    def _normalize_scores(self, scores: pd.Series) -> pd.Series:
        # Normalize scores to 0-100 range
        # Only normalize non-zero scores
        non_zero_mask = scores > 0
        
        if non_zero_mask.sum() == 0:
            return pd.Series(0, index=scores.index)
        
        normalized = scores.copy()
        scaler = MinMaxScaler(feature_range=(1, 100))
        
        # Reshape for sklearn
        non_zero_scores = scores[non_zero_mask].values.reshape(-1, 1)
        normalized_values = scaler.fit_transform(non_zero_scores)
        
        normalized[non_zero_mask] = normalized_values.flatten()
        normalized[~non_zero_mask] = 0
        
        return normalized.round(2)