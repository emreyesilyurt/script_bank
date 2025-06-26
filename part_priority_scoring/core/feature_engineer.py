"""Feature engineering for part scoring."""

import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import RobustScaler
from typing import Dict, Any

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Create and transform features for part scoring."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize feature engineer.
        
        Args:
            config: Feature engineering configuration
        """
        self.config = config or {}
        self.scaler = RobustScaler()
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform dataframe with engineered features.
        
        Args:
            df: Input dataframe
            
        Returns:
            DataFrame with engineered features
        """
        df = df.copy()
        
        # Create log features
        df = self._create_log_features(df)
        
        # Create inverse features  
        df = self._create_inverse_features(df)
        
        # Create binary features
        df = self._create_binary_features(df)
        
        # Create composite features
        df = self._create_composite_features(df)
        
        # Scale features
        df = self._scale_features(df)
        
        return df
    
    def _create_log_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create logarithmic transformations."""
        log_features = self.config.get('log_transforms', ['inventory', 'first_price', 'moq'])
        
        for feature in log_features:
            if feature in df.columns:
                df[f'log_{feature}'] = np.log1p(df[feature].fillna(0).clip(lower=0))
        
        return df
    
    def _create_inverse_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create inverse transformations for 'lower is better' metrics."""
        inverse_features = self.config.get('inverse_transforms', ['leadtime_weeks', 'first_price', 'moq'])
        
        for feature in inverse_features:
            if feature in df.columns:
                df[f'inv_{feature}'] = 1 / (1 + df[feature].fillna(0).clip(lower=0))
        
        return df
    
    def _create_binary_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create binary indicator features."""
        
        # Authorized source
        if 'source_type' in df.columns:
            df['is_authorized'] = (df['source_type'] == 'Authorized').astype(int)
        
        # Has datasheet
        if 'datasheet' in df.columns:
            df['has_datasheet'] = df['datasheet'].notna().astype(int)
        
        # In stock
        if 'inventory' in df.columns:
            df['in_stock'] = (df['inventory'] > 0).astype(int)
        
        # Immediate availability
        if 'leadtime_weeks' in df.columns:
            df['immediate_availability'] = (df['leadtime_weeks'] == 0).astype(int)
        
        return df
    
    def _create_composite_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create composite features from multiple signals."""
        
        # Availability score
        if all(col in df.columns for col in ['inventory', 'moq']):
            in_stock_score = df.get('in_stock', 0) * 0.5
            immediate_score = df.get('immediate_availability', 0) * 0.3
            inventory_ratio = (df['inventory'] / df['moq'].clip(lower=1)).clip(upper=10) * 0.2
            
            df['availability_score'] = (in_stock_score + immediate_score + inventory_ratio).clip(0, 2)
        
        # Demand score
        if 'demand_all_time' in df.columns:
            df['demand_score'] = df['demand_all_time'].fillna(0)
        
        return df
    
    def _scale_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply robust scaling to continuous features."""
        
        # Features to scale
        scale_features = [col for col in df.columns 
                         if col.startswith(('log_', 'inv_', 'availability_', 'demand_'))]
        scale_features = [f for f in scale_features if f in df.columns]
        
        if scale_features:
            try:
                # Fill missing values
                df[scale_features] = df[scale_features].fillna(0)
                
                # Apply scaling
                scaled_values = self.scaler.fit_transform(df[scale_features])
                df[scale_features] = scaled_values
                
                logger.info(f"Scaled {len(scale_features)} features")
            except Exception as e:
                logger.warning(f"Error in feature scaling: {e}")
        
        return df