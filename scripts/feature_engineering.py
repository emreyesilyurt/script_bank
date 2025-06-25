import numpy as np
import pandas as pd
import logging
from sklearn.preprocessing import RobustScaler, MinMaxScaler
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class FeatureEngineer:
    \"\"\"Handles all feature engineering transformations\"\"\"
    
    def __init__(self, config: Dict):
        self.config = config
        self.scalers = {}
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Apply all feature transformations\"\"\"
        df = df.copy()
        
        # Apply log transformations
        df = self._apply_log_transforms(df)
        
        # Apply inverse transformations
        df = self._apply_inverse_transforms(df)
        
        # Create binary features
        df = self._create_binary_features(df)
        
        # Create composite features
        df = self._create_composite_features(df)
        
        # Apply scaling
        df = self._apply_scaling(df)
        
        # Handle missing values
        df = self._handle_missing_values(df)
        
        return df
    
    def _apply_log_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Apply log1p transformation to specified features\"\"\"
        log_features = self.config.get('log_features', [])
        
        for feature in log_features:
            if feature in df.columns:
                # Use first_price for price feature
                col_name = 'first_price' if feature == 'price' else feature
                if col_name in df.columns:
                    df[f'log_{feature}'] = np.log1p(df[col_name].fillna(0))
                    logger.info(f"Applied log transform to {feature}")
        
        return df
    
    def _apply_inverse_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Apply inverse transformation for 'lower is better' metrics\"\"\"
        inverse_features = self.config.get('inverse_features', [])
        
        for feature in inverse_features:
            if feature in df.columns or feature == 'price':
                # Handle special cases
                if feature == 'price':
                    col_name = 'first_price'
                else:
                    col_name = feature
                
                if col_name in df.columns:
                    df[f'inv_{feature}'] = 1 / (1 + df[col_name].fillna(0))
                    logger.info(f"Applied inverse transform to {feature}")
        
        return df
    
    def _create_binary_features(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Create binary indicator features\"\"\"
        binary_features = self.config.get('binary_features', {})
        
        # Simple binary features
        if 'source_type' in df.columns:
            df['is_authorized'] = (df['source_type'] == 'Authorized').astype(int)
        
        if 'datasheet' in df.columns:
            df['has_datasheet'] = df['datasheet'].notna().astype(int)
        
        if 'inventory' in df.columns:
            df['in_stock'] = (df['inventory'] > 0).astype(int)
        
        if 'leadtime_weeks' in df.columns:
            df['immediate_availability'] = (df['leadtime_weeks'] == 0).astype(int)
        
        logger.info("Created binary features")
        return df
    
    def _create_composite_features(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Create composite features\"\"\"
        
        # Availability score
        if all(col in df.columns for col in ['in_stock', 'immediate_availability', 'inventory', 'moq']):
            df['availability_score'] = (
                df['in_stock'] * 0.5 + 
                df['immediate_availability'] * 0.3 + 
                (df['inventory'] / df['moq'].clip(lower=1)) * 0.2
            )
            
            # Clip to reasonable range
            df['availability_score'] = df['availability_score'].clip(0, 2)
            logger.info("Created availability score")
        
        # Demand score (normalized demand_all_time or demand_index)
        if 'demand_all_time' in df.columns:
            df['demand_score'] = df['demand_all_time'].fillna(0)
            # Normalize later with other features
        
        return df
    
    def _apply_scaling(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Apply robust scaling to continuous features\"\"\"
        
        # Features to scale
        scale_features = [
            'log_inventory', 'log_price', 'log_moq',
            'inv_leadtime_weeks', 'inv_price', 'inv_moq',
            'availability_score', 'demand_score'
        ]
        
        # Only scale features that exist
        scale_features = [f for f in scale_features if f in df.columns]
        
        if scale_features:
            scaler = RobustScaler()
            df[scale_features] = scaler.fit_transform(df[scale_features])
            self.scalers['robust'] = scaler
            logger.info(f"Applied robust scaling to {len(scale_features)} features")
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        \"\"\"Handle missing values with appropriate strategies\"\"\"
        
        # Fill binary features with 0
        binary_cols = ['is_authorized', 'has_datasheet', 'in_stock', 'immediate_availability']
        for col in binary_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        # Fill numeric features with 0 after transformation
        numeric_cols = [col for col in df.columns if col.startswith(('log_', 'inv_', 'availability_', 'demand_'))]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        logger.info("Handled missing values")
        return df