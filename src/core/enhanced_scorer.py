# src/core/enhanced_scorer.py
"""Enhanced part scorer with advanced features and monitoring."""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import RobustScaler, MinMaxScaler
import logging
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class EnhancedPartScorer:
    """Production-ready part scorer with advanced features."""
    
    def __init__(self, settings):
        self.settings = settings
        self.weights = settings.get_weights()
        self.feature_config = settings.feature_config
        self.boost_config = self.feature_config.get('boost_conditions', {})
        
        # Initialize scalers
        self.robust_scaler = RobustScaler()
        self.final_scaler = MinMaxScaler(feature_range=(0, 100))
        
        # Metrics tracking
        self.scoring_metrics = {
            'total_processed': 0,
            'total_scored': 0,
            'processing_time': 0,
            'score_distribution': {}
        }
    
    def calculate_scores(self, df: pd.DataFrame, batch_id: str = None) -> pd.DataFrame:
        """Calculate priority scores with comprehensive monitoring."""
        start_time = time.time()
        logger.info(f"Starting score calculation for batch {batch_id} with {len(df)} parts")
        
        try:
            # Create working copy
            scored_df = df.copy()
            
            # Step 1: Apply tiered filtering
            scored_df = self._apply_availability_filter(scored_df)
            available_count = scored_df['is_available'].sum()
            logger.info(f"Available parts after filtering: {available_count}/{len(scored_df)}")
            
            # Step 2: Feature engineering
            scored_df = self._engineer_features(scored_df)
            
            # Step 3: Calculate base scores
            scored_df['base_score'] = self._calculate_weighted_score(scored_df)
            
            # Step 4: Apply business boosts
            scored_df['boosted_score'] = self._apply_business_boosts(scored_df)
            
            # Step 5: Normalize to final scores
            scored_df['priority_score'] = self._normalize_final_scores(scored_df['boosted_score'])
            
            # Step 6: Add percentile rankings
            scored_df['score_percentile'] = self._calculate_percentiles(scored_df['priority_score'])
            
            # Step 7: Add metadata
            scored_df['scored_at'] = datetime.now()
            scored_df['batch_id'] = batch_id
            
            # Update metrics
            processing_time = time.time() - start_time
            self._update_metrics(scored_df, processing_time)
            
            logger.info(f"Score calculation completed in {processing_time:.2f}s")
            self._log_score_distribution(scored_df)
            
            return scored_df
            
        except Exception as e:
            logger.error(f"Error in score calculation: {e}")
            raise
    
    def _apply_availability_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply intelligent availability filtering."""
        df = df.copy()
        
        # Basic availability check
        if all(col in df.columns for col in ['inventory', 'leadtime_weeks']):
            df['is_available'] = ~((df['inventory'] == 0) & (df['leadtime_weeks'] > 12))
        else:
            df['is_available'] = True
        
        # Enhanced availability scoring
        df['availability_tier'] = 0
        
        # Tier 1: Immediate availability
        immediate_mask = (df['inventory'] > 0) & (df['leadtime_weeks'] == 0)
        df.loc[immediate_mask, 'availability_tier'] = 1
        
        # Tier 2: Short-term availability  
        short_term_mask = (df['inventory'] > 0) & (df['leadtime_weeks'] <= 4)
        df.loc[short_term_mask & ~immediate_mask, 'availability_tier'] = 2
        
        # Tier 3: Medium-term availability
        medium_term_mask = (df['inventory'] > 0) & (df['leadtime_weeks'] <= 12)
        df.loc[medium_term_mask & ~short_term_mask, 'availability_tier'] = 3
        
        # Tier 4: Long-term or special order
        df.loc[df['availability_tier'] == 0, 'availability_tier'] = 4
        
        return df
    
    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Advanced feature engineering with error handling."""
        df = df.copy()
        
        # Log transformations
        log_features = self.feature_config['features']['log_transforms']
        for feature in log_features:
            col_name = 'first_price' if feature == 'price' else feature
            if col_name in df.columns:
                df[f'log_{feature}'] = np.log1p(df[col_name].fillna(0).clip(lower=0))
        
        # Inverse transformations
        inverse_features = self.feature_config['features']['inverse_transforms']
        for feature in inverse_features:
            col_name = 'first_price' if feature == 'price' else feature
            if col_name in df.columns:
                df[f'inv_{feature}'] = 1 / (1 + df[col_name].fillna(0).clip(lower=0))
        
        # Binary features
        binary_features = self.feature_config['features']['binary_features']
        for feature_name, feature_config in binary_features.items():
            try:
                if feature_name == 'is_authorized':
                    df[feature_name] = (df['source_type'] == 'Authorized').astype(int)
                elif feature_name == 'has_datasheet':
                    df[feature_name] = df['datasheet'].notna().astype(int)
                elif feature_name == 'in_stock':
                    df[feature_name] = (df['inventory'] > 0).astype(int)
                elif feature_name == 'immediate_availability':
                    df[feature_name] = (df['leadtime_weeks'] == 0).astype(int)
            except Exception as e:
                logger.warning(f"Error creating binary feature {feature_name}: {e}")
                df[feature_name] = 0
        
        # Composite features
        df = self._create_composite_features(df)
        
        # Robust scaling of continuous features
        scale_features = [col for col in df.columns if col.startswith(('log_', 'inv_', 'availability_', 'demand_'))]
        scale_features = [f for f in scale_features if f in df.columns]
        
        if scale_features:
            try:
                # Handle missing values before scaling
                df[scale_features] = df[scale_features].fillna(0)
                
                # Apply robust scaling
                scaled_values = self.robust_scaler.fit_transform(df[scale_features])
                df[scale_features] = scaled_values
                
            except Exception as e:
                logger.error(f"Error in feature scaling: {e}")
                # Continue with unscaled features
        
        return df
    
    def _create_composite_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create composite features from multiple signals."""
        
        # Availability score
        if all(col in df.columns for col in ['inventory', 'moq']):
            try:
                in_stock_score = df.get('in_stock', 0) * 0.5
                immediate_score = df.get('immediate_availability', 0) * 0.3
                inventory_ratio = (df['inventory'] / df['moq'].clip(lower=1)).clip(upper=10) * 0.2
                
                df['availability_score'] = (in_stock_score + immediate_score + inventory_ratio).clip(0, 2)
            except Exception as e:
                logger.warning(f"Error creating availability score: {e}")
                df['availability_score'] = 0
        
        # Demand score normalization
        if 'demand_all_time' in df.columns:
            df['demand_score'] = df['demand_all_time'].fillna(0)
        else:
            df['demand_score'] = 0
        
        # Economic attractiveness score
        if all(col in df.columns for col in ['inv_price', 'inv_moq']):
            df['economic_score'] = df['inv_price'] * 0.6 + df['inv_moq'] * 0.4
        else:
            df['economic_score'] = 0
        
        return df
    
    def _calculate_weighted_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate weighted base score with proper error handling."""
        base_score = pd.Series(0.0, index=df.index)
        
        for feature, weight in self.weights.items():
            if feature in df.columns:
                feature_values = df[feature].fillna(0)
                weighted_contribution = feature_values * weight
                base_score += weighted_contribution
                
                logger.debug(f"Added {feature} (weight={weight:.3f}, "
                           f"mean={feature_values.mean():.3f}, "
                           f"contribution={weighted_contribution.mean():.3f})")
            else:
                logger.warning(f"Weight feature {feature} not found in dataframe")
        
        # Zero out unavailable items
        if 'is_available' in df.columns:
            base_score[~df['is_available']] = 0
        
        return base_score
    
    def _apply_business_boosts(self, df: pd.DataFrame) -> pd.Series:
        """Apply business rule boosts with comprehensive logging."""
        boosted_score = df['base_score'].copy()
        boost_summary = {}
        
        for boost_name, boost_config in self.boost_config.items():
            try:
                condition = boost_config['condition']
                multiplier = boost_config['multiplier']
                
                # Evaluate condition safely
                mask = self._safe_eval_condition(df, condition)
                boost_count = mask.sum() if mask is not None else 0
                
                if boost_count > 0:
                    boosted_score[mask] *= multiplier
                    boost_summary[boost_name] = boost_count
                    logger.info(f"Applied {boost_name} boost (x{multiplier}) to {boost_count} parts")
                
            except Exception as e:
                logger.warning(f"Could not apply boost {boost_name}: {e}")
        
        # Log overall boost effectiveness
        if boost_summary:
            total_boosted = sum(boost_summary.values())
            logger.info(f"Total parts boosted: {total_boosted}/{len(df)} "
                       f"({total_boosted/len(df)*100:.1f}%)")
        
        return boosted_score
    
    def _safe_eval_condition(self, df: pd.DataFrame, condition: str) -> Optional[pd.Series]:
        """Safely evaluate boost conditions."""
        try:
            # Simple condition parsing to avoid eval() security issues
            if "inventory >= 10 * moq" in condition:
                return (df['inventory'] >= 10 * df['moq'])
            elif "leadtime_weeks == 0" in condition:
                return (df['leadtime_weeks'] == 0)
            elif "source_type == 'Authorized'" in condition:
                return (df['source_type'] == 'Authorized')
            elif "has_datasheet == 1" in condition:
                return (df.get('has_datasheet', 0) == 1)
            elif "demand_all_time > 100 AND first_price < 10" in condition:
                return (df['demand_all_time'] > 100) & (df['first_price'] < 10)
            else:
                logger.warning(f"Unknown condition format: {condition}")
                return None
        except Exception as e:
            logger.error(f"Error evaluating condition '{condition}': {e}")
            return None
    
    def _normalize_final_scores(self, scores: pd.Series) -> pd.Series:
        """Normalize scores to 0-100 range with proper handling of edge cases."""
        # Handle edge cases
        if len(scores) == 0:
            return pd.Series(dtype=float)
        
        if scores.max() == scores.min():
            # All scores are the same
            return pd.Series(50.0, index=scores.index)
        
        # Separate zero and non-zero scores
        non_zero_mask = scores > 0
        
        if non_zero_mask.sum() == 0:
            # All scores are zero
            return pd.Series(0.0, index=scores.index)
        
        # Normalize non-zero scores to 1-100 range
        normalized = scores.copy()
        non_zero_scores = scores[non_zero_mask].values.reshape(-1, 1)
        
        try:
            scaled_scores = self.final_scaler.fit_transform(non_zero_scores)
            normalized[non_zero_mask] = scaled_scores.flatten()
            
            # Ensure minimum score of 1 for valid parts
            normalized[non_zero_mask] = normalized[non_zero_mask].clip(lower=1)
            
        except Exception as e:
            logger.error(f"Error in final score normalization: {e}")
            # Fallback to simple min-max scaling
            min_score = non_zero_scores.min()
            max_score = non_zero_scores.max()
            normalized[non_zero_mask] = 1 + (scores[non_zero_mask] - min_score) / (max_score - min_score) * 99
        
        return normalized.round(2)
    
    def _calculate_percentiles(self, scores: pd.Series) -> pd.Series:
        """Calculate percentile rankings for scores."""
        try:
            return scores.rank(pct=True) * 100
        except Exception as e:
            logger.error(f"Error calculating percentiles: {e}")
            return pd.Series(50.0, index=scores.index)
    
    def _update_metrics(self, df: pd.DataFrame, processing_time: float):
        """Update scoring metrics for monitoring."""
        self.scoring_metrics['total_processed'] += len(df)
        self.scoring_metrics['total_scored'] += (df['priority_score'] > 0).sum()
        self.scoring_metrics['processing_time'] += processing_time
        
        # Update score distribution
        score_bins = pd.cut(df['priority_score'], bins=[0, 25, 50, 75, 90, 100], 
                           labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'])
        distribution = score_bins.value_counts().to_dict()
        
        for category, count in distribution.items():
            if category in self.scoring_metrics['score_distribution']:
                self.scoring_metrics['score_distribution'][category] += count
            else:
                self.scoring_metrics['score_distribution'][category] = count
    
    def _log_score_distribution(self, df: pd.DataFrame):
        """Log score distribution for monitoring."""
        scores = df['priority_score']
        
        logger.info(f"Score Distribution - "
                   f"Mean: {scores.mean():.2f}, "
                   f"Median: {scores.median():.2f}, "
                   f"Std: {scores.std():.2f}")
        
        logger.info(f"Score Ranges - "
                   f"High (>90): {(scores > 90).sum()}, "
                   f"Medium (50-90): {((scores >= 50) & (scores <= 90)).sum()}, "
                   f"Low (<50): {(scores < 50).sum()}")
    
    def get_metrics_summary(self) -> Dict:
        """Get comprehensive metrics summary."""
        if self.scoring_metrics['total_processed'] > 0:
            processing_rate = self.scoring_metrics['total_processed'] / self.scoring_metrics['processing_time']
            success_rate = self.scoring_metrics['total_scored'] / self.scoring_metrics['total_processed']
        else:
            processing_rate = 0
            success_rate = 0
        
        return {
            'total_processed': self.scoring_metrics['total_processed'],
            'total_scored': self.scoring_metrics['total_scored'],
            'processing_rate_per_second': round(processing_rate, 2),
            'success_rate': round(success_rate, 4),
            'total_processing_time': round(self.scoring_metrics['processing_time'], 2),
            'score_distribution': self.scoring_metrics['score_distribution']
        }

# Legacy alias for backward compatibility
EnhancedComponentScorer = EnhancedPartScorer