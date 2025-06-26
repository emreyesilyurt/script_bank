"""Part Priority Scoring Module.

A Python module for scoring electronic component parts based on 
availability, demand, pricing, and other business factors.
"""

from .core.scorer import PartScorer
from .core.data_loader import DataLoader
from .core.feature_engineer import FeatureEngineer
from .utils.validators import DataValidator

__version__ = "1.0.0"
__all__ = ["PartScorer", "DataLoader", "FeatureEngineer", "DataValidator", "score_parts"]

def score_parts(df, weights_config=None, feature_config=None):
    """Convenience function to score parts dataframe.
    
    Args:
        df: DataFrame with part data
        weights_config: Optional custom weights dictionary
        feature_config: Optional custom feature configuration
        
    Returns:
        DataFrame with priority_score column added
    """
    from .config.settings import get_default_config
    
    config = get_default_config()
    if weights_config:
        config['weights'].update(weights_config)
    if feature_config:
        config['features'].update(feature_config)
    
    scorer = PartScorer(config)
    return scorer.calculate_scores(df)
