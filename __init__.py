"""Part Priority Scoring Module."""

from .core.scorer import PartScorer
from .core.data_loader import DataLoader
from .core.feature_engineer import FeatureEngineer
from .utils.validators import DataValidator

__version__ = "1.0.0"
__all__ = ["PartScorer", "DataLoader", "FeatureEngineer", "DataValidator"]

# Convenience function for simple usage
def score_parts(df, weights_config=None):
    """Simple function to score parts dataframe."""
    from .core.scorer import PartScorer
    from .config.settings import get_default_config
    
    config = get_default_config()
    if weights_config:
        config.update(weights_config)
    
    scorer = PartScorer(config)
    return scorer.calculate_scores(df)