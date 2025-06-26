"""Part Priority Scoring Module."""

from .core.scorer import PartScorer
from .core.data_loader import DataLoader
from .core.feature_engineer import FeatureEngineer

__version__ = "1.0.0"
__all__ = ["PartScorer", "DataLoader", "FeatureEngineer", "score_parts"]

def score_parts(df, weights_config=None, feature_config=None):
    """Convenience function to score parts dataframe."""
    from .config.settings import get_default_config
    
    config = get_default_config()
    if weights_config:
        config['weights'].update(weights_config)
    if feature_config:
        config['features'].update(feature_config)
    
    scorer = PartScorer(config)
    return scorer.calculate_scores(df)
