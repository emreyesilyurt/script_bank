import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from scripts.feature_engineering import FeatureEngineer
from scripts.data_loader import extract_first_price, extract_leadtime_weeks, extract_order_count


class TestFeatureEngineering:
    
    @pytest.fixture
    def sample_data(self):
        # Create sample data for testing
        return pd.DataFrame({
            'inventory': [0, 100, 1000, 10000],
            'first_price': [10.5, 25.0, 50.0, 100.0],
            'moq': [1, 10, 100, 1000],
            'leadtime_weeks': [0, 1, 8, 16],
            'source_type': ['Authorized', 'Other', 'Authorized', 'Other'],
            'datasheet': ['url1', None, 'url3', None],
            'demand_all_time': [10, 20, 30, 40]
        })
    
    @pytest.fixture
    def feature_config(self):
        # Feature configuration for testing
        return {
            'log_features': ['inventory', 'price', 'moq'],
            'inverse_features': ['leadtime_weeks', 'moq', 'price'],
            'binary_features': {
                'is_authorized': "source_type == 'Authorized'",
                'has_datasheet': "datasheet is not null"
            }
        }
    
    def test_log_transforms(self, sample_data, feature_config):
        # Test logarithmic transformations
        engineer = FeatureEngineer(feature_config)
        result = engineer._apply_log_transforms(sample_data)
        
        # Check log transforms exist
        assert 'log_inventory' in result.columns
        assert 'log_price' in result.columns
        assert 'log_moq' in result.columns
        
        # Check values
        assert result['log_inventory'].iloc[0] == 0  # log1p(0) = 0
        assert result['log_inventory'].iloc[1] == pytest.approx(np.log1p(100))
    
    def test_inverse_transforms(self, sample_data, feature_config):
        # Test inverse transformations
        engineer = FeatureEngineer(feature_config)
        result = engineer._apply_inverse_transforms(sample_data)
        
        # Check inverse transforms exist
        assert 'inv_leadtime_weeks' in result.columns
        assert 'inv_moq' in result.columns
        assert 'inv_price' in result.columns
        
        # Check values - lower input should give higher output
        assert result['inv_leadtime_weeks'].iloc[0] > result['inv_leadtime_weeks'].iloc[3]
        assert result['inv_moq'].iloc[0] > result['inv_moq'].iloc[3]
    
    def test_binary_features(self, sample_data, feature_config):
        # Test binary feature creation
        engineer = FeatureEngineer(feature_config)
        result = engineer._create_binary_features(sample_data)
        
        # Check binary features exist
        assert 'is_authorized' in result.columns
        assert 'has_datasheet' in result.columns
        assert 'in_stock' in result.columns
        
        # Check values
        assert result['is_authorized'].iloc[0] == 1
        assert result['is_authorized'].iloc[1] == 0
        assert result['has_datasheet'].iloc[0] == 1
        assert result['has_datasheet'].iloc[1] == 0
        assert result['in_stock'].iloc[0] == 0
        assert result['in_stock'].iloc[1] == 1
    
    def test_availability_score(self, sample_data, feature_config):
        # Test composite availability score
        engineer = FeatureEngineer(feature_config)
        
        # Add required binary features first
        sample_data = engineer._create_binary_features(sample_data)
        result = engineer._create_composite_features(sample_data)
        
        assert 'availability_score' in result.columns
        
        # Row 0: out of stock, not immediate
        assert result['availability_score'].iloc[0] == 0
        
        # Row 1: in stock, not immediate, inventory/moq = 100/10 = 10
        expected_score = 1 * 0.5 + 0 * 0.3 + 10 * 0.2
        assert result['availability_score'].iloc[1] == pytest.approx(expected_score)


class TestDataParsing:
    
    def test_extract_first_price(self):
        # Test price extraction from JSON
        pricing_json = '''{"pricing": [
            {"break": "1", "price": "42.11"},
            {"break": "10", "price": "40.00"}
        ]}'''
        
        assert extract_first_price(pricing_json) == 42.11
        assert extract_first_price("invalid") is None
        assert extract_first_price("{}") is None
    
    def test_extract_leadtime_weeks(self):
        # Test leadtime extraction
        assert extract_leadtime_weeks("16 Weeks, 0 Days") == 16
        assert extract_leadtime_weeks("1 Week") == 1
        assert extract_leadtime_weeks("Available Now") is None
        assert extract_leadtime_weeks(None) is None
    
    def test_extract_order_count(self):
        # Test order count extraction
        ordered_json = '''{"ordered": [
            {"date": "2024-01-01", "qty": 10},
            {"date": "2024-02-01", "qty": 20}
        ]}'''
        
        assert extract_order_count(ordered_json) == 2
        assert extract_order_count('{"ordered": []}') == 0
        assert extract_order_count("invalid") == 0