"""Tests for data loader functionality."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
from part_priority_scoring import DataLoader

class TestDataLoader:
    
    def test_init_without_project_id(self):
        """Test initialization without project ID."""
        loader = DataLoader()
        assert loader.client is None
        assert loader.dataset == 'datadojo.part_priority_scoring'
    
    def test_init_with_project_id(self):
        """Test initialization with project ID."""
        with patch('part_priority_scoring.core.data_loader.bigquery.Client') as mock_client:
            loader = DataLoader(project_id='test-project')
            assert loader.project_id == 'test-project'
            mock_client.assert_called_once_with(project='test-project')
    
    def test_load_sample_data_no_client(self):
        """Test load_sample_data without client raises error."""
        loader = DataLoader()
        
        with pytest.raises(ValueError, match="BigQuery client not initialized"):
            loader.load_sample_data()
    
    @patch('part_priority_scoring.core.data_loader.bigquery.Client')
    def test_load_sample_data_success(self, mock_client_class):
        """Test successful data loading."""
        # Mock the client and query result
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Mock query result
        mock_result = pd.DataFrame({
            'pn': ['PART001', 'PART002'],
            'inventory': [100, 50],
            'demand_all_time': [500, 200]
        })
        mock_client.query.return_value.to_dataframe.return_value = mock_result
        
        loader = DataLoader(project_id='test-project')
        result = loader.load_sample_data(limit=10)
        
        assert len(result) == 2
        assert 'pn' in result.columns
        assert 'inventory' in result.columns