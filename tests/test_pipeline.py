import pytest
import pandas as pd
import tempfile
import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent))

from scripts.etl_pipeline import merge_datasets, save_results
from scripts.data_loader import DataLoader


class TestPipeline:
    
    @pytest.fixture
    def sample_panda_df(self):
        \"\"\"Create sample panda dataframe\"\"\"
        return pd.DataFrame({
            'pn': ['PN001', 'PN002', 'PN003'],
            'pn_clean': ['PN001C', 'PN002C', 'PN003C'],
            'inventory': [100, 200, 0],
            'pricing': [
                '{"pricing": [{"break": "1", "price": "10"}]}',
                '{"pricing": [{"break": "1", "price": "20"}]}',
                '{"pricing": [{"break": "1", "price": "30"}]}'
            ],
            'leadtime': ['0 Weeks', '2 Weeks', '16 Weeks'],
            'moq': [1, 10, 100],
            'source_type': ['Authorized', 'Other', 'Authorized']
        })
    
    @pytest.fixture
    def sample_demand_df(self):
        \"\"\"Create sample demand dataframe\"\"\"
        return pd.DataFrame({
            'pn': ['PN001', 'PN002', 'PN003', 'PN004'],
            'demand_all_time': [100, 50, 25, 10],
            'demand_totals': [
                '{"demand_totals": [{"demand_index": "100.0"}]}',
                '{"demand_totals": [{"demand_index": "50.0"}]}',
                '{"demand_totals": [{"demand_index": "25.0"}]}',
                '{"demand_totals": [{"demand_index": "10.0"}]}'
            ]
        })
    
    def test_merge_datasets(self, sample_panda_df, sample_demand_df):
        \"\"\"Test dataset merging\"\"\"
        merged = merge_datasets(sample_panda_df, sample_demand_df)
        
        # Check merge worked
        assert len(merged) == 3  # Should match panda_df length
        assert 'demand_all_time' in merged.columns
        assert 'demand_index' in merged.columns
        
        # Check values
        assert merged[merged['pn'] == 'PN001']['demand_all_time'].iloc[0] == 100
        assert merged[merged['pn'] == 'PN001']['demand_index'].iloc[0] == 100.0
    
    def test_deduplication(self, sample_panda_df, sample_demand_df):
        \"\"\"Test deduplication on pn_clean\"\"\"
        # Add duplicate
        duplicate_row = sample_panda_df.iloc[0:1].copy()
        duplicate_row['pn'] = ['PN001_DUP']
        sample_panda_df = pd.concat([sample_panda_df, duplicate_row])
        
        merged = merge_datasets(sample_panda_df, sample_demand_df)
        
        # Should have removed the duplicate
        assert len(merged) == 3
        assert len(merged['pn_clean'].unique()) == 3
    
    def test_save_results_csv(self, sample_panda_df):
        \"\"\"Test saving results to CSV\"\"\"
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test_output.csv"
            save_results(sample_panda_df, str(output_path), format='csv')
            
            assert output_path.exists()
            
            # Read back and verify
            loaded = pd.read_csv(output_path)
            assert len(loaded) == len(sample_panda_df)
            assert list(loaded.columns) == list(sample_panda_df.columns)
    
    def test_data_loader_json_parsing(self):
        \"\"\"Test JSON field parsing in data loader\"\"\"
        loader = DataLoader()
        
        # Create test data
        df = pd.DataFrame({
            'pricing': ['{"pricing": [{"break": "1", "price": "42.11"}]}'],
            'leadtime': ['16 Weeks, 0 Days'],
            'ordered': ['{"ordered": [{"date": "2024-01-01"}]}']
        })
        
        parsed = loader.parse_json_fields(df)
        
        assert 'first_price' in parsed.columns
        assert 'leadtime_weeks' in parsed.columns
        assert 'order_count' in parsed.columns
        
        assert parsed['first_price'].iloc[0] == 42.11
        assert parsed['leadtime_weeks'].iloc[0] == 16
        assert parsed['order_count'].iloc[0] == 1