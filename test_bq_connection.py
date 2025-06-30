from google.cloud import bigquery
import os

def test_bigquery_connection():
    try:
        # Check environment variables
        print("Project ID:", os.getenv('GOOGLE_CLOUD_PROJECT'))
        print("Credentials:", os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
        
        # Test BigQuery client
        client = bigquery.Client()
        print(f"Connected to project: {client.project}")
        
        # Test a simple query
        query = "SELECT 1 as test_value"
        results = client.query(query).to_dataframe()
        print("Query test successful:", results.iloc[0, 0] == 1)
        
        # Test access to your datasets
        datasets = list(client.list_datasets())
        print(f"Available datasets: {[d.dataset_id for d in datasets]}")
        
        return True
        
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_bigquery_connection()