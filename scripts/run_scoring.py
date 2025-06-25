"""Convenience script for running the scoring pipeline."""

#!/usr/bin/env python3

import subprocess
import sys
import argparse
import os
from pathlib import Path

def run_scoring_pipeline(args):
    """Run the scoring pipeline with proper environment setup."""
    
    # Set up environment variables
    env = os.environ.copy()
    
    # Ensure required environment variables are set
    if not env.get('GOOGLE_CLOUD_PROJECT'):
        print("ERROR: GOOGLE_CLOUD_PROJECT environment variable not set")
        return 1
    
    if not env.get('GOOGLE_APPLICATION_CREDENTIALS'):
        print("WARNING: GOOGLE_APPLICATION_CREDENTIALS not set, using default credentials")
    
    # Build command
    cmd = [
        sys.executable, '-m', 'src.pipeline.production_etl',
        '--environment', args.environment,
        '--log-level', args.log_level
    ]
    
    if args.limit:
        cmd.extend(['--limit', str(args.limit)])
    
    # Run the pipeline
    print(f"Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, env=env)
    
    return result.returncode

def main():
    parser = argparse.ArgumentParser(description='Run component scoring pipeline')
    parser.add_argument(
        '--environment', '-e',
        choices=['development', 'staging', 'production'],
        default='development',
        help='Environment to run in'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        help='Limit number of components (for testing)'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    # Validate environment setup
    project_root = Path(__file__).parent.parent
    if not (project_root / 'src').exists():
        print(f"ERROR: src directory not found at {project_root / 'src'}")
        return 1
    
    return run_scoring_pipeline(args)

if __name__ == "__main__":
    sys.exit(main())
