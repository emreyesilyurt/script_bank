"""Monitoring and alerting script for the scoring pipeline."""

import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from google.cloud import bigquery, monitoring_v3
from google.cloud.monitoring_dashboard import v1
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

class PipelineMonitor:
    """Monitor pipeline health and performance."""
    
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.bq_client = bigquery.Client(project=project_id)
        self.monitoring_client = monitoring_v3.MetricServiceClient()
        
        # Thresholds
        self.thresholds = {
            'processing_rate_min': 1000,  # components per second
            'success_rate_min': 0.95,     # 95% success rate
            'avg_score_min': 30,          # minimum average score
            'data_freshness_hours': 6     # maximum data age
        }
    
    def check_pipeline_health(self) -> Dict[str, Any]:
        """Comprehensive pipeline health check."""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'healthy',
            'checks': {}
        }
        
        # Check data freshness
        health_status['checks']['data_freshness'] = self._check_data_freshness()
        
        # Check processing metrics
        health_status['checks']['processing_metrics'] = self._check_processing_metrics()
        
        # Check score quality 
        health_status['checks']['score_quality'] = self._check_score_quality()
        
        # Check BigQuery table health
        health_status['checks']['table_health'] = self._check_table_health()
        
        # Determine overall status
        failed_checks = [name for name, check in health_status['checks'].items() 
                        if check['status'] != 'healthy']
        
        if failed_checks:
            health_status['overall_status'] = 'unhealthy'
            health_status['failed_checks'] = failed_checks
        
        return health_status
    
    def _check_data_freshness(self) -> Dict[str, Any]:
        """Check if data is fresh enough."""
        query = """
        SELECT 
            MAX(processed_at) as latest_processing,
            COUNT(*) as total_rows
        FROM `{project}.datadojo.prod.component_scores`
        WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        """.format(project=self.project_id)
        
        try:
            result = list(self.bq_client.query(query))[0]
            
            if result.latest_processing:
                age_hours = (datetime.now() - result.latest_processing).total_seconds() / 3600
                
                return {
                    'status': 'healthy' if age_hours <= self.thresholds['data_freshness_hours'] else 'unhealthy',
                    'latest_processing': result.latest_processing.isoformat(),
                    'age_hours': round(age_hours, 2),
                    'total_rows_24h': result.total_rows,
                    'threshold_hours': self.thresholds['data_freshness_hours']
                }
            else:
                return {
                    'status': 'unhealthy',
                    'message': 'No recent data found',
                    'total_rows_24h': result.total_rows
                }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error checking data freshness: {e}'
            }
    
    def _check_processing_metrics(self) -> Dict[str, Any]:
        """Check processing performance metrics."""
        query = """
        WITH processing_stats AS (
            SELECT 
                DATE(processed_at) as processing_date,
                COUNT(*) as components_processed,
                COUNT(CASE WHEN priority_score > 0 THEN 1 END) as components_scored,
                AVG(priority_score) as avg_score,
                MIN(processed_at) as start_time,
                MAX(processed_at) as end_time
            FROM `{project}.datadojo.prod.component_scores`
            WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
            GROUP BY DATE(processed_at)
        )
        SELECT 
            processing_date,
            components_processed,
            components_scored,
            avg_score,
            components_scored / components_processed as success_rate,
            components_processed / TIMESTAMP_DIFF(end_time, start_time, SECOND) as processing_rate
        FROM processing_stats
        ORDER BY processing_date DESC
        LIMIT 7
        """.format(project=self.project_id)
        
        try:
            results = list(self.bq_client.query(query))
            
            if not results:
                return {
                    'status': 'unhealthy',
                    'message': 'No processing metrics found'
                }
            
            latest = results[0]
            
            # Check thresholds
            issues = []
            if latest.success_rate < self.thresholds['success_rate_min']:
                issues.append(f"Success rate {latest.success_rate:.3f} below threshold {self.thresholds['success_rate_min']}")
            
            if latest.processing_rate < self.thresholds['processing_rate_min']:
                issues.append(f"Processing rate {latest.processing_rate:.1f} below threshold {self.thresholds['processing_rate_min']}")
            
            if latest.avg_score < self.thresholds['avg_score_min']:
                issues.append(f"Average score {latest.avg_score:.1f} below threshold {self.thresholds['avg_score_min']}")
            
            return {
                'status': 'healthy' if not issues else 'unhealthy',
                'latest_metrics': {
                    'processing_date': latest.processing_date.isoformat(),
                    'components_processed': latest.components_processed,
                    'components_scored': latest.components_scored,
                    'success_rate': round(latest.success_rate, 4),
                    'processing_rate': round(latest.processing_rate, 2),
                    'avg_score': round(latest.avg_score, 2)
                },
                'issues': issues,
                'historical_data': [
                    {
                        'date': r.processing_date.isoformat(),
                        'processed': r.components_processed,
                        'scored': r.components_scored,
                        'success_rate': round(r.success_rate, 4)
                    } for r in results
                ]
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error checking processing metrics: {e}'
            }
    
    def _check_score_quality(self) -> Dict[str, Any]:
        """Check quality of generated scores."""
        query = """
        SELECT 
            COUNT(*) as total_components,
            COUNT(CASE WHEN priority_score > 0 THEN 1 END) as scored_components,
            AVG(priority_score) as avg_score,
            STDDEV(priority_score) as score_stddev,
            MIN(priority_score) as min_score,
            MAX(priority_score) as max_score,
            
            -- Score distribution
            COUNT(CASE WHEN priority_score = 0 THEN 1 END) as zero_scores,
            COUNT(CASE WHEN priority_score > 0 AND priority_score < 25 THEN 1 END) as very_low_scores,
            COUNT(CASE WHEN priority_score >= 25 AND priority_score < 50 THEN 1 END) as low_scores,
            COUNT(CASE WHEN priority_score >= 50 AND priority_score < 75 THEN 1 END) as medium_scores,
            COUNT(CASE WHEN priority_score >= 75 AND priority_score < 90 THEN 1 END) as high_scores,
            COUNT(CASE WHEN priority_score >= 90 THEN 1 END) as very_high_scores
            
        FROM `{project}.datadojo.prod.component_scores`
        WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        """.format(project=self.project_id)
        
        try:
            result = list(self.bq_client.query(query))[0]
            
            # Calculate quality metrics
            score_coverage = result.scored_components / result.total_components if result.total_components > 0 else 0
            score_diversity = 1 - (result.zero_scores / result.total_components) if result.total_components > 0 else 0
            
            issues = []
            if score_coverage < 0.8:
                issues.append(f"Low score coverage: {score_coverage:.3f}")
            
            if result.avg_score < self.thresholds['avg_score_min']:
                issues.append(f"Low average score: {result.avg_score:.1f}")
            
            if result.score_stddev < 10:
                issues.append(f"Low score variance: {result.score_stddev:.1f}")
            
            return {
                'status': 'healthy' if not issues else 'unhealthy',
                'metrics': {
                    'total_components': result.total_components,
                    'scored_components': result.scored_components,
                    'score_coverage': round(score_coverage, 4),
                    'avg_score': round(result.avg_score, 2),
                    'score_stddev': round(result.score_stddev, 2),
                    'min_score': result.min_score,
                    'max_score': result.max_score,
                    'score_diversity': round(score_diversity, 4)
                },
                'distribution': {
                    'zero_scores': result.zero_scores,
                    'very_low_scores': result.very_low_scores,
                    'low_scores': result.low_scores,
                    'medium_scores': result.medium_scores,
                    'high_scores': result.high_scores,
                    'very_high_scores': result.very_high_scores
                },
                'issues': issues
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error checking score quality: {e}'
            }
    
    def _check_table_health(self) -> Dict[str, Any]:
        """Check BigQuery table health."""
        try:
            table_ref = self.bq_client.dataset('datadojo.prod').table('component_scores')
            table = self.bq_client.get_table(table_ref)
            
            # Check table size and modification time
            size_gb = table.num_bytes / (1024**3) if table.num_bytes else 0
            last_modified_hours = (datetime.now() - table.modified).total_seconds() / 3600 if table.modified else None
            
            issues = []
            if size_gb > 100:  # Alert if table is over 100GB
                issues.append(f"Large table size: {size_gb:.2f} GB")
            
            if last_modified_hours and last_modified_hours > 24:
                issues.append(f"Table not modified in {last_modified_hours:.1f} hours")
            
            return {
                'status': 'healthy' if not issues else 'warning',
                'metrics': {
                    'num_rows': table.num_rows,
                    'size_gb': round(size_gb, 2),
                    'last_modified': table.modified.isoformat() if table.modified else None,
                    'last_modified_hours_ago': round(last_modified_hours, 1) if last_modified_hours else None
                },
                'issues': issues
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Error checking table health: {e}'
            }
    
    def send_alert(self, health_status: Dict[str, Any]):
        """Send alert if pipeline is unhealthy."""
        if health_status['overall_status'] != 'healthy':
            logger.error(f"Pipeline health alert: {json.dumps(health_status, indent=2)}")
            
            # Here you would integrate with your alerting system
            # Examples: Slack, PagerDuty, email, etc.
            self._send_email_alert(health_status)
    
    def _send_email_alert(self, health_status: Dict[str, Any]):
        """Send email alert (placeholder implementation)."""
        # This is a basic example - you should configure your actual email settings
        try:
            subject = f"Component Scoring Pipeline Alert - {health_status['overall_status'].upper()}"
            body = f"""
            Pipeline Health Status: {health_status['overall_status']}
            Timestamp: {health_status['timestamp']}
            
            Failed Checks: {', '.join(health_status.get('failed_checks', []))}
            
            Detailed Status:
            {json.dumps(health_status, indent=2)}
            """
            
            logger.info(f"Would send email alert: {subject}")
            # Actual email sending code would go here
            
        except Exception as e:
            logger.error(f"Error sending email alert: {e}")

def main():
    """Main monitoring function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor component scoring pipeline')
    parser.add_argument('--project-id', required=True, help='Google Cloud Project ID')
    parser.add_argument('--continuous', action='store_true', help='Run continuous monitoring')
    parser.add_argument('--interval', type=int, default=300, help='Monitoring interval in seconds')
    
    args = parser.parse_args()
    
    monitor = PipelineMonitor(args.project_id)
    
    if args.continuous:
        logger.info(f"Starting continuous monitoring with {args.interval}s interval")
        while True:
            try:
                health_status = monitor.check_pipeline_health()
                print(json.dumps(health_status, indent=2))
                
                if health_status['overall_status'] != 'healthy':
                    monitor.send_alert(health_status)
                
                time.sleep(args.interval)
                
            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(args.interval)
    else:
        # Single check
        health_status = monitor.check_pipeline_health()
        print(json.dumps(health_status, indent=2))
        
        if health_status['overall_status'] != 'healthy':
            monitor.send_alert(health_status)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()