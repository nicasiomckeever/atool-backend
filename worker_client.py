"""
Worker Client - Routes queue operations through Edge Function
This module handles communication with the Supabase Edge Function
that distributes load across multiple worker Supabase projects.
"""

import os
import requests
from typing import Dict, Any, Optional, List, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class WorkerClient:
    """Client for routing queue operations through Edge Function"""
    
    def __init__(self, edge_function_url: Optional[str] = None, supabase_key: Optional[str] = None):
        """
        Initialize Worker Client
        
        Args:
            edge_function_url: URL of the edge function (e.g., https://xxx.supabase.co/functions/v1/route-queue)
            supabase_key: Supabase key for authentication (preferably service role key for backend)
        """
        self.edge_function_url = edge_function_url or os.getenv('EDGE_FUNCTION_URL')
        # Use service role key for backend calls, fallback to anon key
        self.supabase_key = supabase_key or os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_KEY')
        
        if not self.edge_function_url:
            raise ValueError("Edge function URL not provided. Set EDGE_FUNCTION_URL environment variable.")
        
        if not self.supabase_key:
            raise ValueError("Supabase key not provided. Set SUPABASE_KEY environment variable.")
        
        self.headers = {
            'Content-Type': 'application/json',
            'apikey': self.supabase_key,
            'Authorization': f'Bearer {self.supabase_key}'
        }
        
        logger.info(f"Worker client initialized with edge function: {self.edge_function_url}")
    
    def _make_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make request to edge function with error handling
        
        Args:
            payload: Request payload containing operation, table, data, filters
            
        Returns:
            Response from edge function
        """
        try:
            response = requests.post(
                self.edge_function_url,
                json=payload,
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            result = response.json()
            
            if not result.get('success'):
                raise Exception(f"Edge function returned error: {result.get('error')}")
            
            logger.info(f"Edge function success. Worker used: {result.get('worker')}")
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Edge function request failed: {e}")
            raise Exception(f"Failed to communicate with edge function: {e}")
    
    def insert(self, table: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
        """
        Insert data into worker database via edge function
        
        Args:
            table: Table name (e.g., 'priority1_queue')
            data: Data to insert (single dict or list of dicts)
            
        Returns:
            Inserted data
        """
        payload = {
            'operation': 'insert',
            'table': table,
            'data': data
        }
        
        result = self._make_request(payload)
        return result.get('data')
    
    def update(self, table: str, data: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update data in worker database via edge function
        
        Args:
            table: Table name
            data: Data to update
            filters: Filter conditions (e.g., {'eq': {'id': 123}})
            
        Returns:
            Updated data
        """
        payload = {
            'operation': 'update',
            'table': table,
            'data': data,
            'filters': filters
        }
        
        result = self._make_request(payload)
        return result.get('data')
    
    def delete(self, table: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Delete data from worker database via edge function
        
        Args:
            table: Table name
            filters: Filter conditions
            
        Returns:
            Deleted data
        """
        payload = {
            'operation': 'delete',
            'table': table,
            'filters': filters
        }
        
        result = self._make_request(payload)
        return result.get('data')
    
    def select(self, table: str, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Select data from worker database via edge function
        
        Args:
            table: Table name
            filters: Optional filter conditions
            
        Returns:
            Selected data
        """
        payload = {
            'operation': 'select',
            'table': table
        }
        
        if filters:
            payload['filters'] = filters
        
        result = self._make_request(payload)
        data = result.get('data', [])
        
        # Edge function returns Supabase response object, extract the actual data
        if isinstance(data, dict) and 'data' in data:
            return data.get('data', [])
        
        return data if isinstance(data, list) else []
    
    # Priority queue specific methods
    
    def add_to_queue(self, priority: int, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add job to priority queue
        
        Args:
            priority: Priority level (1, 2, or 3)
            job_data: Job data to insert
            
        Returns:
            Inserted job data
        """
        table_name = f"priority{priority}_queue"
        
        # Ensure timestamp fields
        if 'created_at' not in job_data:
            job_data['created_at'] = datetime.utcnow().isoformat()
        
        if 'updated_at' not in job_data:
            job_data['updated_at'] = datetime.utcnow().isoformat()
        
        return self.insert(table_name, job_data)
    
    def get_next_job(self, priority: int) -> Optional[Dict[str, Any]]:
        """
        Get next job from priority queue
        
        Args:
            priority: Priority level (1, 2, or 3)
            
        Returns:
            Next job or None if queue is empty
        """
        table_name = f"priority{priority}_queue"
        
        filters = {
            'eq': {'status': 'pending'},
            'order': {'column': 'created_at', 'ascending': True},
            'limit': 1
        }
        
        jobs = self.select(table_name, filters)
        return jobs[0] if jobs else None
    
    def update_job_status(self, priority: int, job_id: str, status: str, 
                         additional_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Update job status in queue
        
        Args:
            priority: Priority level
            job_id: Job ID
            status: New status
            additional_data: Additional data to update
            
        Returns:
            Updated job data
        """
        table_name = f"priority{priority}_queue"
        
        update_data = {
            'status': status,
            'updated_at': datetime.utcnow().isoformat()
        }
        
        if additional_data:
            update_data.update(additional_data)
        
        filters = {
            'eq': {'id': job_id}
        }
        
        return self.update(table_name, update_data, filters)
    
    def clear_queue(self, priority: int) -> Dict[str, Any]:
        """
        Clear all jobs from a priority queue (use with caution!)
        
        Args:
            priority: Priority level
            
        Returns:
            Deletion result
        """
        table_name = f"priority{priority}_queue"
        
        # Delete all records (no filter means all)
        filters = {}
        
        return self.delete(table_name, filters)


# Singleton instance
_worker_client: Optional[WorkerClient] = None


def get_worker_client() -> WorkerClient:
    """
    Get or create singleton Worker Client instance
    
    Returns:
        WorkerClient instance
    """
    global _worker_client
    
    if _worker_client is None:
        _worker_client = WorkerClient()
    
    return _worker_client


# Example usage
if __name__ == "__main__":
    # Test the worker client
    import os
    import uuid
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        client = get_worker_client()
        
        # Test insert
        print("Testing insert...")
        test_data = {
            'user_id': str(uuid.uuid4()),  # Generate proper UUID
            'job_id': str(uuid.uuid4()),   # Generate proper UUID
            'request_payload': {
                'prompt': 'Test prompt from edge function',
                'model': 'flux-dev',
                'aspect_ratio': '1:1'
            },
            'processed': False
        }
        job = client.insert('priority1_queue', test_data)
        print(f"Inserted job: {job}")
        
        # Test select (without filters first)
        print("\nTesting select...")
        try:
            jobs = client.select('priority1_queue')
            print(f"✅ Found {len(jobs) if jobs else 0} jobs in queue")
            if jobs:
                print(f"First job ID: {jobs[0].get('job_id', 'N/A')}")
        except Exception as select_error:
            print(f"⚠️ Select failed: {select_error}")
        
        print("\n✅ All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
