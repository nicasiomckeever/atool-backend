"""
Shared Supabase Realtime Connection Manager

Provides a SINGLE global WebSocket connection to Supabase Realtime
that all SSE clients share, eliminating the need for per-user connections.

Architecture:
- ONE background thread with async event loop
- ONE Supabase Realtime WebSocket connection
- Event routing to multiple SSE clients via in-memory queues
- Automatic cleanup when clients disconnect

Performance:
- 100 users = 1 connection (instead of 100)
- 1 thread (instead of 100)
- Minimal memory overhead
"""

import os
import asyncio
import threading
import queue
from typing import Dict, Set
from dotenv import load_dotenv

load_dotenv()


class RealtimeConnectionManager:
    """
    Singleton manager for shared Supabase Realtime connection
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        
        # Job ID to queues mapping: {job_id: set(queue1, queue2, ...)}
        self.subscriptions: Dict[str, Set[queue.Queue]] = {}
        self.subscriptions_lock = threading.Lock()
        
        # Async client and channel
        self.async_client = None
        self.channel = None
        
        # Background thread control
        self.thread = None
        self.loop = None
        self.stop_event = threading.Event()
        self.running = False
        
        print("🔌 Realtime Connection Manager initialized")
    
    def start(self):
        """Start the background Realtime connection thread"""
        if self.running:
            print("⚠️ Realtime manager already running")
            return
        
        self.running = True
        self.stop_event.clear()
        
        # Start background thread
        self.thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.thread.start()
        
        print("✅ Realtime Connection Manager started")
    
    def stop(self):
        """Stop the background thread and cleanup"""
        if not self.running:
            return
        
        print("🛑 Stopping Realtime Connection Manager...")
        self.stop_event.set()
        self.running = False
        
        if self.thread:
            self.thread.join(timeout=5)
        
        print("✅ Realtime Connection Manager stopped")
    
    def subscribe_to_job(self, job_id: str, client_queue: queue.Queue):
        """
        Subscribe a client queue to job updates
        
        Args:
            job_id: Job UUID to watch
            client_queue: Queue to receive updates
        """
        with self.subscriptions_lock:
            if job_id not in self.subscriptions:
                self.subscriptions[job_id] = set()
            
            self.subscriptions[job_id].add(client_queue)
            count = len(self.subscriptions[job_id])
            
            print(f"📥 Client subscribed to job {job_id} ({count} total subscribers)")
    
    def unsubscribe_from_job(self, job_id: str, client_queue: queue.Queue):
        """
        Unsubscribe a client queue from job updates
        
        Args:
            job_id: Job UUID
            client_queue: Queue to remove
        """
        with self.subscriptions_lock:
            if job_id in self.subscriptions:
                self.subscriptions[job_id].discard(client_queue)
                
                # Clean up empty subscription sets
                if not self.subscriptions[job_id]:
                    del self.subscriptions[job_id]
                    print(f"🗑️ No more subscribers for job {job_id}, cleaned up")
                else:
                    count = len(self.subscriptions[job_id])
                    print(f"📤 Client unsubscribed from job {job_id} ({count} remaining)")
    
    def _dispatch_event(self, job_id: str, payload: dict):
        """
        Dispatch event to all subscribers of a job
        
        Args:
            job_id: Job UUID
            payload: Event data from Realtime
        """
        with self.subscriptions_lock:
            if job_id not in self.subscriptions:
                return
            
            subscribers = list(self.subscriptions[job_id])
        
        # Send to all subscriber queues (outside lock to prevent blocking)
        for client_queue in subscribers:
            try:
                client_queue.put_nowait(payload)
            except queue.Full:
                print(f"⚠️ Queue full for job {job_id}, skipping event")
            except Exception as e:
                print(f"❌ Error dispatching to queue: {e}")
    
    def _run_async_loop(self):
        """Run async event loop in background thread"""
        try:
            # Create new event loop for this thread
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # Run the realtime listener
            self.loop.run_until_complete(self._realtime_listener())
            
        except Exception as e:
            print(f"❌ Async loop error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if self.loop:
                self.loop.close()
    
    async def _realtime_listener(self):
        """
        Async listener for ALL job updates
        
        Subscribes to public:jobs:* (all changes to jobs table)
        and routes events to the appropriate client queues
        """
        from supabase import acreate_client
        
        try:
            # Create async Supabase client
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            self.async_client = await acreate_client(supabase_url, supabase_key)
            
            print("🔌 Connecting to Supabase Realtime (shared connection)...")
            
            def handle_job_change(payload):
                """Callback for ANY job change (supports multiple payload shapes)"""
                try:
                    # Support both legacy and new realtime payloads
                    data = payload.get("data") if isinstance(payload, dict) else None
                    
                    # Determine event type
                    event_type = (
                        (payload.get("eventType") if isinstance(payload, dict) else None) or
                        (data.get("type") if isinstance(data, dict) else None) or
                        "UPDATE"
                    )
                    
                    # Extract records from multiple possible keys
                    if isinstance(data, dict):
                        new_record = data.get("record") or data.get("new") or {}
                        old_record = data.get("old_record") or data.get("old") or {}
                    else:
                        new_record = payload.get("new", payload.get("record", {})) if isinstance(payload, dict) else {}
                        old_record = payload.get("old", {}) if isinstance(payload, dict) else {}
                    
                    # Try multiple extraction paths - completion updates often have job_id in old
                    job_id = (
                        (new_record.get("job_id") if isinstance(new_record, dict) else None) or
                        (old_record.get("job_id") if isinstance(old_record, dict) else None) or
                        (payload.get("job_id") if isinstance(payload, dict) else None)
                    )
                    
                    if not job_id:
                        # Silently skip - these are usually metadata-only updates
                        return
                    
                    print(f"🔔 Job {job_id} updated: {event_type}")
                    
                    # Normalize payload to include top-level 'new' for SSE generator
                    normalized_payload = payload
                    if isinstance(payload, dict) and 'new' not in payload:
                        # Build a normalized copy with 'new'
                        normalized_payload = dict(payload)
                        if isinstance(new_record, dict):
                            normalized_payload['new'] = new_record
                        if isinstance(old_record, dict):
                            normalized_payload['old'] = old_record
                        if 'eventType' not in normalized_payload:
                            normalized_payload['eventType'] = event_type
                    
                    # Dispatch to all clients watching this job
                    self._dispatch_event(job_id, normalized_payload)
                    
                except Exception as e:
                    print(f"❌ Error in realtime callback: {e}")
            
            # Subscribe to ALL job changes (wildcard filter)
            self.channel = self.async_client.channel("jobs-all")
            await self.channel.on_postgres_changes(
                event="*",  # All events (INSERT, UPDATE, DELETE)
                schema="public",
                table="jobs",
                callback=handle_job_change
            ).subscribe()
            
            print("✅ Subscribed to ALL job updates (shared connection active)")
            
            # Keep connection alive until stop signal
            while not self.stop_event.is_set():
                await asyncio.sleep(1)
            
            # Cleanup
            await self.channel.unsubscribe()
            print("🔌 Unsubscribed from Supabase Realtime")
            
        except Exception as e:
            print(f"❌ Realtime listener error: {e}")
            import traceback
            traceback.print_exc()


# Global singleton instance
_realtime_manager = RealtimeConnectionManager()


def get_realtime_manager() -> RealtimeConnectionManager:
    """Get the global Realtime manager instance"""
    return _realtime_manager


def ensure_realtime_started():
    """Ensure the Realtime manager is running (call this on app startup)"""
    manager = get_realtime_manager()
    if not manager.running:
        manager.start()
