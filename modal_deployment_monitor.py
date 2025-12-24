"""
Modal Deployment Monitor - Realtime Watching for Fresh URLs
Monitors modal_deployments table when no active URLs are available
Sends ntfy notifications to developers when waiting for new deployments
"""

import os
import time
import threading
import requests
from datetime import datetime
from typing import Optional, Callable
from supabase_client import supabase

# Singleton instance
_monitor_instance = None
_monitor_lock = threading.Lock()


class ModalDeploymentMonitor:
    """
    Monitors modal_deployments table in realtime when no active URLs are available
    Sends developer notifications via ntfy.sh
    """
    
    def __init__(self):
        self.is_monitoring = False
        self.monitor_thread = None
        self.channel = None
        self.callback: Optional[Callable] = None
        self.stop_flag = threading.Event()
        
        # ntfy configuration
        self.ntfy_topic = "resource_finished"  # ntfy.sh/resource_finished
        self.ntfy_url = f"https://ntfy.sh/{self.ntfy_topic}"
        
        print("[ModalDeploymentMonitor] Initialized")
    
    def send_ntfy_notification(self, title: str, message: str, priority: str = "default"):
        """
        Send notification to developer via ntfy.sh
        
        Args:
            title: Notification title
            message: Notification message
            priority: Notification priority (min, low, default, high, urgent)
        """
        try:
            response = requests.post(
                self.ntfy_url,
                data=message.encode('utf-8'),
                headers={
                    "Title": title,
                    "Priority": priority,
                    "Tags": "warning,modal,deployment"
                },
                timeout=5
            )
            
            if response.status_code == 200:
                print(f"[ntfy] ✅ Notification sent: {title}")
                return True
            else:
                print(f"[ntfy] ⚠️ Failed to send notification: {response.status_code}")
                return False
        except Exception as e:
            print(f"[ntfy] ❌ Error sending notification: {e}")
            return False
    
    def start_monitoring(self, on_new_deployment: Optional[Callable] = None):
        """
        Start monitoring modal_deployments table for new active deployments
        
        Args:
            on_new_deployment: Callback function called when new deployment is detected
        """
        if self.is_monitoring:
            print("[ModalDeploymentMonitor] Already monitoring")
            return
        
        self.callback = on_new_deployment
        self.is_monitoring = True
        self.stop_flag.clear()
        
        # Send initial notification
        self.send_ntfy_notification(
            title="⚠️ No Modal URLs Available",
            message="No active Modal deployments found. Waiting for new deployment...\n\n"
                   "Please deploy Modal endpoints:\n"
                   "• modal deploy modal_app_image.py\n"
                   "• modal deploy modal_app.py\n\n"
                   "Then run: python notify_discord.py",
            priority="high"
        )
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        print("[ModalDeploymentMonitor] ✅ Started monitoring for new deployments")
    
    def stop_monitoring(self):
        """Stop monitoring modal_deployments table"""
        if not self.is_monitoring:
            return
        
        print("[ModalDeploymentMonitor] Stopping monitoring...")
        self.is_monitoring = False
        self.stop_flag.set()
        
        # Cleanup realtime channel
        if self.channel:
            try:
                supabase.remove_channel(self.channel)
            except Exception as e:
                print(f"[ModalDeploymentMonitor] Error removing channel: {e}")
            self.channel = None
        
        # Wait for thread to finish
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        print("[ModalDeploymentMonitor] ✅ Stopped monitoring")
    
    def _monitor_loop(self):
        """Background monitoring loop that checks for new deployments"""
        print("[ModalDeploymentMonitor] Monitor loop started")
        
        # Subscribe to modal_deployments table changes
        try:
            def handle_deployment_insert(payload):
                """Handle new deployment insertions"""
                try:
                    new_deployment = payload.get('new', {})
                    is_active = new_deployment.get('is_active', False)
                    deployment_number = new_deployment.get('deployment_number', 'unknown')
                    
                    print(f"[ModalDeploymentMonitor] 🔔 New deployment detected: #{deployment_number}")
                    
                    if is_active:
                        print(f"[ModalDeploymentMonitor] ✅ Active deployment found!")
                        
                        # Send success notification
                        image_url = new_deployment.get('image_url', 'N/A')
                        video_url = new_deployment.get('video_url', 'N/A')
                        
                        self.send_ntfy_notification(
                            title="✅ New Modal Deployment Available",
                            message=f"Deployment #{deployment_number} is now active!\n\n"
                                   f"Image URL: {image_url}\n"
                                   f"Video URL: {video_url}\n\n"
                                   "Workers will now resume processing jobs.",
                            priority="default"
                        )
                        
                        # Call callback if provided
                        if self.callback:
                            try:
                                self.callback(new_deployment)
                            except Exception as e:
                                print(f"[ModalDeploymentMonitor] Error in callback: {e}")
                        
                        # Stop monitoring since we found an active deployment
                        self.stop_monitoring()
                    else:
                        print(f"[ModalDeploymentMonitor] ℹ️ Deployment #{deployment_number} is inactive, continuing to monitor...")
                
                except Exception as e:
                    print(f"[ModalDeploymentMonitor] Error handling deployment insert: {e}")
            
            # Set up realtime subscription
            self.channel = supabase.channel('modal_deployments_monitor')
            
            self.channel.on_postgres_changes(
                event='INSERT',
                schema='public',
                table='modal_deployments',
                callback=handle_deployment_insert
            ).subscribe()
            
            print("[ModalDeploymentMonitor] 🔔 Subscribed to modal_deployments table")
            
            # Keep thread alive while monitoring
            while not self.stop_flag.is_set():
                time.sleep(1)
        
        except Exception as e:
            print(f"[ModalDeploymentMonitor] ❌ Error in monitor loop: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            print("[ModalDeploymentMonitor] Monitor loop ended")
    
    def check_for_active_deployments(self) -> bool:
        """
        Check if there are any active deployments in the database
        
        Returns:
            True if active deployments exist, False otherwise
        """
        try:
            response = supabase.table("modal_deployments").select("*").eq("is_active", True).limit(1).execute()
            
            has_active = response.data and len(response.data) > 0
            
            if has_active:
                print(f"[ModalDeploymentMonitor] ✅ Found active deployment")
            else:
                print(f"[ModalDeploymentMonitor] ⚠️ No active deployments found")
            
            return has_active
        
        except Exception as e:
            print(f"[ModalDeploymentMonitor] Error checking for active deployments: {e}")
            return False


def get_deployment_monitor() -> ModalDeploymentMonitor:
    """
    Get or create singleton ModalDeploymentMonitor instance
    
    Returns:
        ModalDeploymentMonitor instance
    """
    global _monitor_instance
    
    if _monitor_instance is None:
        with _monitor_lock:
            if _monitor_instance is None:
                _monitor_instance = ModalDeploymentMonitor()
    
    return _monitor_instance


if __name__ == "__main__":
    """Test the Modal Deployment Monitor"""
    print("=" * 60)
    print("Testing Modal Deployment Monitor")
    print("=" * 60)
    
    monitor = get_deployment_monitor()
    
    # Test 1: Check for active deployments
    print("\nTest 1: Checking for active deployments...")
    has_active = monitor.check_for_active_deployments()
    print(f"Has active deployments: {has_active}")
    
    # Test 2: Send test notification
    print("\nTest 2: Sending test notification...")
    monitor.send_ntfy_notification(
        title="🧪 Test Notification",
        message="This is a test notification from Modal Deployment Monitor",
        priority="low"
    )
    
    # Test 3: Start monitoring (if no active deployments)
    if not has_active:
        print("\nTest 3: Starting realtime monitoring...")
        
        def on_new_deployment(deployment):
            print(f"📢 Callback: New deployment #{deployment.get('deployment_number')}")
        
        monitor.start_monitoring(on_new_deployment=on_new_deployment)
        
        print("\nMonitoring for 30 seconds...")
        print("Add a new deployment to modal_deployments table to test realtime updates")
        time.sleep(30)
        
        monitor.stop_monitoring()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("=" * 60)
