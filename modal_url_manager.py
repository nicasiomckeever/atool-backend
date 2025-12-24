"""
Modal URL Manager - UNIFIED DEPLOYMENTS VERSION
Manages Modal deployments from Supabase with paired image/video URLs
Uses modal_deployments table for proper FIFO rotation
Integrates with ModalDeploymentMonitor for realtime waiting

IMPORTANT - Fetch Behavior:
==========================
1. URL FETCHING: ALWAYS uses regular database SELECT queries (NOT realtime)
2. MONITORING: Realtime is ONLY used when waiting for new deployments
3. AUTO-SWITCH: Once deployment found, automatically switches back to regular fetch

Flow:
-----
1. get_active_deployment() → Regular SELECT query from database
2. If found → Return deployment (via regular fetch) ✅
3. If NOT found → Start realtime monitoring (wait mode) 🔔
4. When new deployment inserted → Realtime event detected
5. Stop monitoring → Next call uses regular fetch again ✅

This ensures efficient operation:
- Regular fetch: Fast, direct database queries
- Realtime: Only for waiting/notifications, not for fetching
"""

import os
from datetime import datetime
from typing import Optional, Dict, Literal
from supabase_client import supabase

# Import deployment monitor (lazy import to avoid circular dependencies)
_deployment_monitor = None

def get_deployment_monitor():
    """Lazy import of deployment monitor"""
    global _deployment_monitor
    if _deployment_monitor is None:
        from modal_deployment_monitor import get_deployment_monitor as _get_monitor
        _deployment_monitor = _get_monitor()
    return _deployment_monitor


class ModalURLManager:
    """Manages Modal deployments with paired image and video URLs"""
    
    def __init__(self):
        self.current_deployment = None
        self.current_deployment_id = None
        self.current_image_url = None
        self.current_video_url = None
    
    def get_active_deployment(self) -> Optional[Dict]:
        """
        Get the oldest active Modal deployment from modal_deployments (FIFO rotation)
        ALWAYS uses regular database fetch (not realtime)
        Only starts realtime monitoring if NO deployments found
        
        Returns:
            Deployment record if found, None otherwise
        """
        try:
            # IMPORTANT: ALWAYS use regular SELECT query (not realtime)
            # This ensures we fetch the latest state from database
            response = supabase.table("modal_deployments").select("*").eq(
                "is_active", True
            ).order("created_at", desc=False).limit(1).execute()
            
            if response.data and len(response.data) > 0:
                deployment = response.data[0]
                self.current_deployment = deployment
                self.current_deployment_id = deployment["id"]
                self.current_image_url = deployment["image_url"]
                self.current_video_url = deployment["video_url"]
                
                print(f"[OK] ✅ Fetched active Modal deployment #{deployment['deployment_number']} (via regular fetch)")
                print(f"   Deployment ID: {self.current_deployment_id}")
                print(f"   Image URL: {self.current_image_url}")
                print(f"   Video URL: {self.current_video_url}")
                print(f"   Created: {deployment.get('created_at')}")
                
                # IMPORTANT: Stop realtime monitoring if it was running
                # After this point, all fetches will be regular database queries
                monitor = get_deployment_monitor()
                if monitor.is_monitoring:
                    print("[INFO] 🛑 Stopping realtime monitoring - switching to regular fetch mode")
                    monitor.stop_monitoring()
                
                return deployment
            else:
                print("[WARN] ⚠️ No active Modal deployments available")
                
                # Only start realtime monitoring if not already monitoring
                # Realtime is ONLY used for waiting, NOT for fetching
                monitor = get_deployment_monitor()
                if not monitor.is_monitoring:
                    print("[INFO] 🔔 Starting realtime monitoring (waiting for new deployments)")
                    print("[INFO] ℹ️  Note: Once deployment available, will switch back to regular fetch")
                    monitor.start_monitoring()
                
                return None
                
        except Exception as e:
            print(f"[ERROR] Error fetching active deployment: {e}")
            return None
    
    def get_endpoint_url(self, job_type: Literal["image", "video"] = "image") -> Optional[str]:
        """
        Get appropriate Modal endpoint URL based on job type from current deployment
        
        Args:
            job_type: Type of job - "image" or "video"
            
        Returns:
            URL for the appropriate endpoint
        """
        # ALWAYS fetch fresh deployment from database
        # This ensures we don't use a deployment that was marked inactive by another process
        deployment = self.get_active_deployment()
        if not deployment:
            print(f"[ERROR] No active deployment found for {job_type} job")
            return None
        
        # Update last_used_at timestamp
        try:
            supabase.table("modal_deployments").update({
                "last_used_at": datetime.utcnow().isoformat()
            }).eq("id", self.current_deployment_id).execute()
        except:
            pass  # Non-critical
        
        # Return appropriate URL based on job type
        if job_type == "video":
            print(f"[VIDEO] Using video endpoint: {self.current_video_url}")
            return self.current_video_url
        else:
            print(f"[IMAGE] Using image endpoint: {self.current_image_url}")
            return self.current_image_url
    
    def mark_deployment_inactive(self, deployment_id: Optional[str] = None) -> bool:
        """
        Mark a Modal deployment as inactive (both image and video URLs)
        
        Args:
            deployment_id: The UUID of the deployment (optional, uses current if not provided)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            target_id = deployment_id or self.current_deployment_id
            
            if not target_id:
                print("[WARN] No deployment ID provided to mark as inactive")
                return False
            
            print(f"[DEBUG] Attempting to mark deployment as inactive: {target_id}")
            
            # Update modal_deployments table
            update_data = {
                "is_active": False,
                "updated_at": datetime.utcnow().isoformat()
            }
            
            response = supabase.table("modal_deployments").update(
                update_data
            ).eq("id", target_id).execute()
            
            if response.data and len(response.data) > 0:
                deployment_num = response.data[0].get("deployment_number", "unknown")
                print(f"[OK] Marked deployment #{deployment_num} as inactive")
                print(f"   Both image and video URLs deactivated")
                
                # Clear current deployment if it was the one marked inactive
                if target_id == self.current_deployment_id:
                    self.current_deployment = None
                    self.current_deployment_id = None
                    self.current_image_url = None
                    self.current_video_url = None
                
                return True
            else:
                print(f"[WARN] Deployment not found in modal_deployments table: {target_id}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error marking deployment as inactive: {e}")
            return False
    
    def is_limit_reached_error(self, error_message: str) -> bool:
        """
        Check if error message indicates deployment should be marked inactive
        (rate limit, stopped endpoint, deployment errors, etc.)
        
        Args:
            error_message: Error message to check
            
        Returns:
            True if deployment should be marked inactive, False otherwise
        """
        if not error_message:
            return False
        
        # Patterns indicating deployment should be deactivated and rotated
        expiry_indicators = [
            # Rate limit errors
            "limit reached",
            "rate limit",
            "too many requests",
            "quota exceeded",
            "429",
            "limit exceeded",
            "rate_limit",
            
            # Stopped/dead endpoint errors
            "is stopped",
            "endpoint is stopped",
            "app is stopped",
            "404",
            "not found",
            
            # Deployment/availability errors
            "deployment not found",
            "no active deployment",
            "unavailable",
            "unreachable"
        ]
        
        error_lower = str(error_message).lower()
        return any(indicator in error_lower for indicator in expiry_indicators)
    
    def get_deployment_stats(self) -> Dict:
        """
        Get statistics about Modal deployments in database
        
        Returns:
            Dict with counts by status
        """
        try:
            # Get all deployments from modal_deployments
            all_deployments = supabase.table("modal_deployments").select("*").execute()
            
            stats = {
                "total": len(all_deployments.data),
                "active": 0,
                "inactive": 0,
                "oldest_active": None,
                "newest_active": None
            }
            
            active_deployments = [d for d in all_deployments.data if d.get("is_active")]
            
            stats["active"] = len(active_deployments)
            stats["inactive"] = stats["total"] - stats["active"]
            
            if active_deployments:
                # Sort by created_at
                active_sorted = sorted(active_deployments, key=lambda x: x.get("created_at", ""))
                stats["oldest_active"] = active_sorted[0].get("deployment_number")
                stats["newest_active"] = active_sorted[-1].get("deployment_number")
            
            return stats
            
        except Exception as e:
            print(f"❌ Error getting deployment stats: {e}")
            return {"error": str(e)}


# Singleton instance
_modal_url_manager: Optional[ModalURLManager] = None


def get_modal_url_manager() -> ModalURLManager:
    """
    Get or create singleton ModalURLManager instance
    
    Returns:
        ModalURLManager instance
    """
    global _modal_url_manager
    
    if _modal_url_manager is None:
        _modal_url_manager = ModalURLManager()
    
    return _modal_url_manager


if __name__ == "__main__":
    """Test the Modal URL Manager"""
    print("[TEST] Testing Modal URL Manager\n")
    
    manager = get_modal_url_manager()
    
    # Test 1: Get active deployment
    print("Test 1: Get active Modal deployment")
    deployment = manager.get_active_deployment()
    if deployment:
        print(f"[OK] Got deployment #{deployment['deployment_number']}\n")
    else:
        print("[WARN] No active deployments available\n")
    
    # Test 2: Get endpoint URLs
    print("Test 2: Get endpoint URLs")
    image_url = manager.get_endpoint_url("image")
    video_url = manager.get_endpoint_url("video")
    print(f"Image URL: {image_url}")
    print(f"Video URL: {video_url}\n")
    
    # Test 3: Get stats
    print("Test 3: Get deployment statistics")
    stats = manager.get_deployment_stats()
    print(f"Stats: {stats}\n")
    
    # Test 4: Check error detection
    print("Test 4: Error detection")
    print(f"Is 'rate limit reached' an expiry error? {manager.is_limit_reached_error('rate limit reached')}")
    print(f"Is '404: app is stopped' an expiry error? {manager.is_limit_reached_error('404: app is stopped')}")
    print(f"Is 'connection timeout' an expiry error? {manager.is_limit_reached_error('connection timeout')}\n")
    
    print("[OK] All tests completed!")
