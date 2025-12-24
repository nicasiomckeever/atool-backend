"""
Cloudinary Manager with Automatic Account Rotation
Handles multiple Cloudinary accounts and switches based on bandwidth/storage usage
"""

import os
import requests
from dotenv import load_dotenv
import cloudinary
import cloudinary.uploader
import cloudinary.api
import tempfile
from pathlib import Path

load_dotenv()

class CloudinaryManager:
    """
    Manages multiple Cloudinary accounts with automatic rotation
    based on bandwidth and storage usage.

    Accounts are loaded from environment variables ONLY (no hard-coded creds):
    - Preferred: CLOUDINARY_ACCOUNTS as JSON array of objects with keys
      { name, cloud_name, api_key, api_secret }
    - Fallback: Indexed env vars for multiple accounts (up to 10):
      CLOUDINARY_1_CLOUD_NAME, CLOUDINARY_1_API_KEY, CLOUDINARY_1_API_SECRET, CLOUDINARY_1_NAME
      CLOUDINARY_2_CLOUD_NAME, ...
    - Legacy single-account support:
      CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET
    """
    
    # Bandwidth threshold in bytes (20GB)
    BANDWIDTH_THRESHOLD = 20 * 1024 * 1024 * 1024  # 20GB
    
    # Storage threshold percentage
    STORAGE_THRESHOLD_PERCENT = 95  # Switch at 95% storage usage
    
    def __init__(self):
        """Initialize with multiple Cloudinary accounts (from env only)"""
        self.accounts = self._load_accounts_from_env()
        
        if not self.accounts:
            raise ValueError("No valid Cloudinary accounts configured in environment!")
        
        self.current_account_index = 0
        print(f"[CLOUDINARY MANAGER] Initialized with {len(self.accounts)} account(s)")
        for i, acc in enumerate(self.accounts):
            masked_key = self._mask(acc.get('api_key'))
            print(f"  {i+1}. {acc.get('name','Account')} | cloud: {acc.get('cloud_name')} | key: {masked_key}")

    @staticmethod
    def _mask(value: str, visible: int = 4) -> str:
        if not value:
            return ""
        return f"{value[:visible]}...{value[-visible:]}" if len(value) > visible*2 else "*" * len(value)

    def _load_accounts_from_env(self):
        """Load Cloudinary accounts from environment.

        Order of precedence:
        1) CLOUDINARY_ACCOUNTS (JSON array)
        2) Indexed vars CLOUDINARY_{i}_CLOUD_NAME/API_KEY/API_SECRET (i=1..10)
        3) Legacy single account CLOUDINARY_CLOUD_NAME/API_KEY/API_SECRET
        """
        import json

        accounts = []

        # 1) JSON array
        raw = os.getenv("CLOUDINARY_ACCOUNTS")
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    for idx, acc in enumerate(parsed):
                        if not isinstance(acc, dict):
                            continue
                        cloud_name = acc.get("cloud_name")
                        api_key = acc.get("api_key")
                        api_secret = acc.get("api_secret")
                        name = acc.get("name") or f"Account {idx+1}"
                        if cloud_name and api_key and api_secret:
                            accounts.append({
                                "name": name,
                                "cloud_name": cloud_name,
                                "api_key": api_key,
                                "api_secret": api_secret,
                            })
            except Exception as e:
                print(f"[CLOUDINARY MANAGER] Warning: Failed to parse CLOUDINARY_ACCOUNTS JSON: {e}")

        # 2) Indexed vars (only if none loaded yet)
        if not accounts:
            for i in range(1, 11):
                # Support both CLOUDINARY_1_* and CLOUDINARY_*_1 styles (first preferred)
                cn = os.getenv(f"CLOUDINARY_{i}_CLOUD_NAME") or os.getenv(f"CLOUDINARY_CLOUD_NAME_{i}")
                ak = os.getenv(f"CLOUDINARY_{i}_API_KEY") or os.getenv(f"CLOUDINARY_API_KEY_{i}")
                as_ = os.getenv(f"CLOUDINARY_{i}_API_SECRET") or os.getenv(f"CLOUDINARY_API_SECRET_{i}")
                nm = os.getenv(f"CLOUDINARY_{i}_NAME") or os.getenv(f"CLOUDINARY_NAME_{i}") or f"Account {i}"
                if cn and ak and as_:
                    accounts.append({
                        "name": nm,
                        "cloud_name": cn,
                        "api_key": ak,
                        "api_secret": as_,
                    })

        # 3) Legacy single account (append even if indexed accounts exist)
        cn = os.getenv("CLOUDINARY_CLOUD_NAME")
        ak = os.getenv("CLOUDINARY_API_KEY")
        as_ = os.getenv("CLOUDINARY_API_SECRET")
        if cn and ak and as_:
            # Avoid duplicates by cloud_name+api_key
            exists = any((a.get("cloud_name") == cn and a.get("api_key") == ak) for a in accounts)
            if not exists:
                accounts.append({
                    "name": "Primary",
                    "cloud_name": cn,
                    "api_key": ak,
                    "api_secret": as_,
                })

        # Final filter
        accounts = [a for a in accounts if a.get("cloud_name") and a.get("api_key") and a.get("api_secret")]
        return accounts
    
    def get_current_account(self):
        """Get the currently active account"""
        return self.accounts[self.current_account_index]
    
    def configure_account(self, account):
        """Configure Cloudinary with specific account credentials"""
        try:
            cloudinary.config(
                cloud_name=account["cloud_name"],
                api_key=account["api_key"],
                api_secret=account["api_secret"],
                secure=True
            )
            print(f"[CLOUDINARY MANAGER] Configured account: {account['name']} ({account['cloud_name']})")
            return True
        except Exception as e:
            print(f"[CLOUDINARY MANAGER ERROR] Failed to configure {account['name']}: {e}")
            return False
    
    def check_account_usage(self, account):
        """
        Check bandwidth and storage usage for a Cloudinary account
        
        Returns:
            dict: {
                "bandwidth_used": int (bytes),
                "bandwidth_limit": int (bytes),
                "bandwidth_percent": float,
                "storage_used": int (bytes),
                "storage_limit": int (bytes),
                "storage_percent": float,
                "over_threshold": bool
            }
        """
        try:
            url = f"https://api.cloudinary.com/v1_1/{account['cloud_name']}/usage"
            auth = (account["api_key"], account["api_secret"])
            
            print(f"[CLOUDINARY MANAGER] Checking usage for {account['name']}...")
            response = requests.get(url, auth=auth, timeout=10)
            
            if response.status_code != 200:
                print(f"[CLOUDINARY MANAGER] Usage check failed: {response.status_code}")
                return None
            
            data = response.json()
            usage = data.get("usage", {})
            
            # Bandwidth info
            bandwidth = usage.get("bandwidth", {})
            bandwidth_used = bandwidth.get("used", 0)
            bandwidth_limit = bandwidth.get("limit", 0)
            bandwidth_unlimited = bandwidth.get("unlimited", False)
            
            # Storage info
            storage = usage.get("storage", {})
            storage_used = storage.get("used", 0)
            storage_limit = storage.get("limit", 0)
            storage_unlimited = storage.get("unlimited", False)
            
            # Calculate percentages
            bandwidth_percent = (bandwidth_used / bandwidth_limit * 100) if bandwidth_limit > 0 else 0
            storage_percent = (storage_used / storage_limit * 100) if storage_limit > 0 else 0
            
            # Check if over threshold
            over_bandwidth = bandwidth_used >= self.BANDWIDTH_THRESHOLD and not bandwidth_unlimited
            over_storage = storage_percent >= self.STORAGE_THRESHOLD_PERCENT and not storage_unlimited
            over_threshold = over_bandwidth or over_storage
            
            usage_info = {
                "bandwidth_used": bandwidth_used,
                "bandwidth_limit": bandwidth_limit,
                "bandwidth_percent": bandwidth_percent,
                "bandwidth_unlimited": bandwidth_unlimited,
                "storage_used": storage_used,
                "storage_limit": storage_limit,
                "storage_percent": storage_percent,
                "storage_unlimited": storage_unlimited,
                "over_threshold": over_threshold,
                "over_bandwidth": over_bandwidth,
                "over_storage": over_storage
            }
            
            # Log usage
            print(f"[CLOUDINARY MANAGER] {account['name']} Usage:")
            print(f"  Bandwidth: {bandwidth_used / (1024**3):.2f}GB / {bandwidth_limit / (1024**3):.2f}GB ({bandwidth_percent:.1f}%)")
            print(f"  Storage: {storage_used / (1024**3):.2f}GB / {storage_limit / (1024**3):.2f}GB ({storage_percent:.1f}%)")
            
            if over_bandwidth:
                print(f"  ⚠️  BANDWIDTH THRESHOLD EXCEEDED (20GB)")
            if over_storage:
                print(f"  ⚠️  STORAGE THRESHOLD EXCEEDED (95%)")
            
            return usage_info
            
        except Exception as e:
            print(f"[CLOUDINARY MANAGER ERROR] Failed to check usage for {account['name']}: {e}")
            return None
    
    def rotate_to_next_account(self):
        """Switch to the next available account"""
        start_index = self.current_account_index
        
        # Try each account in sequence
        for i in range(len(self.accounts)):
            next_index = (start_index + i + 1) % len(self.accounts)
            next_account = self.accounts[next_index]
            
            print(f"[CLOUDINARY MANAGER] Trying account {next_index + 1}: {next_account['name']}")
            
            # Check usage of this account
            usage = self.check_account_usage(next_account)
            
            if usage and not usage.get("over_threshold"):
                # This account is good to use
                self.current_account_index = next_index
                self.configure_account(next_account)
                print(f"[CLOUDINARY MANAGER] ✅ Switched to account: {next_account['name']}")
                return True
            else:
                print(f"[CLOUDINARY MANAGER] ❌ Account {next_account['name']} not available")
        
        # All accounts are over threshold
        print(f"[CLOUDINARY MANAGER] ⚠️  All accounts are over threshold! Using current: {self.get_current_account()['name']}")
        return False
    
    def select_best_account(self):
        """
        Check current account and rotate if needed
        Returns True if a good account is available
        """
        current = self.get_current_account()
        
        # Check current account usage
        usage = self.check_account_usage(current)
        
        if not usage:
            print(f"[CLOUDINARY MANAGER] Could not check usage, using current account: {current['name']}")
            self.configure_account(current)
            return True
        
        if usage.get("over_threshold"):
            print(f"[CLOUDINARY MANAGER] Current account {current['name']} is over threshold, rotating...")
            return self.rotate_to_next_account()
        else:
            print(f"[CLOUDINARY MANAGER] ✅ Current account {current['name']} is healthy")
            self.configure_account(current)
            return True
    
    def upload_with_retry(self, upload_func, *args, **kwargs):
        """
        Upload with automatic retry on different accounts if errors occur
        
        Args:
            upload_func: The upload function to call (e.g., cloudinary.uploader.upload)
            *args, **kwargs: Arguments to pass to upload function
        
        Returns:
            Upload result or raises exception
        """
        max_retries = len(self.accounts)
        
        for attempt in range(max_retries):
            try:
                # Select best account before upload
                self.select_best_account()
                
                current = self.get_current_account()
                print(f"[CLOUDINARY MANAGER] Uploading using account: {current['name']} (attempt {attempt + 1}/{max_retries})")
                
                # Perform upload
                result = upload_func(*args, **kwargs)
                
                print(f"[CLOUDINARY MANAGER] ✅ Upload successful on {current['name']}")
                return result
                
            except Exception as e:
                error_msg = str(e).lower()
                current = self.get_current_account()
                
                print(f"[CLOUDINARY MANAGER] ❌ Upload failed on {current['name']}: {e}")
                
                # Check if error is quota/limit related
                is_quota_error = any(keyword in error_msg for keyword in [
                    "quota", "limit", "exceeded", "storage", "bandwidth"
                ])
                
                if is_quota_error and attempt < max_retries - 1:
                    print(f"[CLOUDINARY MANAGER] Quota error detected, rotating to next account...")
                    self.rotate_to_next_account()
                elif attempt < max_retries - 1:
                    print(f"[CLOUDINARY MANAGER] Error occurred, trying next account...")
                    self.rotate_to_next_account()
                else:
                    print(f"[CLOUDINARY MANAGER] All accounts failed!")
                    raise
    
    def upload_image(self, image_path, folder_name="ai-generated-images", metadata=None):
        """
        Upload an image to Cloudinary with automatic account rotation
        
        Args:
            image_path: Path to the image file
            folder_name: Folder name in Cloudinary
            metadata: Optional metadata dict
        
        Returns:
            dict with upload result
        """
        try:
            if not os.path.exists(image_path):
                return {
                    "success": False,
                    "error": f"Image file not found: {image_path}"
                }
            
            file_name = os.path.basename(image_path)
            print(f"[CLOUDINARY MANAGER] Uploading image: {file_name}")
            
            # Build upload parameters
            upload_params = {
                "folder": folder_name,
                "resource_type": "image",
                "overwrite": False,
                "unique_filename": True
            }
            
            # Add context metadata if provided
            if metadata:
                context_str = "|".join([f"{k}={v}" for k, v in metadata.items() if v])
                if context_str:
                    upload_params["context"] = context_str
            
            # Upload with retry logic
            upload_result = self.upload_with_retry(
                cloudinary.uploader.upload,
                image_path,
                **upload_params
            )
            
            current = self.get_current_account()
            print(f"[CLOUDINARY MANAGER] ✅ Image uploaded successfully to {current['name']}")
            print(f"[CLOUDINARY MANAGER] URL: {upload_result['secure_url']}")
            
            return {
                "success": True,
                "public_url": upload_result['url'],
                "secure_url": upload_result['secure_url'],
                "file_name": file_name,
                "public_id": upload_result['public_id'],
                "width": upload_result.get('width'),
                "height": upload_result.get('height'),
                "format": upload_result.get('format'),
                "account_used": current['name']
            }
            
        except Exception as e:
            print(f"[CLOUDINARY MANAGER ERROR] Upload failed: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e)
            }
    
    def upload_image_from_bytes(self, image_bytes, file_name, folder_name="ai-generated-images", metadata=None):
        """Upload an image from bytes"""
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file_name).suffix) as tmp_file:
                tmp_file.write(image_bytes)
                tmp_path = tmp_file.name
            
            # Upload the temporary file
            result = self.upload_image(tmp_path, folder_name, metadata=metadata)
            
            # Clean up temporary file
            try:
                os.unlink(tmp_path)
            except:
                pass
            
            return result
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def upload_video(self, video_path, job_id=None, folder_name="ai-generated-videos", metadata=None):
        """
        Upload a video to Cloudinary with automatic account rotation
        
        Args:
            video_path: Path to the video file
            job_id: Job ID for naming (optional)
            folder_name: Folder name in Cloudinary
            metadata: Optional metadata dict
        
        Returns:
            str: Secure URL of uploaded video
        """
        try:
            if not os.path.exists(video_path):
                raise Exception(f"Video file not found: {video_path}")
            
            file_name = os.path.basename(video_path)
            print(f"[CLOUDINARY MANAGER] Uploading video: {file_name}")
            
            # Build upload parameters
            upload_params = {
                "folder": folder_name,
                "resource_type": "video",
                "overwrite": False,
                "unique_filename": True
            }
            
            # Add public_id if job_id provided
            if job_id:
                upload_params["public_id"] = f"{folder_name}/video_{job_id}"
            
            # Add context metadata if provided
            if metadata:
                context_str = "|".join([f"{k}={v}" for k, v in metadata.items() if v])
                if context_str:
                    upload_params["context"] = context_str
            
            # Upload with retry logic
            upload_result = self.upload_with_retry(
                cloudinary.uploader.upload,
                video_path,
                **upload_params
            )
            
            current = self.get_current_account()
            print(f"[CLOUDINARY MANAGER] ✅ Video uploaded successfully to {current['name']}")
            print(f"[CLOUDINARY MANAGER] URL: {upload_result['secure_url']}")
            
            return upload_result['secure_url']
            
        except Exception as e:
            print(f"[CLOUDINARY MANAGER ERROR] Video upload failed: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Failed to upload video: {str(e)}")


# Global instance
_cloudinary_manager = None

def get_cloudinary_manager():
    """Get or create the global CloudinaryManager instance"""
    global _cloudinary_manager
    if _cloudinary_manager is None:
        _cloudinary_manager = CloudinaryManager()
    return _cloudinary_manager
