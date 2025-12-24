"""
Mega Cloud Storage Integration
Handles uploading images to Mega.nz and generating public share links
"""

import os
import tempfile
from pathlib import Path
from dotenv import load_dotenv
from mega import Mega

load_dotenv()

class MegaStorage:
    """Mega cloud storage handler for image uploads"""
    
    def __init__(self):
        self.mega_email = os.getenv("MEGA_EMAIL")
        self.mega_password = os.getenv("MEGA_PASSWORD")
        self.mega = None
        self._authenticated = False
    
    def authenticate(self):
        """Authenticate with Mega using credentials from environment"""
        if self._authenticated:
            return True
        
        if not self.mega_email or not self.mega_password:
            raise ValueError("MEGA_EMAIL and MEGA_PASSWORD must be set in .env file")
        
        try:
            print("[MEGA] Authenticating with Mega...")
            self.mega = Mega()
            self.mega.login(self.mega_email, self.mega_password)
            self._authenticated = True
            print("[MEGA] Authentication successful!")
            return True
        except Exception as e:
            print(f"[MEGA ERROR] Authentication failed: {e}")
            raise Exception(f"Failed to authenticate with Mega: {str(e)}")
    
    def upload_image(self, image_path, folder_name="ai-generated-images"):
        """
        Upload an image to Mega and return the public link
        
        Args:
            image_path: Path to the image file to upload
            folder_name: Name of the folder in Mega to upload to (default: "ai-generated-images")
        
        Returns:
            dict: {
                "success": bool,
                "public_link": str,
                "file_name": str,
                "error": str (if failed)
            }
        """
        try:
            # Ensure authenticated
            if not self._authenticated:
                self.authenticate()
            
            if not os.path.exists(image_path):
                return {
                    "success": False,
                    "error": f"Image file not found: {image_path}"
                }
            
            file_name = os.path.basename(image_path)
            print(f"[MEGA] Uploading {file_name} to Mega...")
            
            # Upload directly to root folder (simpler and more reliable)
            # New accounts may have issues with folder creation
            print(f"[MEGA] Uploading to root folder...")
            uploaded_file = self.mega.upload(image_path)
            
            # Generate public link
            public_link = self.mega.get_upload_link(uploaded_file)
            
            print(f"[MEGA] Upload successful!")
            print(f"[MEGA] Public link: {public_link}")
            
            return {
                "success": True,
                "public_link": public_link,
                "file_name": file_name
            }
            
        except Exception as e:
            print(f"[MEGA ERROR] Upload failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def upload_image_from_bytes(self, image_bytes, file_name, folder_name="ai-generated-images"):
        """
        Upload an image from bytes to Mega and return the public link
        
        Args:
            image_bytes: Image data as bytes
            file_name: Name for the uploaded file
            folder_name: Name of the folder in Mega to upload to
        
        Returns:
            dict: Same as upload_image()
        """
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file_name).suffix) as tmp_file:
                tmp_file.write(image_bytes)
                tmp_path = tmp_file.name
            
            # Upload the temporary file
            result = self.upload_image(tmp_path, folder_name)
            
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


# Global instance
_mega_storage = None

def get_mega_storage():
    """Get or create the global MegaStorage instance"""
    global _mega_storage
    if _mega_storage is None:
        _mega_storage = MegaStorage()
    return _mega_storage


def download_from_mega_url(mega_url):
    """
    Download a file from a Mega.nz public link
    
    Args:
        mega_url: Public Mega.nz URL (e.g., https://mega.nz/#!...)
    
    Returns:
        bytes: File data, or None if failed
    """
    import tempfile
    import time
    import glob
    
    temp_dir = None
    try:
        print(f"[MEGA] Downloading from public link: {mega_url}")
        
        # Create Mega instance (no login needed for public links)
        mega = Mega()
        
        # Create a unique temporary directory
        temp_dir = tempfile.mkdtemp(prefix=f"mega_download_{int(time.time() * 1000)}_")
        
        print(f"[MEGA] Downloading to directory: {temp_dir}")
        
        # Download to the temp directory (mega.download_url expects a directory)
        downloaded_path = mega.download_url(mega_url, dest_path=temp_dir)
        
        print(f"[MEGA] Downloaded to: {downloaded_path}")
        
        # Read the file data
        with open(downloaded_path, 'rb') as f:
            file_data = f.read()
        
        print(f"[MEGA] Downloaded {len(file_data)} bytes")
        
        # Clean up
        try:
            os.unlink(downloaded_path)
            os.rmdir(temp_dir)
        except Exception as cleanup_error:
            print(f"[MEGA] Warning: Could not fully cleanup: {cleanup_error}")
        
        return file_data
        
    except Exception as e:
        print(f"[MEGA ERROR] Failed to download from URL: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to cleanup temp directory on error
        if temp_dir and os.path.exists(temp_dir):
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except:
                pass
        
        return None
