"""
Cloudinary Cloud Storage Integration
Handles uploading images to Cloudinary and generating public URLs
"""

import os
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import cloudinary
import cloudinary.uploader
import cloudinary.api

load_dotenv()

class CloudinaryStorage:
    """Cloudinary cloud storage handler for image uploads"""
    
    def __init__(self):
        self.cloud_name = os.getenv("CLOUDINARY_CLOUD_NAME")
        self.api_key = os.getenv("CLOUDINARY_API_KEY")
        self.api_secret = os.getenv("CLOUDINARY_API_SECRET")
        self._configured = False
    
    def configure(self):
        """Configure Cloudinary with credentials from environment"""
        if self._configured:
            return True
        
        if not self.cloud_name or not self.api_key or not self.api_secret:
            raise ValueError("CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, and CLOUDINARY_API_SECRET must be set in .env file")
        
        try:
            print("[CLOUDINARY] Configuring Cloudinary...")
            cloudinary.config(
                cloud_name=self.cloud_name,
                api_key=self.api_key,
                api_secret=self.api_secret,
                secure=True
            )
            self._configured = True
            print(f"[CLOUDINARY] Configuration successful! Cloud: {self.cloud_name}")
            return True
        except Exception as e:
            print(f"[CLOUDINARY ERROR] Configuration failed: {e}")
            raise Exception(f"Failed to configure Cloudinary: {str(e)}")
    
    def upload_image(self, image_path, folder_name="ai-generated-images", metadata=None):
        """
        Upload an image to Cloudinary and return the public URL
        
        Args:
            image_path: Path to the image file to upload
            folder_name: Folder name in Cloudinary to upload to (default: "ai-generated-images")
            metadata: Optional dict with context metadata (prompt, model, aspect_ratio, etc.)
        
        Returns:
            dict: {
                "success": bool,
                "public_url": str,
                "secure_url": str,
                "file_name": str,
                "public_id": str,
                "error": str (if failed)
            }
        """
        try:
            # Ensure configured
            if not self._configured:
                self.configure()
            
            if not os.path.exists(image_path):
                return {
                    "success": False,
                    "error": f"Image file not found: {image_path}"
                }
            
            file_name = os.path.basename(image_path)
            print(f"[CLOUDINARY] Uploading {file_name} to Cloudinary...")
            
            # Build upload parameters
            upload_params = {
                "folder": folder_name,
                "resource_type": "image",
                "overwrite": False,
                "unique_filename": True
            }
            
            # Add context metadata if provided
            if metadata:
                print(f"[CLOUDINARY] Received metadata: {metadata}")
                # Format: key1=value1|key2=value2
                # Filter out None and empty string values
                context_str = "|".join([f"{k}={v}" for k, v in metadata.items() if v])
                if context_str:
                    upload_params["context"] = context_str
                    print(f"[CLOUDINARY] Context string: {context_str}")
                else:
                    print(f"[CLOUDINARY] Warning: All metadata values were empty, no context added")
            
            # Upload to Cloudinary
            upload_result = cloudinary.uploader.upload(image_path, **upload_params)
            
            print(f"[CLOUDINARY] Upload successful!")
            print(f"[CLOUDINARY] Public URL: {upload_result['secure_url']}")
            print(f"[CLOUDINARY] Public ID: {upload_result['public_id']}")
            
            return {
                "success": True,
                "public_url": upload_result['url'],
                "secure_url": upload_result['secure_url'],
                "file_name": file_name,
                "public_id": upload_result['public_id'],
                "width": upload_result.get('width'),
                "height": upload_result.get('height'),
                "format": upload_result.get('format')
            }
            
        except Exception as e:
            print(f"[CLOUDINARY ERROR] Upload failed: {e}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "error": str(e)
            }
    
    def upload_image_from_bytes(self, image_bytes, file_name, folder_name="ai-generated-images", metadata=None):
        """
        Upload an image from bytes to Cloudinary and return the public URL
        
        Args:
            image_bytes: Image data as bytes
            file_name: Name for the uploaded file
            folder_name: Folder name in Cloudinary to upload to
            metadata: Optional dict with context metadata (prompt, model, aspect_ratio, etc.)
        
        Returns:
            dict: Same as upload_image()
        """
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file_name).suffix) as tmp_file:
                tmp_file.write(image_bytes)
                tmp_path = tmp_file.name
            
            # Upload the temporary file with metadata
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
        Upload a video to Cloudinary and return the public URL
        
        Args:
            video_path: Path to the video file to upload
            job_id: Job ID for naming (optional)
            folder_name: Folder name in Cloudinary to upload to (default: "ai-generated-videos")
            metadata: Optional dict with context metadata (prompt, model, etc.)
        
        Returns:
            str: Secure URL of the uploaded video
        """
        try:
            # Ensure configured
            if not self._configured:
                self.configure()
            
            if not os.path.exists(video_path):
                raise Exception(f"Video file not found: {video_path}")
            
            file_name = os.path.basename(video_path)
            print(f"[CLOUDINARY] Uploading video {file_name} to Cloudinary...")
            
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
                print(f"[CLOUDINARY] Received video metadata: {metadata}")
                context_str = "|".join([f"{k}={v}" for k, v in metadata.items() if v])
                if context_str:
                    upload_params["context"] = context_str
                    print(f"[CLOUDINARY] Context string: {context_str}")
            
            # Upload to Cloudinary
            upload_result = cloudinary.uploader.upload(video_path, **upload_params)
            
            print(f"[CLOUDINARY] Video upload successful!")
            print(f"[CLOUDINARY] Secure URL: {upload_result['secure_url']}")
            print(f"[CLOUDINARY] Public ID: {upload_result['public_id']}")
            print(f"[CLOUDINARY] Duration: {upload_result.get('duration', 'N/A')}s")
            
            return upload_result['secure_url']
            
        except Exception as e:
            print(f"[CLOUDINARY ERROR] Video upload failed: {e}")
            import traceback
            traceback.print_exc()
            raise Exception(f"Failed to upload video to Cloudinary: {str(e)}")
    
    def delete_image(self, public_id):
        """
        Delete an image from Cloudinary
        
        Args:
            public_id: The public_id of the image to delete
        
        Returns:
            dict: {"success": bool, "result": str}
        """
        try:
            if not self._configured:
                self.configure()
            
            print(f"[CLOUDINARY] Deleting image: {public_id}")
            result = cloudinary.uploader.destroy(public_id)
            
            print(f"[CLOUDINARY] Delete result: {result}")
            return {
                "success": result.get('result') == 'ok',
                "result": result.get('result')
            }
        except Exception as e:
            print(f"[CLOUDINARY ERROR] Delete failed: {e}")
            return {
                "success": False,
                "error": str(e)
            }


# Global instance
_cloudinary_storage = None

def get_cloudinary_storage():
    """Get or create the global CloudinaryStorage instance"""
    global _cloudinary_storage
    if _cloudinary_storage is None:
        _cloudinary_storage = CloudinaryStorage()
    return _cloudinary_storage
