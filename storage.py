"""
Storage Module
Handles image uploads and downloads from Supabase Storage
"""

import os
import io
from typing import Optional
from PIL import Image
from supabase_client import supabase


BUCKET_NAME = "generated-images"


def upload_image(image_data: bytes, user_id: str, job_id: str, 
                 create_thumbnail: bool = True) -> dict:
    """
    Upload generated image to Supabase Storage
    
    Args:
        image_data: Raw image bytes
        user_id: UUID of the user
        job_id: UUID of the job
        create_thumbnail: Whether to create a thumbnail (default: True)
        
    Returns:
        dict with image URLs
    """
    try:
        # Create file path: user_id/job_id.png
        file_path = f"{user_id}/{job_id}.png"
        
        # Upload full image
        upload_response = supabase.storage.from_(BUCKET_NAME).upload(
            path=file_path,
            file=image_data,
            file_options={"content-type": "image/png"}
        )
        
        # Get public URL
        image_url = supabase.storage.from_(BUCKET_NAME).get_public_url(file_path)
        
        thumbnail_url = None
        
        # Create and upload thumbnail
        if create_thumbnail:
            thumbnail_data = create_thumbnail_image(image_data, max_size=256)
            thumbnail_path = f"{user_id}/thumbnails/{job_id}.png"
            
            supabase.storage.from_(BUCKET_NAME).upload(
                path=thumbnail_path,
                file=thumbnail_data,
                file_options={"content-type": "image/png"}
            )
            
            thumbnail_url = supabase.storage.from_(BUCKET_NAME).get_public_url(thumbnail_path)
        
        print(f"✅ Image uploaded: {file_path}")
        
        return {
            "success": True,
            "image_url": image_url,
            "thumbnail_url": thumbnail_url
        }
        
    except Exception as e:
        print(f"❌ Error uploading image: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def upload_image_from_path(image_path: str, user_id: str, job_id: str, 
                           create_thumbnail: bool = True) -> dict:
    """
    Upload image from file path
    
    Args:
        image_path: Path to image file
        user_id: UUID of the user
        job_id: UUID of the job
        create_thumbnail: Whether to create a thumbnail
        
    Returns:
        dict with image URLs
    """
    try:
        with open(image_path, 'rb') as f:
            image_data = f.read()
        
        return upload_image(image_data, user_id, job_id, create_thumbnail)
        
    except Exception as e:
        print(f"❌ Error reading image file: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def create_thumbnail_image(image_data: bytes, max_size: int = 256) -> bytes:
    """
    Create thumbnail from image data
    
    Args:
        image_data: Original image bytes
        max_size: Maximum dimension (default: 256)
        
    Returns:
        Thumbnail image bytes
    """
    try:
        # Open image
        img = Image.open(io.BytesIO(image_data))
        
        # Calculate new dimensions (maintain aspect ratio)
        img.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
        
        # Save to bytes
        output = io.BytesIO()
        img.save(output, format='PNG', optimize=True)
        output.seek(0)
        
        return output.read()
        
    except Exception as e:
        print(f"❌ Error creating thumbnail: {e}")
        return image_data  # Return original if thumbnail fails


def get_image_url(user_id: str, job_id: str) -> dict:
    """
    Get public URL for an image
    
    Args:
        user_id: UUID of the user
        job_id: UUID of the job
        
    Returns:
        dict with image URL
    """
    try:
        file_path = f"{user_id}/{job_id}.png"
        
        # Check if file exists
        files = supabase.storage.from_(BUCKET_NAME).list(f"{user_id}")
        
        file_exists = any(f["name"] == f"{job_id}.png" for f in files)
        
        if not file_exists:
            return {
                "success": False,
                "error": "Image not found"
            }
        
        image_url = supabase.storage.from_(BUCKET_NAME).get_public_url(file_path)
        
        return {
            "success": True,
            "image_url": image_url
        }
        
    except Exception as e:
        print(f"❌ Error getting image URL: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def delete_image(user_id: str, job_id: str) -> dict:
    """
    Delete image from storage
    
    Args:
        user_id: UUID of the user
        job_id: UUID of the job
        
    Returns:
        dict with success status
    """
    try:
        file_path = f"{user_id}/{job_id}.png"
        thumbnail_path = f"{user_id}/thumbnails/{job_id}.png"
        
        # Delete main image
        supabase.storage.from_(BUCKET_NAME).remove([file_path])
        
        # Try to delete thumbnail (might not exist)
        try:
            supabase.storage.from_(BUCKET_NAME).remove([thumbnail_path])
        except:
            pass
        
        print(f"✅ Image deleted: {file_path}")
        
        return {
            "success": True,
            "message": "Image deleted successfully"
        }
        
    except Exception as e:
        print(f"❌ Error deleting image: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def generate_signed_url(user_id: str, job_id: str, expires_in: int = 3600) -> dict:
    """
    Generate a signed URL for private image access
    
    Args:
        user_id: UUID of the user
        job_id: UUID of the job
        expires_in: Expiry time in seconds (default: 1 hour)
        
    Returns:
        dict with signed URL
    """
    try:
        file_path = f"{user_id}/{job_id}.png"
        
        # Create signed URL
        signed_url = supabase.storage.from_(BUCKET_NAME).create_signed_url(
            path=file_path,
            expires_in=expires_in
        )
        
        return {
            "success": True,
            "signed_url": signed_url["signedURL"],
            "expires_in": expires_in
        }
        
    except Exception as e:
        print(f"❌ Error generating signed URL: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def list_user_images(user_id: str) -> dict:
    """
    List all images for a user
    
    Args:
        user_id: UUID of the user
        
    Returns:
        dict with list of image paths
    """
    try:
        files = supabase.storage.from_(BUCKET_NAME).list(user_id)
        
        image_files = [f for f in files if f["name"].endswith(".png")]
        
        return {
            "success": True,
            "images": image_files,
            "count": len(image_files)
        }
        
    except Exception as e:
        print(f"❌ Error listing images: {e}")
        return {
            "success": False,
            "error": str(e)
        }
