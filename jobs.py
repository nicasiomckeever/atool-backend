"""
Job Management Module
Handles creation, tracking, and updates of image generation jobs
"""

import os
import uuid
from datetime import datetime
from typing import Optional, List, Dict
from supabase_client import supabase
from worker_client import get_worker_client

# Beta unlimited mode - no credit deduction
UNLIMITED_MODE = os.getenv("UNLIMITED_MODE", "true").lower() == "true"

# ✅ OPTIMIZATION: Use batch RPC for job creation (reduces 6 calls to 1)
USE_BATCH_JOB_CREATION = os.getenv("USE_BATCH_JOB_CREATION", "true").lower() == "true"

# ✅ EDGE FUNCTION: Route queue operations through edge function to worker projects
USE_EDGE_FUNCTION = os.getenv("USE_EDGE_FUNCTION", "false").lower() == "true"


def create_job(user_id: str, prompt: str, model: str = "flux-dev", 
               aspect_ratio: str = "1:1", negative_prompt: str = "",
               job_type: str = "image", duration: int = 5, image_url: str = None) -> dict:
    """
    Create a new image/video generation job
    ✅ OPTIMIZED: Can use batch RPC (6 operations → 1 call)
    Set USE_BATCH_JOB_CREATION=true to enable
    
    Args:
        user_id: UUID of the user
        prompt: Text prompt for image/video generation
        model: AI model to use (default: flux-dev)
        aspect_ratio: Image aspect ratio (default: 1:1)
        negative_prompt: Things to avoid in generation
        job_type: Type of job - 'image' or 'video'
        duration: Duration in seconds for video generation (default: 5)
        image_url: Optional uploaded image URL for image-to-video
        
    Returns:
        dict with job data
    """
    try:
        # ✅ OPTIMIZED: Use batch RPC if enabled (reduces 6 calls to 1)
        # NOTE: Batch RPC doesn't support duration/image_url, so skip for video jobs OR when image_url provided
        if USE_BATCH_JOB_CREATION and job_type != "video" and not image_url:
            try:
                print(f"🚀 Using BATCH RPC for job creation")
                batch_response = supabase.rpc(
                    'create_job_batch',
                    {
                        'p_user_id': user_id,
                        'p_prompt': prompt,
                        'p_model': model,
                        'p_aspect_ratio': aspect_ratio
                    }
                ).execute()
                
                if batch_response.data:
                    result = batch_response.data
                    if result.get('success'):
                        print(f"✅ Batch job creation successful: {result['job']['id']}")
                        return {
                            "success": True,
                            "job": result['job'],
                            "credits_remaining": result['credits_remaining']
                        }
                    else:
                        print(f"⚠️ Batch RPC failed: {result.get('error')}, falling back to traditional method")
                else:
                    print("⚠️ Batch RPC returned no data, falling back to traditional method")
            except Exception as batch_error:
                print(f"⚠️ Batch RPC error: {batch_error}, falling back to traditional method")
        
        # Traditional method (fallback or when batch is disabled)
        print(f"📝 Using TRADITIONAL method for job creation")
        
        # Get user credits and generation_count
        user_response = supabase.table("users").select("credits, generation_count").eq("id", user_id).execute()
        
        if not user_response.data:
            return {
                "success": False,
                "error": "User not found"
            }
        
        credits = user_response.data[0]["credits"]
        generation_count = user_response.data[0].get("generation_count", 0)
        
        # Check if user has enough credits (skip check in unlimited mode)
        if not UNLIMITED_MODE and credits < 1:
            return {
                "success": False,
                "error": "Insufficient credits"
            }
        
        # OPTIMIZED: Atomic increment with single API call
        # Uses PostgreSQL's increment operator to avoid race conditions
        # DO THIS BEFORE creating job so priority is included in INSERT event
        increment_response = supabase.rpc(
            'increment_generation_count',
            {'user_uuid': user_id}
        ).execute()
        
        # Fallback if RPC doesn't exist: use traditional update
        if not increment_response.data:
            new_generation_count = generation_count + 1
            supabase.table("users").update({
                "generation_count": new_generation_count
            }).eq("id", user_id).execute()
        else:
            new_generation_count = increment_response.data
        
        # Determine priority queue based on generation_count
        # ≤10 → priority1, ≤50 → priority2, >50 → priority3
        if new_generation_count <= 10:
            queue_table = "priority1_queue"
            priority_level = 1
        elif new_generation_count <= 50:
            queue_table = "priority2_queue"
            priority_level = 2
        else:
            queue_table = "priority3_queue"
            priority_level = 3
        
        # Create job WITH priority in metadata from the start
        metadata = {"priority": priority_level}
        
        # Debug: Log what we're receiving
        print(f"🔍 create_job called with: job_type='{job_type}', duration={duration}")
        
        # Add duration to metadata for video jobs
        if job_type == "video":
            metadata["duration"] = duration
            print(f"⏱️  Added duration ({duration}s) to job metadata")
        else:
            print(f"⚠️  job_type is '{job_type}', NOT 'video' - duration NOT added to metadata")
        
        # Add image_url to metadata for image-to-video
        if image_url:
            metadata["input_image_url"] = image_url
            print(f"🖼️  Added input image URL to job metadata: {image_url}")
        else:
            print(f"⚠️  No image_url provided - creating text-only job")
        
        job_data = {
            "user_id": user_id,
            "prompt": prompt,
            "model": model,
            "aspect_ratio": aspect_ratio,
            "status": "pending",
            "progress": 0,
            "metadata": metadata
        }
        
        # Add image_url to job table if provided (for Qwen Image Edit and other image-based models)
        if image_url:
            job_data["image_url"] = image_url
            print(f"✅ Added image_url to job_data: {image_url}")
        
        # Note: job_type is not stored in database, but passed to metadata for video jobs
        
        # Add negative_prompt only if database supports it (optional field)
        # Currently omitted as the jobs table doesn't have this column
        
        job_response = supabase.table("jobs").insert(job_data).execute()
        
        if not job_response.data:
            return {
                "success": False,
                "error": "Failed to create job"
            }
        
        job = job_response.data[0]
        
        # Debug: Print what we got back
        print(f"📊 Job created response: {job}")
        
        # Use job_id field (database uses job_id, not id)
        job_id = job.get("job_id") or job.get("id")
        
        if not job_id:
            print(f"⚠️ Warning: No job ID in response. Available fields: {list(job.keys())}")
            return {
                "success": False,
                "error": f"Job created but ID not returned. Fields: {list(job.keys())}"
            }
        
        # Insert into appropriate priority queue
        queue_entry = {
            "user_id": user_id,
            "job_id": job_id,
            "request_payload": {
                "prompt": prompt,
                "model": model,
                "aspect_ratio": aspect_ratio,
                "negative_prompt": negative_prompt
            }
        }
        
        # ✅ EDGE FUNCTION: Route to worker projects if enabled
        if USE_EDGE_FUNCTION:
            try:
                worker_client = get_worker_client()
                worker_client.insert(queue_table, queue_entry)
                print(f"📥 Job {job_id} added to {queue_table} via EDGE FUNCTION (generation #{new_generation_count}, priority={priority_level})")
            except Exception as edge_error:
                print(f"⚠️ Edge function failed, falling back to direct Supabase: {edge_error}")
                supabase.table(queue_table).insert(queue_entry).execute()
                print(f"📥 Job {job_id} added to {queue_table} via FALLBACK (generation #{new_generation_count}, priority={priority_level})")
        else:
            supabase.table(queue_table).insert(queue_entry).execute()
            print(f"📥 Job {job_id} added to {queue_table} (generation #{new_generation_count}, priority={priority_level})")
        
        # Deduct credit (skip in unlimited mode)
        if not UNLIMITED_MODE:
            supabase.table("users").update({
                "credits": credits - 1
            }).eq("id", user_id).execute()
            
            # Log usage
            supabase.table("usage_logs").insert({
                "user_id": user_id,
                "job_id": job_id,
                "credits_used": 1,
                "action": "image_generation"
            }).execute()
            
            print(f"✅ Job created: {job_id} for user {user_id} (credit deducted)")
        else:
            print(f"✅ Job created: {job_id} for user {user_id} (UNLIMITED MODE - no credit deducted)")
        
        return {
            "success": True,
            "job": {
                "id": job_id,  # Return as 'id' for API consistency
                "status": job["status"],
                "progress": job["progress"],
                "prompt": job["prompt"],
                "model": job["model"],
                "aspect_ratio": job["aspect_ratio"],
                "created_at": job["created_at"],
                "priority": priority_level,
                "generation_number": new_generation_count
            },
            "credits_remaining": credits - 1
        }
        
    except Exception as e:
        print(f"❌ Error creating job: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def get_job(job_id: str) -> dict:
    """
    Get job details by ID
    
    Args:
        job_id: UUID of the job
        
    Returns:
        dict with job data
    """
    try:
        response = supabase.table("jobs").select("*").eq("job_id", job_id).execute()
        
        if not response.data:
            return {
                "success": False,
                "error": "Job not found"
            }
        
        job = response.data[0]
        
        return {
            "success": True,
            "job": job
        }
        
    except Exception as e:
        print(f"❌ Error getting job: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def get_user_jobs(user_id: str, status: Optional[str] = None, limit: int = 50) -> dict:
    """
    Get all jobs for a user
    
    Args:
        user_id: UUID of the user
        status: Filter by status (pending/running/completed/failed)
        limit: Maximum number of jobs to return
        
    Returns:
        dict with list of jobs
    """
    try:
        query = supabase.table("jobs").select("*").eq("user_id", user_id)
        
        if status:
            query = query.eq("status", status)
        
        response = query.order("created_at", desc=True).limit(limit).execute()
        
        return {
            "success": True,
            "jobs": response.data,
            "count": len(response.data)
        }
        
    except Exception as e:
        print(f"❌ Error getting user jobs: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def update_job_status(job_id: str, status: str, progress: Optional[int] = None, 
                      error_message: Optional[str] = None) -> dict:
    """
    Update job status and progress
    
    Args:
        job_id: UUID of the job
        status: New status (pending/running/completed/failed/cancelled)
        progress: Progress percentage (0-100)
        error_message: Error message if failed
        
    Returns:
        dict with success status
    """
    try:
        update_data = {"status": status}
        
        if progress is not None:
            update_data["progress"] = progress
        
        if error_message:
            update_data["error_message"] = error_message
        
        if status == "running" and "started_at" not in update_data:
            update_data["started_at"] = datetime.utcnow().isoformat()
        
        if status in ["completed", "failed", "cancelled"]:
            update_data["completed_at"] = datetime.utcnow().isoformat()
            if status == "completed":
                update_data["progress"] = 100
        
        response = supabase.table("jobs").update(update_data).eq("job_id", job_id).execute()
        
        if not response.data:
            return {
                "success": False,
                "error": "Job not found"
            }
        
        print(f"✅ Job {job_id} updated to {status}")
        
        # Add 'id' field for frontend compatibility
        job_data = response.data[0]
        job_data["id"] = job_data.get("job_id")
        
        return {
            "success": True,
            "job": job_data
        }
        
    except Exception as e:
        print(f"❌ Error updating job: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def update_job_result(job_id: str, image_url: str, thumbnail_url: Optional[str] = None, video_url: Optional[str] = None) -> dict:
    """
    Update job with generated image/video URLs
    
    Args:
        job_id: UUID of the job
        image_url: URL of the generated image (or video for backwards compatibility)
        thumbnail_url: URL of the thumbnail (optional)
        video_url: URL of the generated video (optional, for video jobs)
        
    Returns:
        dict with success status
    """
    try:
        update_data = {
            "image_url": image_url,
            "status": "completed",
            "progress": 100,
            "completed_at": datetime.utcnow().isoformat()
        }
        
        if thumbnail_url:
            update_data["thumbnail_url"] = thumbnail_url
        
        # ✅ FIX: Also save video_url if provided (for video generation jobs)
        if video_url:
            update_data["video_url"] = video_url
            print(f"📹 Saving video URL: {video_url}")
        
        response = supabase.table("jobs").update(update_data).eq("job_id", job_id).execute()
        
        if not response.data:
            return {
                "success": False,
                "error": "Job not found"
            }
        
        print(f"✅ Job {job_id} completed with image")
        
        return {
            "success": True,
            "job": response.data[0]
        }
        
    except Exception as e:
        print(f"❌ Error updating job result: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def cancel_job(job_id: str, user_id: str) -> dict:
    """
    Cancel a pending or running job
    
    Args:
        job_id: UUID of the job
        user_id: UUID of the user (for verification)
        
    Returns:
        dict with success status
    """
    try:
        # Verify job belongs to user
        job_response = supabase.table("jobs").select("*").eq("job_id", job_id).eq("user_id", user_id).execute()
        
        if not job_response.data:
            return {
                "success": False,
                "error": "Job not found or unauthorized"
            }
        
        job = job_response.data[0]
        
        # Can only cancel pending or running jobs
        if job["status"] not in ["pending", "running"]:
            return {
                "success": False,
                "error": f"Cannot cancel job with status: {job['status']}"
            }
        
        # Update job status
        response = supabase.table("jobs").update({
            "status": "cancelled",
            "completed_at": datetime.utcnow().isoformat()
        }).eq("job_id", job_id).execute()
        
        # Refund credit if job was pending (not started yet)
        if job["status"] == "pending":
            user_response = supabase.table("users").select("credits").eq("id", user_id).execute()
            current_credits = user_response.data[0]["credits"]
            
            supabase.table("users").update({
                "credits": current_credits + 1
            }).eq("id", user_id).execute()
            
            print(f"✅ Job {job_id} cancelled, credit refunded")
        else:
            print(f"✅ Job {job_id} cancelled")
        
        return {
            "success": True,
            "message": "Job cancelled successfully",
            "refunded": job["status"] == "pending"
        }
        
    except Exception as e:
        print(f"❌ Error cancelling job: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def get_job_stats(user_id: str) -> dict:
    """
    Get job statistics for a user
    
    Args:
        user_id: UUID of the user
        
    Returns:
        dict with job statistics
    """
    try:
        # Get all jobs
        all_jobs = supabase.table("jobs").select("status").eq("user_id", user_id).execute()
        
        total = len(all_jobs.data)
        pending = len([j for j in all_jobs.data if j["status"] == "pending"])
        running = len([j for j in all_jobs.data if j["status"] == "running"])
        completed = len([j for j in all_jobs.data if j["status"] == "completed"])
        failed = len([j for j in all_jobs.data if j["status"] == "failed"])
        cancelled = len([j for j in all_jobs.data if j["status"] == "cancelled"])
        
        # Get user credits
        user_response = supabase.table("users").select("credits").eq("id", user_id).execute()
        credits = user_response.data[0]["credits"] if user_response.data else 0
        
        return {
            "success": True,
            "stats": {
                "total_jobs": total,
                "pending": pending,
                "running": running,
                "completed": completed,
                "failed": failed,
                "cancelled": cancelled,
                "credits_remaining": credits
            }
        }
        
    except Exception as e:
        print(f"❌ Error getting job stats: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def get_next_pending_job() -> dict:
    """
    Get the next pending job from the queue (for worker)
    ✅ OPTIMIZED: Uses single RPC call instead of 3 separate queries
    Reduces API calls from 3-7 per poll to 1-3 per poll
    Savings: ~34,000 calls/day
    
    Returns:
        dict with job data or None
    """
    try:
        # ✅ OPTIMIZED: Single RPC call checks all 3 priority queues
        # Before: 3 separate SELECT queries (one per priority)
        # After: 1 UNION query via RPC
        priority_response = supabase.rpc('get_next_priority_job', {}).execute()
        
        if priority_response.data and len(priority_response.data) > 0:
            queue_entry = priority_response.data[0]
            job_id = queue_entry["job_id"]
            queue_table = queue_entry["queue_table"]
            priority_level = queue_entry["priority_level"]
            queue_id = queue_entry["queue_id"]
            
            # Mark as processed in the appropriate queue table
            supabase.table(queue_table).update({
                "processed": True,
                "processed_at": datetime.utcnow().isoformat()
            }).eq("queue_id", queue_id).execute()
            
            # Get full job details
            job_response = supabase.table("jobs").select("*").eq("job_id", job_id).execute()
            
            if job_response.data:
                priority_emoji = {1: "🔵", 2: "🟡", 3: "🟠"}
                print(f"{priority_emoji.get(priority_level, '⚪')} Worker picked job {job_id} from PRIORITY {priority_level} queue")
                return {
                    "success": True,
                    "job": job_response.data[0],
                    "priority": priority_level
                }
        
        # No jobs in any queue
        print("💤 No pending jobs in any priority queue")
        return {
            "success": True,
            "job": None
        }
        
    except Exception as e:
        print(f"❌ Error getting next job: {e}")
        return {
            "success": False,
            "error": str(e)
        }
