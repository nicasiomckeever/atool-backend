"""
Job Worker with Supabase Realtime WebSocket
Replaces polling with instant push notifications
"""

import os
import sys
import time
import base64
import asyncio
import threading
import requests
import logging
from dotenv import load_dotenv
from modal_url_manager import get_modal_url_manager

# Fix Windows console encoding for emoji support
if sys.platform == "win32":
    try:
        import codecs
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())
    except Exception:
        pass  # Ignore if encoding setup fails

# Suppress noisy websocket errors during keepalive timeout (expected behavior)
# These errors occur when connection times out during long-running jobs
# Our auto-reconnection logic handles this gracefully
logging.getLogger('websockets').setLevel(logging.CRITICAL)
logging.getLogger('websockets.protocol').setLevel(logging.CRITICAL)
logging.getLogger('realtime').setLevel(logging.WARNING)
logging.getLogger('root').setLevel(logging.WARNING)

# Load environment variables
load_dotenv()

# Use internal worker URL for container-to-container communication
BACKEND_URL = os.getenv("WORKER_BACKEND_URL") or os.getenv("BACKEND_URL", "http://localhost:5000")
SUPABASE_URL = os.getenv("SUPABASE_URL")
# CRITICAL: Worker needs SERVICE_ROLE_KEY for Realtime subscriptions (anon key gets 401)
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

# SSL Certificate Verification (disable for self-signed certs)
VERIFY_SSL = os.getenv("VERIFY_SSL", "False").lower() == "true"

print("=" * 60)
print("🤖 JOB WORKER STARTING (REALTIME MODE)")
print("=" * 60)
print(f"📡 Backend URL (external): {os.getenv('BACKEND_URL', 'NOT SET')}")
print(f"📡 Worker Backend URL (internal): {os.getenv('WORKER_BACKEND_URL', 'NOT SET')}")
print(f"📡 Using: {BACKEND_URL}")
print(f"🔗 Supabase URL: {SUPABASE_URL}")
print("=" * 60)
print()
sys.stdout.flush()  # Force flush output immediately

def get_comfyui_url():
    """Fetch ComfyUI URL from backend (Modal URL from Discord)"""
    try:
        response = requests.get(f"{BACKEND_URL}/get-url", timeout=10, verify=VERIFY_SSL)
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("url"):
                url = data["url"]
                print(f"✅ ComfyUI URL: {url}")
                return url
        return None
    except Exception as e:
        print(f"❌ Error fetching ComfyUI URL: {e}")
        return None

def on_new_job(payload):
    """Callback when new job is created via realtime"""
    try:
        print()
        print("🔔 REALTIME EVENT RECEIVED!")
        print(f"Payload: {payload}")
        
        # Extract job data from payload
        record = payload.get("record") or payload.get("new") or payload
        
        if not record:
            print("⚠️ No record in payload")
            return
        
        # Only process pending jobs
        if record.get("status") != "pending":
            print(f"⏭️ Skipping job with status: {record.get('status')}")
            return
        
        job_id = record.get("job_id") or record.get("id")
        
        # Get priority from metadata
        metadata = record.get("metadata", {})
        priority = metadata.get("priority", "N/A")
        priority_emoji = {1: "🔵", 2: "🟡", 3: "🟠"}.get(priority, "⚪")
        
        print()
        print("=" * 60)
        print(f"📋 NEW JOB (REALTIME): {job_id}")
        print("=" * 60)
        print(f"👤 User: {record.get('user_id')}")
        print(f"📝 Prompt: {record.get('prompt')}")
        print(f"🤖 Model: {record.get('model')}")
        print(f"📐 Aspect Ratio: {record.get('aspect_ratio')}")
        print(f"{priority_emoji} Priority: {priority}")
        print("=" * 60)
        print()
        
        # Make sure Modal is awake
        print(f"📋 Ensuring Modal is awake...")
        comfyui_url = get_comfyui_url()
        if not comfyui_url:
            print(f"⚠️ Could not get Modal URL, skipping job...")
            return
        
        # Process the job
        process_job(record, comfyui_url)
        
    except Exception as e:
        print(f"❌ Error in realtime callback: {e}")
        import traceback
        traceback.print_exc()


def process_job(job, comfyui_url=None):
    """Process a job by calling ComfyUI API or Video API with HYBRID ROUTING"""
    job_id = job.get("job_id") or job.get("id")
    job_type = job.get("job_type", "image")  # Default to image if not specified
    
    # Detect video jobs by model name if job_type not specified
    model = job.get("model", "")
    # Support both LTX-Video and Wan2.2 (Wan2.2 uses ComfyUI workflows)
    video_models = ["ltx-video-13b", "ltx-video", "wan22-animate-14b", "wan2.2", "wan"]
    
    if job_type == "image" and any(vm in model.lower() for vm in video_models):
        job_type = "video"
        print(f"🔍 Detected VIDEO job based on model: {model}")
    
    print(f"\n{'='*60}")
    print(f"🎨 HYBRID ROUTING: {job_type.upper()} generation")
    print(f"{'='*60}")
    
    # HYBRID ROUTING: Get appropriate endpoint URL based on job type
    manager = get_modal_url_manager()
    endpoint_url = manager.get_endpoint_url(job_type)
    
    if not endpoint_url:
        print(f"❌ Could not get {job_type} endpoint URL")
        return
    
    print(f"📡 Using endpoint: {endpoint_url}")
    print(f"🎯 Job ID: {job_id}")
    print(f"{'='*60}\n")
    
    # Route to appropriate handler based on job type
    if job_type == "video":
        return process_video_job(job, endpoint_url)
    else:
        return process_image_job(job, endpoint_url)


def process_video_job(job, base_url):
    """Process a video generation job via unified /generate endpoint"""
    job_id = job.get("job_id") or job.get("id")
    
    print(f"\n{'='*70}")
    print(f"🎬 PROCESSING VIDEO JOB")
    print(f"{'='*70}")
    print(f"📋 Job ID: {job_id}")
    print(f"👤 User ID: {job.get('user_id', 'N/A')}")
    print(f"📝 Prompt: {job.get('prompt', 'N/A')}")
    print(f"🤖 Model: {job.get('model', 'N/A')}")
    print(f"📐 Aspect Ratio: {job.get('aspect_ratio', '16:9')}")
    print(f"🔗 Modal Endpoint: {base_url}")
    print(f"{'='*70}\n")
    sys.stdout.flush()
    
    # Track the current URL in the manager for proper expiry handling
    url_manager = get_modal_url_manager()
    url_manager.current_url = base_url
    
    try:
        # Update job status to running
        requests.post(
            f"{BACKEND_URL}/worker/job/{job_id}/progress",
            json={
                "progress": 10,
                "message": "Starting video generation..."
            },
            timeout=10
        )
        
        # Use unified API on the main endpoint (/generate with type: "video")
        video_api_url = base_url
        
        # Check for input image URL and duration in job metadata
        metadata = job.get("metadata", {})
        print(f"📦 Job metadata: {metadata}")
        input_image_url = metadata.get("input_image_url")
        duration = metadata.get("duration", 5)  # Default to 5 seconds
        print(f"⏱️  Duration from metadata: {duration} seconds")
        
        # AUTO-DETECT: Determine if i2v or t2v based on image presence
        job_model = job.get("model", "wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors")
        
        # Override model based on image presence (use exact Modal API filenames)
        if input_image_url:
            # Image provided -> use image-to-video model and workflow
            actual_model = "wan2.2_i2v_high_noise_14B_fp16.safetensors"
            workflow_type = "image-to-video"
            print(f"🖼️  AUTO-DETECTED: Image-to-Video mode")
            print(f"   Input image: {input_image_url}")
        else:
            # No image -> use text-to-video model and workflow
            actual_model = "wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors"
            workflow_type = "text-to-video"
            print(f"📝 AUTO-DETECTED: Text-to-Video mode")
        
        print(f"🤖 Model: {actual_model} (Original: {job_model})")
        print(f"🎬 Workflow: {workflow_type}")
        print(f"⏱️  Duration: {duration} seconds")
        
        # Map aspect ratio to WAN 2.2 supported resolutions
        aspect_ratio = job.get("aspect_ratio", "16:9")
        aspect_ratio_map = {
            "16:9": (1024, 576),   # Landscape
            "1:1": (768, 768),      # Square
            "9:16": (576, 1024),    # Portrait
        }
        width, height = aspect_ratio_map.get(aspect_ratio, (1024, 576))  # Default to 16:9
        print(f"📐 Aspect Ratio: {aspect_ratio} → {width}x{height}")
        
        # Prepare unified generation payload
        payload = {
            "type": "video",
            "prompt": job.get("prompt"),
            "model": actual_model,  # Use auto-detected model
            "workflow_type": workflow_type,  # Pass workflow type to API
            "width": width,
            "height": height,
            "duration": duration,  # Pass duration to unified API
            "fps": 25,  # 25 fps for faster generation
        }
        
        # Add input image URL if available (for image-to-video)
        if input_image_url:
            payload["input_image_url"] = input_image_url
        
        print(f"📤 Sending video generation request to {video_api_url}/generate")
        print(f"📦 Payload: {payload}")
        print(f"⏱️  Timeout: 1800 seconds (30 minutes)")
        
        # Call video API (longer timeout for video generation)
        response = requests.post(
            f"{video_api_url}/generate",
            json=payload,
            timeout=1800  # 30 minutes for video generation
        )
        
        print(f"📥 Response status: {response.status_code}")
        print(f"📄 Response headers: {dict(response.headers)}")
        
        if response.status_code != 200:
            # Try to get error details
            try:
                error_data = response.json()
                error_msg = error_data.get("error", response.text)
            except:
                error_msg = response.text
            raise Exception(f"Video API returned status {response.status_code}: {error_msg}")
        
        # Update progress
        requests.post(
            f"{BACKEND_URL}/worker/job/{job_id}/progress",
            json={
                "progress": 50,
                "message": "Video generated, uploading..."
            },
            timeout=10
        )
        
        # Save video file temporarily
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_file:
            tmp_file.write(response.content)
            video_path = tmp_file.name
        
        print(f"💾 Video saved temporarily to: {video_path}")
        
        # Upload to Cloudinary
        print(f"☁️  Uploading video to Cloudinary...")
        from cloudinary_manager import get_cloudinary_manager
        cloudinary = get_cloudinary_manager()
        
        video_url = cloudinary.upload_video(video_path, job_id)
        print(f"✅ Video uploaded: {video_url}")
        
        # Clean up temp file
        import os as os_module
        os_module.unlink(video_path)
        
        # Mark job as completed with video URL (use image_url field for compatibility)
        requests.post(
            f"{BACKEND_URL}/worker/job/{job_id}/complete",
            json={
                "image_url": video_url,  # Backend expects image_url field
                "video_url": video_url,  # Also include for clarity
                "success": True
            },
            timeout=10
        )
        
        print(f"✅ Video job {job_id} completed successfully!")
        
    except Exception as e:
        error_message = str(e)
        print(f"❌ Error processing video job: {error_message}")
        
        # Check if it's a rate limit error
        url_manager = get_modal_url_manager()
        
        # Debug: Show what we're checking
        print(f"[DEBUG] Checking if error should trigger URL expiry...")
        print(f"[DEBUG] Error message (lowercase): {error_message.lower()}")
        
        is_expiry_error = url_manager.is_limit_reached_error(error_message)
        print(f"[DEBUG] is_limit_reached_error() returned: {is_expiry_error}")
        
        if is_expiry_error:
            print("[ALERT] Modal deployment should be marked inactive (stopped/limit/error)!")
            print(f"[ACTION] Current deployment ID: {url_manager.current_deployment_id}")
            print("[ACTION] Marking current deployment as inactive in database...")
            
            success = url_manager.mark_deployment_inactive()
            print(f"[ACTION] Mark inactive result: {success}")
            
            # Invalidate the cache in app.py so it fetches fresh URL
            if success:
                print("[ACTION] Invalidating app.py cache...")
                try:
                    invalidate_response = requests.post(
                        f"{BACKEND_URL}/invalidate-cache",
                        timeout=5
                    )
                    if invalidate_response.status_code == 200:
                        print("[OK] Cache invalidation triggered successfully")
                    else:
                        print(f"[WARN] Cache invalidation returned {invalidate_response.status_code}")
                except Exception as cache_err:
                    print(f"[WARN] Failed to invalidate cache: {cache_err}")
            
            print("[ACTION] Getting next active deployment...")
            next_deployment = url_manager.get_active_deployment()
            if next_deployment:
                print(f"[OK] Next deployment ready: #{next_deployment['deployment_number']}")
                print("[INFO] Retrying ALL pending jobs with new deployment...")
                
                # STEP 3: URL Rotation Recovery - retry all pending jobs
                retry_all_pending_jobs()
            else:
                print("[ERROR] No active deployments available!")
            
            # DO NOT mark job as failed - leave it pending for retry
            print("[TERMINATE] Terminating current task without marking complete")
            print("[INFO] Job status remains unchanged for automatic retry")
            return
        else:
            print("[DEBUG] Error does not match expiry patterns, not rotating URL")
        
        # DO NOT mark job as failed for ANY error - leave it pending for retry
        print("[TERMINATE] Terminating current task without marking as failed")
        print("[INFO] Job remains in pending status for automatic retry")
        print(f"[DEBUG] Error was: {error_message}")
        return


def process_image_job(job, comfyui_url):
    """Process an image generation job"""
    job_id = job.get("job_id") or job.get("id")
    
    print(f"\n{'='*70}")
    print(f"🎨 PROCESSING IMAGE JOB")
    print(f"{'='*70}")
    print(f"📋 Job ID: {job_id}")
    print(f"👤 User ID: {job.get('user_id', 'N/A')}")
    print(f"📝 Prompt: {job.get('prompt', 'N/A')}")
    print(f"🤖 Model: {job.get('model', 'N/A')}")
    print(f"📐 Aspect Ratio: {job.get('aspect_ratio', '1:1')}")
    print(f"🚫 Negative Prompt: {job.get('negative_prompt', 'N/A')}")
    print(f"🔗 Modal Endpoint: {comfyui_url}")
    print(f"{'='*70}\n")
    sys.stdout.flush()  # Force immediate output on Windows
    
    # Track the current URL in the manager for proper expiry handling
    url_manager = get_modal_url_manager()
    url_manager.current_url = comfyui_url
    
    try:
        # Update job status to running
        requests.post(
            f"{BACKEND_URL}/worker/job/{job_id}/progress",
            json={
                "progress": 10,
                "message": "Starting generation..."
            },
            timeout=10
        )
        
        # Debug: Log all job fields
        print(f"🔍 Job object fields:")
        print(f"   Available: {list(job.keys())}")
        for key, value in job.items():
            if key == "metadata":
                print(f"   {key}: {value}")
            elif key not in ["prompt", "negative_prompt"]:  # Skip long fields
                print(f"   {key}: {value}")
        
        # Prepare payload
        model_name = job.get("model", "openflux1-v0.1.0-fp8.safetensors")
        metadata = job.get("metadata", {}) or {}
        input_image_url = metadata.get("input_image_url") or job.get("image_url")
        is_qwen = isinstance(model_name, str) and ("qwen" in model_name.lower())

        payload = {
            "prompt": job.get("prompt"),
            "aspect_ratio": job.get("aspect_ratio", "1:1"),
            "model": model_name  # Default to provided model
        }

        # Debug: Log what we found
        print(f"🔍 Image job processing:")
        print(f"   Model: {model_name}")
        print(f"   Is Qwen: {is_qwen}")
        print(f"   Metadata: {metadata}")
        print(f"   job.get('image_url'): {job.get('image_url')}")
        print(f"   Input Image URL: {input_image_url}")

        # Qwen Image Edit support: require input_image_url and provide sensible defaults
        if is_qwen:
            payload["input_image_url"] = input_image_url
            payload["steps"] = 20
            payload["cfg"] = 2.5
            # Special flag for Qwen workflow
            payload["is_qwen"] = True
            payload["qwen_model"] = "qwen_image_edit_fp8_e4m3fn.safetensors"
            payload["qwen_vae"] = "qwen_image_vae.safetensors"
            payload["qwen_text_encoder"] = "qwen_2.5_vl_7b_fp8_scaled.safetensors"
            print(f"🖼️  Qwen Image Edit detected, using input image: {input_image_url}")
            print(f"   Qwen Model: {payload['qwen_model']}")
            print(f"   Qwen VAE: {payload['qwen_vae']}")
            print(f"   Qwen Text Encoder: {payload['qwen_text_encoder']}")
        
        print(f"📤 Sending generation request to {comfyui_url}/generate")
        print(f"📦 Payload:")
        for key, value in payload.items():
            if key == "prompt" and len(str(value)) > 100:
                print(f"   {key}: {str(value)[:100]}...")
            else:
                print(f"   {key}: {value}")
        print(f"⏱️  Timeout: 300 seconds (5 minutes)")
        print()
        sys.stdout.flush()
        
        # Retry logic for Modal cold start
        max_retries = 3
        retry_delay = 10  # Start with 10 seconds
        response = None
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    print(f"🔄 Retry attempt {attempt + 1}/{max_retries} after {retry_delay}s...")
                    time.sleep(retry_delay)
                
                print(f"⏳ Sending request to Modal...")
                sys.stdout.flush()
                
                response = requests.post(
                    f"{comfyui_url}/generate",
                    json=payload,
                    timeout=300  # 5 minutes timeout for generation
                )
                
                print(f"📥 Response received! Status: {response.status_code}")
                print(f"📄 Response headers: {dict(response.headers)}")
                sys.stdout.flush()
                
                content_type = response.headers.get('Content-Type', '')
                print(f"📋 Content-Type: {content_type}")
                
                # Check for Modal stopped error (404)
                if response.status_code == 404 and "app for invoked web endpoint is stopped" in response.text:
                    if attempt < max_retries - 1:
                        print(f"⚠️  Modal is stopped, triggering cold start (takes ~30-60s)...")
                        retry_delay = 30  # Wait longer for cold start
                        continue
                    else:
                        raise Exception("Modal failed to start after multiple retries")
                
                # Success or other error - break retry loop
                break
                
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    print(f"⏱️  Request timed out, retrying...")
                    retry_delay = 30
                    continue
                raise Exception("Request timed out after 5 minutes")
            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    print(f"🔌 Connection error, retrying... ({e})")
                    retry_delay = 20
                    continue
                raise Exception(f"Connection error: {e}")
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"❌ Request failed, retrying... ({e})")
                    retry_delay = 15
                    continue
                raise Exception(f"Request failed: {e}")
        
        if response.status_code != 200:
            raise Exception(f"ComfyUI returned {response.status_code}: {response.text}")
        
        if response.status_code == 200:
            # Check if response is image or JSON
            content_type = response.headers.get("Content-Type", "")
            print(f"📋 Content-Type: {content_type}")
            
            if "image" in content_type:
                # Got image directly
                image_data = response.content
                print(f"🖼️ Received image directly ({len(image_data)} bytes)")
                
                # Prepare metadata for Cloudinary
                metadata = {
                    "prompt": job.get("prompt", ""),
                    "model": job.get("model", ""),
                    "aspect_ratio": job.get("aspect_ratio", ""),
                    "job_id": job_id,
                    "user_id": job.get("user_id", "")
                }
                
                print(f"📋 Metadata to upload:")
                for key, value in metadata.items():
                    print(f"   {key}: '{value}' (type: {type(value).__name__})")
                
                # Upload to Cloudinary with metadata
                print(f"\n{'='*70}")
                print(f"☁️  UPLOADING TO CLOUDINARY")
                print(f"{'='*70}")
                print(f"📁 File name: job_{job_id}.png")
                print(f"📏 Image size: {len(image_data)} bytes ({len(image_data)/1024:.1f} KB)")
                print(f"⏱️  Uploading...")
                sys.stdout.flush()
                
                upload_response = requests.post(
                    f"{BACKEND_URL}/cloudinary/upload-image",
                    json={
                        "image_data": base64.b64encode(image_data).decode('utf-8'),
                        "file_name": f"job_{job_id}.png",
                        "metadata": metadata
                    },
                    timeout=60
                )
                print(f"{'='*70}\n")
                sys.stdout.flush()
                
                if upload_response.status_code == 200:
                    cloudinary_data = upload_response.json()
                    image_url = cloudinary_data.get('secure_url')
                    cloudinary_link = cloudinary_data.get('secure_url')
                    
                    print(f"\n{'='*70}")
                    print(f"✅ CLOUDINARY UPLOAD SUCCESSFUL!")
                    print(f"{'='*70}")
                    print(f"🔗 URL: {cloudinary_link}")
                    print(f"{'='*70}\n")
                    sys.stdout.flush()
                    
                    # Mark job as complete
                    print(f"💾 Marking job as complete in database...")
                    sys.stdout.flush()
                    
                    complete_response = requests.post(
                        f"{BACKEND_URL}/worker/job/{job_id}/complete",
                        json={
                            "image_url": cloudinary_link or image_url,
                            "thumbnail_url": image_url
                        },
                        timeout=10
                    )
                    
                    if complete_response.status_code == 200:
                        print(f"\n{'='*70}")
                        print(f"🎉 JOB COMPLETED SUCCESSFULLY!")
                        print(f"{'='*70}")
                        print(f"📋 Job ID: {job_id}")
                        print(f"🖼️  Image URL: {cloudinary_link}")
                        print(f"⏱️  Total time: Complete")
                        print(f"{'='*70}\n")
                        sys.stdout.flush()
                    else:
                        print(f"⚠️  Failed to mark job complete: {complete_response.status_code}")
                        sys.stdout.flush()
                    
                    print()
                else:
                    raise Exception(f"Cloudinary upload failed: {upload_response.status_code} - {upload_response.text}")
            else:
                # JSON response with URL - need to download and upload to Cloudinary
                print(f"📝 Response is JSON")
                data = response.json()
                
                if not data.get("success"):
                    raise Exception(data.get("error", "Unknown generation error"))
                
                temp_image_url = data.get("image_url") or data.get("url")
                cloudinary_link = data.get("cloudinary_link")
                
                print(f"✅ Generation complete!")
                print(f"🔗 Temporary Image URL: {temp_image_url}")
                
                # If no Cloudinary link, download image and upload to Cloudinary
                if not cloudinary_link and temp_image_url:
                    print(f"📥 Downloading image from temporary URL...")
                    try:
                        img_response = requests.get(temp_image_url, timeout=30)
                        if img_response.status_code == 200:
                            image_data = img_response.content
                            print(f"✅ Downloaded image ({len(image_data)} bytes)")
                            
                            # Prepare metadata
                            metadata = {
                                "prompt": job.get("prompt", ""),
                                "model": job.get("model", ""),
                                "aspect_ratio": job.get("aspect_ratio", ""),
                                "job_id": job_id,
                                "user_id": job.get("user_id", "")
                            }
                            
                            # Upload to Cloudinary
                            print(f"☁️  Uploading to Cloudinary...")
                            upload_response = requests.post(
                                f"{BACKEND_URL}/cloudinary/upload-image",
                                json={
                                    "image_data": base64.b64encode(image_data).decode('utf-8'),
                                    "file_name": f"job_{job_id}.png",
                                    "metadata": metadata
                                },
                                timeout=60
                            )
                            
                            if upload_response.status_code == 200:
                                cloudinary_data = upload_response.json()
                                cloudinary_link = cloudinary_data.get('secure_url')
                                print(f"✅ Uploaded to Cloudinary: {cloudinary_link}")
                            else:
                                print(f"⚠️  Cloudinary upload failed: {upload_response.status_code}")
                                print(f"   Will use temporary URL as fallback")
                        else:
                            print(f"⚠️  Failed to download image: {img_response.status_code}")
                    except Exception as download_err:
                        print(f"⚠️  Error downloading/uploading image: {download_err}")
                        print(f"   Will use temporary URL as fallback")
                
                if cloudinary_link:
                    print(f"☁️  Final Cloudinary Link: {cloudinary_link}")
                
                # Mark job as complete with Cloudinary URL (or fallback to temp URL)
                final_url = cloudinary_link or temp_image_url
                print(f"💾 Saving to database: {final_url}")
                
                complete_response = requests.post(
                    f"{BACKEND_URL}/worker/job/{job_id}/complete",
                    json={
                        "image_url": final_url,
                        "thumbnail_url": final_url
                    },
                    timeout=10
                )
                
                if complete_response.status_code == 200:
                    print(f"✅ Job {job_id} marked as complete!")
                else:
                    print(f"⚠️  Failed to mark job complete: {complete_response.status_code}")
                
                print()
        else:
            raise Exception(f"ComfyUI returned {response.status_code}: {response.text}")
            
    except Exception as e:
        error_message = str(e)
        print(f"❌ Error processing image job: {error_message}")
        
        # Check if it's a deployment error (rate limit, stopped endpoint, etc.)
        url_manager = get_modal_url_manager()
        if url_manager.is_limit_reached_error(error_message):
            print("[ALERT] Modal deployment should be marked inactive (stopped/limit/error)!")
            print(f"[ACTION] Current deployment ID: {url_manager.current_deployment_id}")
            print("[ACTION] Marking current deployment as inactive in database...")
            
            success = url_manager.mark_deployment_inactive()
            print(f"[ACTION] Mark inactive result: {success}")
            
            # Invalidate the cache in app.py so it fetches fresh URL
            if success:
                print("[ACTION] Invalidating app.py cache...")
                try:
                    invalidate_response = requests.post(
                        f"{BACKEND_URL}/invalidate-cache",
                        timeout=5
                    )
                    if invalidate_response.status_code == 200:
                        print("[OK] Cache invalidation triggered successfully")
                    else:
                        print(f"[WARN] Cache invalidation returned {invalidate_response.status_code}")
                except Exception as cache_err:
                    print(f"[WARN] Failed to invalidate cache: {cache_err}")
            
            print("[ACTION] Getting next active deployment...")
            next_deployment = url_manager.get_active_deployment()
            if next_deployment:
                print(f"[OK] Next deployment ready: #{next_deployment['deployment_number']}")
                print("[INFO] Retrying ALL pending jobs with new deployment...")
                
                # STEP 3: URL Rotation Recovery - retry all pending jobs
                retry_all_pending_jobs()
            else:
                print("[ERROR] No active deployments available!")
            
            # DO NOT mark job as failed - leave it pending for retry
            print("[TERMINATE] Terminating current task without marking complete")
            print("[INFO] Job status remains unchanged for automatic retry")
            return
        
        # DO NOT mark job as failed for ANY error - leave it pending for retry
        print("[TERMINATE] Terminating current task without marking as failed")
        print("[INFO] Job remains in pending status for automatic retry")
        print(f"[DEBUG] Error was: {error_message}")
        return

def fetch_all_pending_jobs():
    """Fetch all pending jobs from the database"""
    try:
        print("📥 Fetching all pending jobs from database...")
        response = requests.get(
            f"{BACKEND_URL}/worker/pending-jobs",
            timeout=10,
            verify=VERIFY_SSL
        )
        
        if response.status_code == 200:
            data = response.json()
            jobs = data.get("jobs", [])
            print(f"✅ Found {len(jobs)} pending job(s)")
            return jobs
        else:
            print(f"⚠️  Failed to fetch pending jobs: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error fetching pending jobs: {e}")
        return []


def process_all_pending_jobs():
    """Process all pending jobs (backlog catch-up)"""
    print("\n" + "="*60)
    print("🔄 BACKLOG CATCH-UP: Processing pending jobs")
    print("="*60)
    
    pending_jobs = fetch_all_pending_jobs()
    
    if not pending_jobs:
        print("✅ No pending jobs in backlog")
        print("="*60 + "\n")
        return
    
    print(f"📋 Processing {len(pending_jobs)} pending job(s)...\n")
    
    for idx, job in enumerate(pending_jobs, 1):
        job_id = job.get("job_id")
        job_type = job.get("job_type", "image")
        prompt = job.get("prompt", "")[:50]
        
        print(f"[{idx}/{len(pending_jobs)}] Processing job {job_id} ({job_type})")
        print(f"   Prompt: {prompt}...")
        
        try:
            # Get the appropriate URL based on job type
            url_manager = get_modal_url_manager()
            endpoint_url = url_manager.get_endpoint_url(job_type)
            
            if not endpoint_url:
                print(f"   ⚠️  No active deployment available, skipping for now")
                continue
            
            # Process based on job type
            if job_type == "video":
                process_video_job(job, endpoint_url)
            else:
                process_image_job(job, endpoint_url)
            
            print(f"   ✅ Job {job_id} processed successfully\n")
        except Exception as e:
            print(f"   ⚠️  Job {job_id} processing failed: {e}\n")
            # Don't stop - continue with next job
            continue
    
    print("="*60)
    print("✅ Backlog catch-up completed")
    print("="*60 + "\n")


def retry_all_pending_jobs():
    """Retry all pending jobs after URL rotation (called after deployment marked inactive)"""
    print("\n" + "="*60)
    print("🔄 URL ROTATION RECOVERY: Retrying pending jobs with new deployment")
    print("="*60)
    
    # Small delay to allow cache invalidation to propagate
    time.sleep(1)
    
    # Process all pending jobs with new active deployment
    process_all_pending_jobs()


async def realtime_listener():
    """
    Async listener for NEW pending jobs via Supabase Realtime
    Subscribes to INSERT events on jobs table where status='pending'
    """
    from supabase import acreate_client
    
    try:
        # Create async Supabase client
        print("🔌 Connecting to Supabase Realtime...")
        async_client = await acreate_client(SUPABASE_URL, SUPABASE_KEY)
        
        def handle_new_job(payload):
            """Callback for NEW job inserts (NON-BLOCKING)"""
            try:
                # Extract record from correct payload structure
                # Payload structure: {'data': {'type': 'INSERT', 'record': {...}}}
                data = payload.get("data", {})
                record = data.get("record", payload.get("new", payload.get("record", {})))
                
                if not record:
                    print(f"⚠️ No record found in payload: {payload}")
                    sys.stdout.flush()
                    return
                
                status = record.get("status")
                
                # Only process pending jobs
                if status != "pending":
                    return
                
                job_id = record.get("job_id")
                job_type = record.get("job_type", "image")
                
                print(f"\n{'='*70}")
                print(f"🔔 NEW JOB RECEIVED VIA REALTIME!")
                print(f"{'='*70}")
                print(f"📋 Job ID: {job_id}")
                print(f"📝 Type: {job_type}")
                print(f"🎯 Status: {status}")
                print(f"💬 Prompt: {record.get('prompt', '')[:50]}...")
                print(f"{'='*70}\n")
                sys.stdout.flush()
                
                # Get appropriate endpoint URL
                url_manager = get_modal_url_manager()
                endpoint_url = url_manager.get_endpoint_url(job_type)
                
                if not endpoint_url:
                    print(f"   ⚠️ No active deployment available")
                    sys.stdout.flush()
                    return
                
                # Process job in SEPARATE THREAD to avoid blocking the event loop
                # This allows the callback to return immediately and keep receiving events
                def process_in_thread():
                    try:
                        if job_type == "video":
                            process_video_job(record, endpoint_url)
                        else:
                            process_image_job(record, endpoint_url)
                        
                        print(f"\n{'='*70}")
                        print(f"✅ REALTIME JOB COMPLETED: {job_id}")
                        print(f"{'='*70}\n")
                        sys.stdout.flush()
                    except Exception as thread_err:
                        print(f"\n{'='*70}")
                        print(f"❌ ERROR PROCESSING JOB IN THREAD")
                        print(f"{'='*70}")
                        print(f"Error: {thread_err}")
                        import traceback
                        traceback.print_exc()
                        print(f"{'='*70}\n")
                        sys.stdout.flush()
                
                # Spawn thread and return immediately
                job_thread = threading.Thread(target=process_in_thread, daemon=True)
                job_thread.start()
                print(f"🧵 Job processing started in background thread")
                sys.stdout.flush()
                
            except Exception as e:
                print(f"\n{'='*70}")
                print(f"❌ ERROR IN REALTIME CALLBACK")
                print(f"{'='*70}")
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
                print(f"{'='*70}\n")
                sys.stdout.flush()
        
        # Subscribe to ALL events on jobs table (same as app.py - proven to work)
        # Filter for pending jobs in the callback instead
        channel = async_client.channel("job-worker-pending")

        # Subscribe to postgres changes (AsyncRealtimeChannel has no on_subscribe)
        subscription_result = await channel.on_postgres_changes(
            event="*",  # ALL events (same as realtime_manager.py)
            schema="public",
            table="jobs",
            callback=handle_new_job
        ).subscribe()
        
        print(f"✅ Subscription result: {subscription_result}")
        print("✅ Subscribed to new pending jobs (Realtime active)")
        print()
        print("⚠️  NOTE: If events don't arrive, check Supabase Dashboard:")
        print("   Database → Replication → Enable Realtime for 'jobs' table")
        print()
        print("=" * 60)
        print("⏳ LISTENING FOR NEW JOBS...")
        print("=" * 60)
        print()
        sys.stdout.flush()
        
        # Keep connection alive
        while True:
            await asyncio.sleep(1)
        
    except Exception as e:
        print(f"❌ Realtime listener error: {e}")
        import traceback
        traceback.print_exc()


def run_async_listener():
    """Run async listener in background thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(realtime_listener())


def start_realtime():
    """Start job worker with Realtime subscription"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Missing Supabase credentials!")
        print("Set SUPABASE_URL and SUPABASE_ANON_KEY in .env")
        sys.exit(1)
    
    # STEP 1: Process backlog on startup (ONE-TIME CATCH-UP)
    print("\n" + "="*60)
    print("🚀 WORKER STARTUP: Initial backlog catch-up")
    print("="*60)
    process_all_pending_jobs()
    print("✅ Initial backlog processed!\n")
    
    print("=" * 60)
    print("✅ JOB WORKER READY")
    print("=" * 60)
    print("💡 Switching to REALTIME mode (no more polling)")
    print("💡 Will receive instant notifications for new jobs")
    print("=" * 60)
    print()
    sys.stdout.flush()
    
    # STEP 2: Start Realtime listener in background thread
    realtime_thread = threading.Thread(target=run_async_listener, daemon=True)
    realtime_thread.start()
    
    # Keep main thread alive with heartbeat
    print("💓 Worker heartbeat every 30 seconds...")
    print("   Press Ctrl+C to stop")
    print()
    sys.stdout.flush()
    
    try:
        from datetime import datetime
        last_heartbeat = time.time()
        while True:
            time.sleep(5)
            
            # Heartbeat every 30 seconds
            if time.time() - last_heartbeat >= 30:
                print(f"💓 [{datetime.now().strftime('%H:%M:%S')}] Worker alive, listening for jobs...")
                sys.stdout.flush()
                last_heartbeat = time.time()
                
    except KeyboardInterrupt:
        print("\n\n🛑 Worker stopped by user (Ctrl+C)")
        sys.exit(0)

if __name__ == "__main__":
    start_realtime()
