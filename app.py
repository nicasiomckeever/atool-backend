import os
import re
import time
from typing import Iterable, Optional
from pathlib import Path

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Import our new modules
from auth import send_magic_link, verify_magic_link, logout, get_user_from_token
from jobs import create_job, get_job, get_user_jobs, update_job_status, update_job_result, cancel_job, get_job_stats, get_next_pending_job
from storage import upload_image_from_path, get_image_url, delete_image
from middleware import require_auth, get_current_user, extract_token
from supabase_client import supabase
from cloudinary_manager import get_cloudinary_manager
from modal_url_manager import get_modal_url_manager
from realtime_manager import ensure_realtime_started, get_realtime_manager
import coins  # Coin system module
import monetag_api  # MoneyTag API integration

app = Flask(__name__)

# Start shared Realtime connection manager on app startup
ensure_realtime_started()

# Configure CORS to allow requests from frontend
# This fixes the "No 'Access-Control-Allow-Origin' header" error
allowed_origins = [
    "http://localhost:3000",  # React dev server (npm run dev)
    "http://localhost:5173",  # Vite dev server (alternative port)
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "https://rasenai.qzz.io",  # Production frontend domain
    "https://api.rasenai.qzz.io",  # Backend API domain
    "https://api.rasenai.qzz.io:8080",  # Backend API with port
    "https://free.wispbyte.com",  # Wispbyte staging
    "https://atoolwispbyte.duckdns.org",
    "https://atoolwispbyte.duckdns.org:8080",
]

# Temporarily allow all origins for testing - comment out after verifying CORS works
# TODO: Remove this after testing and revert to specific origins list above
CORS(app)  # Allow all origins (TESTING ONLY - NOT PRODUCTION SAFE)

# Debug: Print allowed origins on startup
print("\n" + "="*70)
print("🔐 CORS CONFIGURATION:")
print("="*70)
print("⚠️  WARNING: ALL ORIGINS ALLOWED (TESTING MODE)")
print("="*70 + "\n")

load_dotenv()


# Allow OPTIONS preflight to bypass auth checks (important for CORS preflight)
@app.before_request
def _allow_options_preflight():
    if request.method == "OPTIONS":
        return ('', 200)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required.")
    return value


# Discord Configuration
BOT_TOKEN = _require_env("DISCORD_BOT_TOKEN")
CHANNEL_ID = _require_env("DISCORD_CHANNEL_ID")

# Cache for the latest URL (infinite - no expiry)
cached_url = None
cached_url_timestamp = None
cache_invalidation_flag = False  # Flag to trigger cache refresh


def _extract_ngrok_url(values: Iterable[Optional[str]]) -> Optional[str]:
    """Return the first valid ngrok or Modal URL found within the provided texts."""
    # Pattern for Modal URLs: https://[subdomain]--[app-name].modal.run
    modal_pattern = re.compile(
        r"https?://[a-zA-Z0-9\-]+--[a-zA-Z0-9\-]+\.modal\.run(?:/[\w\-./?=&%#!:+~]*)?",
        re.IGNORECASE,
    )
    
    # Pattern for ngrok URLs (legacy support)
    ngrok_pattern = re.compile(
        r"https?://[a-zA-Z0-9\-.]+\.ngrok[a-z0-9\-]*\.(?:io|app|dev|com)(?:/[\w\-./?=&%#!:+~]*)?",
        re.IGNORECASE,
    )

    for value in values:
        if not value:
            continue
        
        # Try Modal pattern first (new format)
        match = modal_pattern.search(value)
        if match:
            cleaned = match.group(0).strip(" <>`'\",)")
            return cleaned
        
        # Fallback to ngrok pattern (legacy)
        match = ngrok_pattern.search(value)
        if match:
            cleaned = match.group(0).strip(" <>`'\",)")
            return cleaned
    
    return None

@app.route("/get-url", methods=["GET"])
def get_url():
    """Fetch the latest Modal URL from Supabase (NEW METHOD)"""
    global cached_url, cached_url_timestamp, cache_invalidation_flag
    
    print("\n" + "="*60)
    print("🔍 FETCHING MODAL URL FROM SUPABASE")
    print("="*60)
    
    # Check if cache invalidation was triggered
    if cache_invalidation_flag:
        print("🔄 Cache invalidation flag set - fetching fresh URL...")
        cached_url = None
        cached_url_timestamp = None
        cache_invalidation_flag = False
    
    # Check if cache exists (infinite cache - no age limit)
    if cached_url and cached_url_timestamp:
        cache_age = time.time() - cached_url_timestamp
        print(f"💾 Using cached URL (age: {int(cache_age)}s, no expiry)")
        print(f"🔗 Cached URL: {cached_url}")
        print("="*60 + "\n")
        return jsonify({
            "success": True,
            "url": cached_url,
            "cached": True,
            "cache_age_seconds": int(cache_age),
            "source": "supabase"
        }), 200
    
    # Fetch fresh Modal deployment from Supabase
    try:
        url_manager = get_modal_url_manager()
        
        # Get job_type from query params (default to "image" for backward compatibility)
        job_type = request.args.get('job_type', 'image')
        
        # Get the appropriate endpoint URL from active deployment
        modal_url = url_manager.get_endpoint_url(job_type)
        
        if modal_url:
            # Cache the URL
            cached_url = modal_url
            cached_url_timestamp = time.time()
            
            print(f"✅ SUCCESS! Found active Modal deployment")
            print(f"🔗 {job_type.upper()} URL: {modal_url}")
            print(f"💾 Cached for future requests")
            print("="*60 + "\n")
            
            return jsonify({
                "success": True,
                "url": modal_url,
                "job_type": job_type,
                "cached": False,
                "source": "modal_deployments"
            }), 200
        else:
            print(f"⚠️  No active Modal deployments available")
            print(f"💡 Make sure to run the migration and add deployment URLs")
            print("="*60 + "\n")
            
            return jsonify({
                "success": False,
                "url": None,
                "error": "No fresh Modal URLs available. Please deploy Modal and run notify_discord.py",
                "source": "supabase"
            }), 503
    
    except Exception as e:
        print(f"❌ ERROR fetching from Supabase: {e}")
        print("="*60 + "\n")
        
        return jsonify({
            "success": False,
            "url": None,
            "error": f"Failed to fetch Modal URL: {str(e)}",
            "source": "supabase"
        }), 500


@app.route("/invalidate-cache", methods=["POST"])
def invalidate_cache():
    """Invalidate the cached Modal URL (called by job worker when deployment is marked inactive)"""
    global cache_invalidation_flag
    
    print("\n" + "="*60)
    print("🔄 CACHE INVALIDATION REQUEST")
    print("="*60)
    
    cache_invalidation_flag = True
    
    print("✅ Cache invalidation flag set")
    print("💡 Next /get-url request will fetch fresh from database")
    print("="*60 + "\n")
    
    return jsonify({
        "success": True,
        "message": "Cache will be invalidated on next request"
    }), 200


def _legacy_discord_url_fetch():
    """Legacy Discord URL fetching logic (kept for reference)"""
    global cached_url, cached_url_timestamp
    
    headers = {"Authorization": f"Bot {BOT_TOKEN}"}
    url = f"https://discord.com/api/v10/channels/{CHANNEL_ID}/messages?limit=20"
    
    print(f"📡 Requesting Discord API...")
    print(f"   Channel ID: {CHANNEL_ID}")
    print(f"   Checking last 20 messages...")
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        print(f"📥 Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ ERROR: Discord API returned {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return jsonify({
                "success": False,
                "url": cached_url,
                "error": f"Discord API error: {response.status_code}",
                "cached": True if cached_url else False
            }), 200
        
        messages = response.json()
        print(f"✅ Successfully fetched {len(messages)} messages")
        print(f"ℹ️  Messages are in descending order (newest first)")
        
        # Search for ngrok URL in messages
        # Updated regex to handle Discord link formatting (<url>), various ngrok domains, and paths
        # Use non-capturing group (?:...) to get full match with findall
        print(f"\n🔎 Searching for LATEST ngrok URL in messages...")
        for i, msg in enumerate(messages):
            content = msg.get("content", "")
            author = msg.get("author", {}).get("username", "Unknown")
            timestamp = msg.get("timestamp", "Unknown")
            
            print(f"\n   Message {i+1}:")
            print(f"   Author: {author}")
            print(f"   Time: {timestamp}")
            print(f"   Content: '{content}'")
            
            # Also check embeds for URLs
            embeds = msg.get("embeds", [])
            if embeds:
                print(f"   Embeds: {len(embeds)} found")
                for embed in embeds:
                    if 'url' in embed:
                        print(f"   Embed URL: {embed['url']}")
                    if 'description' in embed:
                        print(f"   Embed Description: {embed['description']}")
            
            # Check attachments
            attachments = msg.get("attachments", [])
            if attachments:
                print(f"   Attachments: {len(attachments)} found")
            
            # Check message content first
            found_url = _extract_ngrok_url([content])

            # Check embeds if not found in content
            if not found_url and embeds:
                for embed in embeds:
                    embed_values = [
                        embed.get("url"),
                        embed.get("description"),
                        embed.get("title"),
                    ]

                    footer = embed.get("footer", {})
                    if isinstance(footer, dict):
                        embed_values.append(footer.get("text"))

                    for field in embed.get("fields", []) or []:
                        if isinstance(field, dict):
                            embed_values.append(field.get("value"))

                    found_url = _extract_ngrok_url(embed_values)
                    if not found_url:
                        found_url = _extract_ngrok_url([str(embed)])
                    if found_url:
                        print(f"   ℹ️  Found URL in embed")
                        break

            # Check attachments as a fallback
            if not found_url and attachments:
                attachment_urls = []
                for att in attachments:
                    if not isinstance(att, dict):
                        continue
                    attachment_urls.extend(
                        [att.get("url"), att.get("proxy_url"), att.get("href"), str(att)]
                    )
                found_url = _extract_ngrok_url(attachment_urls)

            # Final fallback: scan the raw message payload
            if not found_url:
                found_url = _extract_ngrok_url([str(msg)])

            if found_url:
                cached_url = found_url  # Cache the URL
                cached_url_timestamp = time.time()  # Set cache timestamp
                
                print(f"   ✅ FOUND URL: {found_url}")
                
                from datetime import datetime
                msg_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                
                print(f"\n✅ SUCCESS! Found LATEST ngrok URL!")
                print(f"🔗 URL: {found_url}")
                print(f"📅 From message #{i+1} (most recent)")
                print(f"🕐 Posted at: {msg_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                print(f"👤 By: {author}")
                print(f"💾 Cached for future requests")
                print(f"ℹ️  Note: Picking from message #{i+1} because messages are ordered newest-first")
                print("="*60 + "\n")
                
                return jsonify({
                    "success": True,
                    "url": found_url,
                    "message": content,
                    "timestamp": msg.get("timestamp"),
                    "cached": False
                }), 200
        
        # No URL found
        print(f"\n⚠️  No ngrok URL found in any messages")
        print(f"\n💡 TROUBLESHOOTING:")
        print(f"   - If message content is empty, enable 'Message Content Intent' in Discord Developer Portal")
        print(f"   - Go to: https://discord.com/developers/applications")
        print(f"   - Select your bot → Bot → Privileged Gateway Intents → Enable 'Message Content Intent'")
        print(f"   - Save changes and restart the bot")
        if cached_url:
            print(f"\n💾 Using cached URL: {cached_url}")
        else:
            print(f"\n❌ No cached URL available")
        print("="*60 + "\n")
        
        return jsonify({
            "success": False,
            "url": cached_url,
            "error": "No ngrok URL found in recent messages",
            "cached": True if cached_url else False
        }), 200
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ REQUEST FAILED!")
        print(f"   Error: {str(e)}")
        print("="*60 + "\n")
        
        return jsonify({
            "success": False,
            "url": cached_url,
            "error": f"Request failed: {str(e)}",
            "cached": True if cached_url else False
        }), 200


@app.route("/generate", methods=["POST"])
def generate():
    """Proxy endpoint to forward generation requests to ComfyUI"""
    global cached_url
    
    print("\n" + "="*60)
    print("🎨 GENERATE REQUEST RECEIVED")
    print("="*60)
    
    # Get the latest URL if not cached
    if not cached_url:
        print("⚠️  No cached URL, fetching from Discord...")
        url_response = get_url()
        url_data = url_response[0].get_json()
        if not url_data.get("success"):
            print("❌ Failed to get URL from Discord")
            return jsonify({
                "success": False,
                "error": "No ComfyUI URL available. Please check Discord for the latest ngrok URL."
            }), 503
        cached_url = url_data.get("url")
        print(f"✅ Got URL: {cached_url}")
    else:
        print(f"💾 Using cached URL: {cached_url}")
    
    # Get prompt from request
    data = request.get_json()
    prompt = data.get("prompt", "")
    aspect_ratio = data.get("aspect_ratio", "1:1")

    if not isinstance(aspect_ratio, str) or not aspect_ratio.strip():
        aspect_ratio = "1:1"
    
    if not prompt:
        print("❌ No prompt provided")
        return jsonify({
            "success": False,
            "error": "No prompt provided"
        }), 400
    
    print(f"📝 Prompt: {prompt[:100]}...")
    print(f"📐 Aspect Ratio: {aspect_ratio}")
    
    try:
        # Forward request to ComfyUI
        comfy_url = f"{cached_url}/generate"
        print(f"📤 Forwarding to: {comfy_url}")
        
        response = requests.post(
            comfy_url,
            json={"prompt": prompt, "aspect_ratio": aspect_ratio},
            timeout=60
        )
        
        print(f"📥 ComfyUI Response: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Generation successful!")
            print("="*60 + "\n")
            return jsonify({
                "success": True,
                **response.json()
            }), 200
        else:
            print(f"⚠️  ComfyUI returned error: {response.status_code}")
            print("="*60 + "\n")
            return jsonify({
                "success": False,
                "error": f"ComfyUI returned status {response.status_code}",
                "details": response.text
            }), response.status_code
            
    except requests.exceptions.RequestException as e:
        # Clear cache on connection error
        print(f"❌ Connection failed: {str(e)}")
        print("🗑️  Clearing cached URL")
        print("="*60 + "\n")
        cached_url = None
        return jsonify({
            "success": False,
            "error": f"Failed to connect to ComfyUI: {str(e)}",
            "suggestion": "The ngrok URL may have expired. Check Discord for a new URL."
        }), 503


@app.route("/list-models", methods=["GET"])
def list_models():
    """Fetch available models from the current ComfyUI endpoint."""
    global cached_url

    print("\n" + "=" * 60)
    print("🧠 LIST MODELS REQUEST RECEIVED")
    print("=" * 60)

    target_override = request.args.get("target")
    force_refresh = request.args.get("force") in {"1", "true", "yes"}

    if target_override:
        cleaned_target = target_override.rstrip("/")
        print(f"🎯 Target override provided: {cleaned_target}")
        cached_url = cleaned_target

    if force_refresh:
        print("🔄 Force refresh requested - clearing cached URL")
        cached_url = None

    if not cached_url:
        print("⚠️  No cached URL detected, fetching latest from Discord...")
        url_response = get_url()
        url_data = url_response[0].get_json()
        if not url_data.get("success"):
            print("❌ Unable to obtain ComfyUI URL")
            print("=" * 60 + "\n")
            return jsonify({
                "success": False,
                "error": "No ComfyUI URL available. Please check Discord for the latest ngrok URL.",
            }), 503
        cached_url = url_data.get("url")
        print(f"✅ Cached URL set to: {cached_url}")
    else:
        print(f"💾 Using cached URL: {cached_url}")

    # Use /models for Modal API, /list-models for legacy ngrok
    if "modal.run" in cached_url:
        target_url = f"{cached_url}/models"
    else:
        target_url = f"{cached_url}/list-models"
    print(f"📡 Requesting models from: {target_url}")

    try:
        response = requests.get(target_url, timeout=20)
        print(f"📥 Response status: {response.status_code}")

        try:
            data = response.json()
        except ValueError:
            data = None

        if data is not None:
            if isinstance(data, list):
                print(f"📦 Received list with {len(data)} entries")
            elif isinstance(data, dict):
                keys_preview = ", ".join(list(data.keys())[:5])
                print(f"📚 Received dict with keys: {keys_preview}")
        else:
            preview = response.text[:300]
            print("📄 Response (truncated):")
            print(preview)

        if response.status_code != 200:
            print("⚠️  Non-200 response received from ComfyUI")
            print("=" * 60 + "\n")
            return jsonify({
                "success": False,
                "error": f"ComfyUI returned status {response.status_code}",
                "details": data if data is not None else response.text,
            }), response.status_code

        # Normalize response format for different APIs
        # Modal API returns: {"count": 1, "models": [...]}
        # Legacy API returns: [...] or {"models": [...]}
        if isinstance(data, dict) and "models" in data:
            # Normalize Modal format to a flat list of model names for the frontend
            models_field = data["models"]
            models = []
            if isinstance(models_field, dict):
                # Include common image model dirs plus diffusion_models (for Qwen Image Edit)
                preferred_keys = ["unet", "checkpoints", "diffusion_models"]
                for key in preferred_keys:
                    for item in models_field.get(key, []) or []:
                        name = item.get("name") if isinstance(item, dict) else str(item)
                        if name and name not in models:
                            models.append(name)

                # Ensure Qwen Image Edit appears even if not captured above
                # Look through any dict values for entries that include 'qwen'
                if not any("qwen" in m.lower() for m in models):
                    for key, items in models_field.items():
                        if not isinstance(items, (list, tuple)):
                            continue
                        for item in items:
                            name = item.get("name") if isinstance(item, dict) else str(item)
                            if name and "qwen" in name.lower() and name not in models:
                                models.append(name)

            elif isinstance(models_field, list):
                # Legacy list format
                models = [m.get("name", m) if isinstance(m, dict) else str(m) for m in models_field]
            else:
                models = []
            print(f"📋 Normalized response: {len(models)} image models")
        elif isinstance(data, list):
            # Already a list of models
            models = data
            print(f"📋 Response is already a list: {len(models)} models")
        else:
            # Unexpected format
            models = []
            print(f"⚠️ Unexpected response format: {type(data)}")
        
        print("=" * 60 + "\n")
        return jsonify({"success": True, "models": models}), 200
        
    except requests.exceptions.RequestException as exc:
        print(f"❌ Failed to fetch models: {str(exc)}")
        print("=" * 60 + "\n")
        return jsonify({
            "success": False,
            "error": f"Failed to fetch models: {str(exc)}",
            "suggestion": "The ngrok URL may have expired. Check Discord for a new URL.",
        }), 503



@app.route("/list-video-models", methods=["GET"])
def list_video_models():
    """Fetch available video models from the Modal video API endpoint"""
    global cached_url
    
    print("\n" + "=" * 60)
    print("🎬 LIST VIDEO MODELS REQUEST RECEIVED")
    print("=" * 60)
    
    if not cached_url:
        print("⚠️  No cached URL detected, fetching latest from Discord...")
        url_response = get_url()
        url_data = url_response[0].get_json()
        if not url_data.get("success"):
            print("❌ Unable to obtain API URL")
            print("=" * 60 + "\n")
            return jsonify({
                "success": False,
                "error": "No API URL available.",
            }), 503
        cached_url = url_data.get("url")
        print(f"✅ Cached URL set to: {cached_url}")
    else:
        print(f"💾 Using cached URL: {cached_url}")
    
    # Use the MAIN serve endpoint which also has video-models endpoint in simple_api.py
    # Don't use serve-video as it has H100 provisioning delays
    video_api_url = cached_url
    
    target_url = f"{video_api_url}/video-models"
    print(f"📡 Requesting video models from: {target_url}")
    
    try:
        # Increase timeout to 300 seconds (5 minutes) for Modal cold starts + custom node loading
        print(f"⏳ Waiting up to 300 seconds for response (Modal may be cold starting)...")
        response = requests.get(target_url, timeout=300)
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code != 200:
            print("⚠️  Non-200 response from video API")
            print("=" * 60 + "\n")
            return jsonify({
                "success": False,
                "error": f"Video API returned status {response.status_code}",
            }), response.status_code
        
        data = response.json() or {}
        print("✅ Video models fetched successfully")
        # Normalize to simple list of model names for frontend select
        models = []
        if isinstance(data, dict):
            items = data.get("models") or data.get("video_model_dirs") or []
        elif isinstance(data, list):
            items = data
        else:
            items = []
        for it in items:
            if isinstance(it, dict):
                name = it.get("name") or it.get("model")
                if name and name not in models:
                    models.append(name)
            else:
                models.append(str(it))
        print(f"📋 Video models normalized: {models}")
        print("=" * 60 + "\n")
        return jsonify({"success": True, "models": models}), 200
        
    except requests.exceptions.Timeout as exc:
        print(f"⏱️  Request timeout (likely Modal cold start): {str(exc)}")
        print("💡 The Modal video endpoint may be starting up. Please try again in 30-60 seconds.")
        print("=" * 60 + "\n")
        return jsonify({
            "success": False,
            "error": "Video API timeout - the endpoint may be cold starting. Please wait 30-60 seconds and try again.",
            "suggestion": "Modal cold starts can take time. Retry in a moment."
        }), 503
    except requests.exceptions.RequestException as exc:
        print(f"❌ Failed to fetch video models: {str(exc)}")
        print("=" * 60 + "\n")
        return jsonify({
            "success": False,
            "error": f"Failed to fetch video models: {str(exc)}",
            "suggestion": "Check that the video API is deployed: modal deploy modal_app.py"
        }), 503


@app.route("/generate-video", methods=["POST"])
def generate_video():
    """Proxy endpoint to forward video generation requests to ComfyUI Modal API"""
    global cached_url
    
    print("\n" + "=" * 60)
    print("🎬 VIDEO GENERATION REQUEST RECEIVED")
    print("=" * 60)
    
    # Get the latest URL if not cached
    if not cached_url:
        print("⚠️  No cached URL, fetching from Discord...")
        url_response = get_url()
        url_data = url_response[0].get_json()
        if not url_data.get("success"):
            print("❌ Failed to get URL from Discord")
            return jsonify({
                "success": False,
                "error": "No ComfyUI URL available. Please check Discord for the latest URL."
            }), 503
        cached_url = url_data.get("url")
        print(f"✅ Got URL: {cached_url}")
    else:
        print(f"💾 Using cached URL: {cached_url}")
    
    # Get request data
    data = request.get_json()
    prompt = data.get("prompt", "")
    model = data.get("model", "ltx-video-13b")
    width = data.get("width", 768)
    height = data.get("height", 512)
    num_frames = data.get("num_frames", 81)
    fps = data.get("fps", 25)
    
    if not prompt:
        print("❌ No prompt provided")
        return jsonify({
            "success": False,
            "error": "No prompt provided"
        }), 400
    
    print(f"📝 Prompt: {prompt[:100]}...")
    print(f"🤖 Model: {model}")
    print(f"📐 Resolution: {width}x{height}")
    print(f"🎞️  Frames: {num_frames} @ {fps}fps")
    
    try:
        # Forward request to ComfyUI Modal API
        comfy_url = f"{cached_url}/generate-video"
        print(f"📤 Forwarding to: {comfy_url}")
        
        response = requests.post(
            comfy_url,
            json={
                "prompt": prompt,
                "model": model,
                "width": width,
                "height": height,
                "num_frames": num_frames,
                "fps": fps
            },
            timeout=600  # 10 minutes for video generation
        )
        
        print(f"📥 ComfyUI Response: {response.status_code}")
        
        if response.status_code == 200:
            # Video generation returns the video file directly
            print("✅ Video generation successful!")
            print("=" * 60 + "\n")
            
            # Return the video as a file
            from flask import Response
            return Response(
                response.content,
                mimetype='video/mp4',
                headers={
                    'Content-Type': 'video/mp4',
                    'Content-Disposition': 'attachment; filename="generated_video.mp4"'
                }
            )
        else:
            print(f"⚠️  ComfyUI returned error: {response.status_code}")
            print("=" * 60 + "\n")
            return jsonify({
                "success": False,
                "error": f"ComfyUI returned status {response.status_code}",
                "details": response.text
            }), response.status_code
            
    except requests.exceptions.RequestException as e:
        # Clear cache on connection error
        print(f"❌ Connection failed: {str(e)}")
        print("🗑️  Clearing cached URL")
        print("=" * 60 + "\n")
        cached_url = None
        return jsonify({
            "success": False,
            "error": f"Failed to connect to ComfyUI: {str(e)}",
            "suggestion": "The URL may have expired. Check Discord for a new URL."
        }), 503


# ============================================
# Authentication Endpoints
# ============================================

@app.route("/auth/magic-link", methods=["POST"])
def auth_send_magic_link():
    """Send magic link to user's email"""
    data = request.get_json()
    email = data.get("email")
    
    if not email:
        return jsonify({
            "success": False,
            "error": "Email is required"
        }), 400
    
    result = send_magic_link(email)
    
    if result["success"]:
        return jsonify(result), 200
    else:
        return jsonify(result), 400


@app.route("/auth/verify", methods=["GET"])
def auth_verify_magic_link():
    """Verify magic link token"""
    token = request.args.get("token")
    
    if not token:
        return jsonify({
            "success": False,
            "error": "Token is required"
        }), 400
    
    result = verify_magic_link(token)
    
    if result["success"]:
        return jsonify(result), 200
    else:
        return jsonify(result), 400


@app.route("/auth/me", methods=["GET"])
@require_auth
def auth_get_current_user():
    """Get current user info with full details from database"""
    user_context = get_current_user()
    
    if not user_context or not user_context.get("success"):
        return jsonify({
            "success": False,
            "error": "Not authenticated"
        }), 401
    
    try:
        # Get full user data from database
        user_response = supabase.table("users").select("*").eq("id", user_context["user_id"]).execute()
        
        if not user_response.data:
            return jsonify({
                "success": False,
                "error": "User not found"
            }), 404
        
        user = user_response.data[0]
        
        return jsonify({
            "success": True,
            "user": {
                "id": user["id"],
                "email": user["email"],
                "credits": user["credits"],
                "created_at": user["created_at"],
                "last_login": user.get("last_login"),
                "is_active": user.get("is_active", True)
            }
        }), 200
    except Exception as e:
        print(f"❌ Error getting user: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/auth/logout", methods=["POST"])
@require_auth
def auth_logout():
    """Logout current user"""
    token = extract_token(request)
    
    if token:
        result = logout(token)
        return jsonify(result), 200
    else:
        return jsonify({
            "success": False,
            "error": "No token provided"
        }), 400


# ============================================
# Job Endpoints
# ============================================

@app.route("/jobs", methods=["POST"])
@require_auth
def jobs_create():
    """Create a new job"""
    user = get_current_user()
    
    # Handle both JSON and multipart/form-data
    if request.content_type and 'multipart/form-data' in request.content_type:
        # Form data with file upload
        print(f"\n📋 Received multipart/form-data request")
        print(f"   Form keys: {list(request.form.keys())}")
        print(f"   File keys: {list(request.files.keys())}")
        
        prompt = request.form.get("prompt")
        model = request.form.get("model", "flux-dev")
        aspect_ratio = request.form.get("aspect_ratio", "1:1")
        negative_prompt = request.form.get("negative_prompt", "")
        job_type = request.form.get("job_type", "image")
        duration = int(request.form.get("duration", 5))  # Duration in seconds for videos
        
        # Handle uploaded image
        uploaded_image = request.files.get("image")
        image_url = None
        
        if uploaded_image:
            print(f"\n📸 Image file received:")
            print(f"   Filename: {uploaded_image.filename}")
            print(f"   Size: {len(uploaded_image.read())} bytes")
            uploaded_image.seek(0)  # Reset file pointer after reading size
            
            try:
                # Save temporarily and upload to Cloudinary
                import tempfile
                import uuid
                temp_dir = tempfile.gettempdir()
                temp_filename = f"{uuid.uuid4()}_{uploaded_image.filename}"
                temp_path = os.path.join(temp_dir, temp_filename)
                
                uploaded_image.save(temp_path)
                print(f"✅ Saved uploaded image to: {temp_path}")
                
                # Upload to Cloudinary
                storage = get_cloudinary_manager()
                print(f"☁️  Uploading to Cloudinary...")
                cloudinary_result = storage.upload_image(temp_path, folder_name="user_uploads")
                print(f"   Result: {cloudinary_result}")
                
                image_url = cloudinary_result.get('secure_url') or cloudinary_result.get('url')
                if image_url:
                    print(f"✅ Uploaded to Cloudinary: {image_url}")
                else:
                    print(f"❌ No URL in Cloudinary result: {cloudinary_result}")
                
                # Clean up temp file
                os.remove(temp_path)
                
            except Exception as e:
                import traceback
                print(f"❌ Error handling uploaded image: {e}")
                print(f"   Traceback: {traceback.format_exc()}")
                return jsonify({
                    "success": False,
                    "error": f"Failed to process uploaded image: {str(e)}"
                }), 400
        else:
            print(f"⚠️  No image file in request.files")
    else:
        # Regular JSON request
        data = request.get_json()
        prompt = data.get("prompt")
        model = data.get("model", "flux-dev")
        aspect_ratio = data.get("aspect_ratio", "1:1")
        negative_prompt = data.get("negative_prompt", "")
        job_type = data.get("job_type", "image")
        duration = int(data.get("duration", 5))  # Duration in seconds for videos
        image_url = data.get("image_url", None)  # For passing existing URLs
    
    if not prompt:
        return jsonify({
            "success": False,
            "error": "Prompt is required"
        }), 400
    
    # ============================================================================
    # COIN SYSTEM CHECK - Check balance before creating job
    # ============================================================================
    user_id = user["user_id"]
    required_coins = coins.GENERATION_COST  # 5 coins per generation
    
    print(f"💰 Checking coin balance for user {user_id}")
    
    # Check if user has enough coins
    if not coins.has_sufficient_coins(user_id, required_coins):
        balance = coins.get_coin_balance(user_id)
        coins_needed = coins.get_coins_needed(user_id, required_coins)
        
        print(f"⚠️ Insufficient coins: has {balance}, needs {required_coins}")
        
        return jsonify({
            "success": False,
            "error": "insufficient_coins",
            "message": f"Not enough coins. You need {coins_needed} more coin(s) to generate.",
            "balance": balance,
            "required": required_coins,
            "coins_needed": coins_needed
        }), 402  # 402 Payment Required
    
    # Debug logging
    print(f"📋 Creating job:")
    print(f"   Job Type: {job_type}")
    print(f"   Model: {model}")
    print(f"   Duration: {duration}s")
    print(f"   Image URL: {image_url}")
    
    result = create_job(
        user_id=user_id,
        prompt=prompt,
        model=model,
        aspect_ratio=aspect_ratio,
        negative_prompt=negative_prompt,
        job_type=job_type,
        duration=duration,  # Pass duration
        image_url=image_url  # Pass the uploaded image URL
    )
    
    if result["success"]:
        # ============================================================================
        # COIN SYSTEM - Deduct coins after job creation
        # ============================================================================
        job_id = result["job"]["id"]
        
        print(f"💸 Deducting {required_coins} coins for job {job_id}")
        
        deduct_success = coins.deduct_coins(
            user_id=user_id,
            coins_amount=required_coins,
            reference_id=job_id,
            description=f"Generated {job_type}: {prompt[:50]}..."
        )
        
        if deduct_success:
            # Get updated balance
            updated_stats = coins.get_coin_stats(user_id)
            print(f"✅ Coins deducted. New balance: {updated_stats['balance']}")
            
            # Add coin info to response
            result["coins_deducted"] = required_coins
            result["coins_remaining"] = updated_stats["balance"]
            result["generations_available"] = updated_stats["generations_available"]
        else:
            print(f"⚠️ Warning: Coins deduction failed for job {job_id}")
            # Job was already created, so we don't fail the request
            # But we log the issue for manual review
        
        return jsonify(result), 201
    else:
        return jsonify(result), 400


@app.route("/jobs", methods=["GET"])
@require_auth
def jobs_get_all():
    """Get all jobs for current user"""
    user = get_current_user()
    status = request.args.get("status")
    limit = int(request.args.get("limit", 50))
    
    result = get_user_jobs(user["user_id"], status, limit)
    
    if result["success"]:
        return jsonify(result), 200
    else:
        return jsonify(result), 400


@app.route("/jobs/<job_id>", methods=["GET"])
@require_auth
def jobs_get_one(job_id):
    """Get specific job"""
    result = get_job(job_id)
    
    if result["success"]:
        return jsonify(result), 200
    else:
        return jsonify(result), 404


@app.route("/jobs/<job_id>", methods=["DELETE"])
@require_auth
def jobs_cancel(job_id):
    """Cancel a job"""
    user = get_current_user()
    result = cancel_job(job_id, user["user_id"])
    
    if result["success"]:
        return jsonify(result), 200
    else:
        return jsonify(result), 400


@app.route("/jobs/stats", methods=["GET"])
@require_auth
def jobs_get_stats():
    """Get job statistics"""
    user = get_current_user()
    result = get_job_stats(user["user_id"])
    
    if result["success"]:
        return jsonify(result), 200
    else:
        return jsonify(result), 400


@app.route("/jobs/in-progress", methods=["GET"])
@require_auth
def jobs_get_in_progress():
    """Get user's last pending or running job (for resume after refresh/login)"""
    user = get_current_user()
    job_type = request.args.get("job_type", "image")  # Default to image
    
    print(f"📥 Fetching in-progress job for user {user['user_id']}, type: {job_type}")
    
    try:
        # Query for last pending or running job for this user and job type
        response = supabase.table("jobs").select("*").eq(
            "user_id", user["user_id"]
        ).eq(
            "job_type", job_type
        ).in_(
            "status", ["pending", "running"]
        ).order("created_at", desc=True).limit(1).execute()
        
        if response.data and len(response.data) > 0:
            job = response.data[0]
            print(f"   ✅ Found in-progress job: {job['job_id']} (status: {job['status']})")
            return jsonify({
                "success": True,
                "job": job
            }), 200
        else:
            print(f"   💤 No in-progress jobs")
            return jsonify({
                "success": False,
                "message": "No in-progress jobs found"
            }), 200
    except Exception as e:
        print(f"   ❌ Error fetching in-progress job: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/jobs/<job_id>/stream", methods=["GET"])
@require_auth
def jobs_stream_status(job_id):
    """Stream job status updates via Server-Sent Events (SSE)
    
    Uses SHARED Supabase Realtime connection (scalable to thousands of users!)
    - Single global WebSocket connection for all jobs
    - Real push-based updates (no polling)
    - Minimal resource usage (1 thread, 1 connection)
    - Automatic event routing to correct clients
    """
    from flask import Response, stream_with_context
    import json
    import queue
    
    user = get_current_user()
    
    print(f"📡 SSE stream requested for job {job_id} by user {user['user_id']}")
    
    # Verify user owns this job
    try:
        job_response = supabase.table("jobs").select("*").eq("job_id", job_id).single().execute()
        if not job_response.data or job_response.data.get("user_id") != user["user_id"]:
            return jsonify({"success": False, "error": "Job not found or unauthorized"}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    
    print(f"✅ SSE stream authorized for job {job_id}")
    
    # Create queue for this client
    client_queue = queue.Queue(maxsize=100)
    
    # Subscribe to job updates via shared manager
    realtime_manager = get_realtime_manager()
    realtime_manager.subscribe_to_job(job_id, client_queue)
    
    def generate():
        """Generate SSE events from shared realtime connection"""
        try:
            # Send initial connection event
            yield f"data: {json.dumps({'type': 'connected', 'job_id': job_id})}\n\n"
            
            # Stream updates from queue
            while True:
                try:
                    # Wait for update with timeout (30s keepalive)
                    payload = client_queue.get(timeout=30)
                    
                    # Check for error
                    if isinstance(payload, dict) and "error" in payload:
                        print(f"⚠️ Realtime error: {payload['error']}")
                        yield f"data: {json.dumps({'type': 'error', 'error': payload['error']})}\n\n"
                        break
                    
                    # Extract job data from realtime payload
                    job_data = payload.get('new') if isinstance(payload, dict) else None
                    
                    if job_data:
                        print(f"📤 SSE update: {job_id} - {job_data.get('status')}")
                        
                        event_data = {
                            'type': 'update',
                            'event': payload.get('eventType', 'UPDATE'),
                            'job': job_data
                        }
                        
                        yield f"data: {json.dumps(event_data)}\n\n"
                        
                        # Close stream if job is complete
                        if job_data.get('status') in ['completed', 'failed', 'cancelled']:
                            print(f"✅ Job {job_id} finished with status: {job_data.get('status')}")
                            break
                    
                except queue.Empty:
                    # Send keepalive ping
                    yield f": keepalive\n\n"
                    
        except GeneratorExit:
            print(f"🔌 Client disconnected from job {job_id} stream")
        finally:
            # Unsubscribe from shared manager
            realtime_manager.unsubscribe_from_job(job_id, client_queue)
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )


# ============================================
# Worker Endpoints (Internal API for workers)
# ============================================

@app.route("/worker/next-job", methods=["GET"])
def worker_get_next_job():
    """Get the next pending job for worker to process"""
    result = get_next_pending_job()
    
    print(f"🔍 Worker requesting next job...")
    print(f"   Result: {result}")
    
    # get_next_pending_job() already returns {"success": True, "job": {...}}
    if result.get("success") and result.get("job"):
        job = result["job"]
        job_id = job.get("job_id")
        print(f"   ✅ Job found: {job_id}")
        print(f"   📝 Prompt: {job.get('prompt', '')[:50]}...")
        return jsonify(result), 200
    else:
        print(f"   💤 No pending jobs")
        return jsonify({
            "success": False,
            "message": "No pending jobs"
        }), 200


@app.route("/worker/pending-jobs", methods=["GET"])
def worker_get_pending_jobs():
    """Get ALL pending jobs for backlog catch-up"""
    try:
        print(f"📥 Worker requesting all pending jobs...")
        
        # Query for all pending jobs from database
        response = supabase.table("jobs").select("*").eq("status", "pending").order("created_at", desc=False).execute()
        
        if response.data:
            jobs = response.data
            print(f"   ✅ Found {len(jobs)} pending job(s)")
            return jsonify({
                "success": True,
                "jobs": jobs,
                "count": len(jobs)
            }), 200
        else:
            print(f"   💤 No pending jobs")
            return jsonify({
                "success": True,
                "jobs": [],
                "count": 0
            }), 200
    except Exception as e:
        print(f"   ❌ Error fetching pending jobs: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "jobs": [],
            "count": 0
        }), 500


@app.route("/worker/job/<job_id>/progress", methods=["POST"])
def worker_update_progress(job_id):
    """Update job progress from worker"""
    data = request.get_json()
    progress = data.get("progress", 0)
    message = data.get("message", "")
    
    print(f"📊 Worker progress update: job_id={job_id}, progress={progress}, message={message}")
    
    # Update job with progress (note: error_message is for errors, not status messages)
    result = update_job_status(
        job_id,
        status="running",
        progress=progress
    )
    
    if result.get("success"):
        return jsonify({"success": True}), 200
    else:
        return jsonify({"success": False, "error": "Failed to update progress"}), 500


@app.route("/worker/job/<job_id>/complete", methods=["POST"])
def worker_complete_job(job_id):
    """Mark job as complete with image/video URL"""
    data = request.get_json()
    image_url = data.get("image_url")
    thumbnail_url = data.get("thumbnail_url")
    video_url = data.get("video_url")  # ✅ FIX: Accept video_url from worker
    
    print(f"🎉 Worker marking job complete: {job_id}")
    print(f"   Image URL: {image_url}")
    print(f"   Thumbnail URL: {thumbnail_url}")
    print(f"   Video URL: {video_url}")  # ✅ FIX: Log video URL
    
    if not image_url:
        return jsonify({"success": False, "error": "image_url required"}), 400
    
    # Update job with results
    result = update_job_result(
        job_id,
        image_url=image_url,
        thumbnail_url=thumbnail_url,
        video_url=video_url  # ✅ FIX: Pass video_url to update function
    )
    
    if result.get("success"):
        return jsonify({"success": True}), 200
    else:
        return jsonify({"success": False, "error": "Failed to complete job"}), 500


@app.route("/worker/job/<job_id>/fail", methods=["POST"])
def worker_fail_job(job_id):
    """Mark job as failed"""
    data = request.get_json()
    error_message = data.get("error", "Unknown error")
    
    success = update_job_status(
        job_id,
        status="failed",
        error_message=error_message
    )
    
    if success:
        return jsonify({"success": True}), 200
    else:
        return jsonify({"success": False, "error": "Failed to mark as failed"}), 500


@app.route("/worker/upload", methods=["POST"])
def worker_upload_image():
    """Upload image to Supabase storage (called by worker)"""
    import base64
    from io import BytesIO
    
    data = request.get_json()
    job_id = data.get("job_id")
    image_data_b64 = data.get("image_data")
    
    if not job_id or not image_data_b64:
        return jsonify({"success": False, "error": "job_id and image_data required"}), 400
    
    try:
        # Decode base64 image
        image_data = base64.b64decode(image_data_b64)
        
        # Create temporary file
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(image_data)
            tmp_path = tmp.name
        
        # Upload to Supabase
        from storage import upload_image
        result = upload_image(job_id, tmp_path)
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        if result["success"]:
            return jsonify({
                "success": True,
                "image_url": result["image_url"],
                "thumbnail_url": result.get("thumbnail_url")
            }), 200
        else:
            return jsonify({"success": False, "error": result.get("error")}), 500
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# MEGA ENDPOINTS DISABLED - No longer using Mega storage


@app.route("/cloudinary/upload-image", methods=["POST"])
def cloudinary_upload_image():
    """
    Upload image to Cloudinary cloud storage and return public URL
    
    Accepts:
    - JSON with base64 encoded image: {"image_data": "base64...", "file_name": "image.png"}
    - Or multipart/form-data with file upload
    
    Returns:
    - {"success": true, "secure_url": "https://res.cloudinary.com/...", "public_url": "..."}
    """
    import base64
    
    print("\n" + "="*60)
    print("☁️ CLOUDINARY UPLOAD REQUEST")
    print("="*60)
    
    try:
        cloudinary_storage = get_cloudinary_manager()
        
        # Check if it's JSON with base64 data or file upload
        if request.is_json:
            data = request.get_json()
            image_data_b64 = data.get("image_data")
            file_name = data.get("file_name", f"image_{int(time.time())}.png")
            metadata = data.get("metadata")  # Get metadata if provided
            
            if not image_data_b64:
                print("❌ No image_data provided in JSON")
                return jsonify({
                    "success": False,
                    "error": "image_data (base64) is required"
                }), 400
            
            print(f"📦 Decoding base64 image data...")
            print(f"📝 File name: {file_name}")
            if metadata:
                print(f"📋 Metadata: {list(metadata.keys())}")
            
            # Decode base64 image
            image_bytes = base64.b64decode(image_data_b64)
            
            # Upload from bytes with metadata
            result = cloudinary_storage.upload_image_from_bytes(image_bytes, file_name, metadata=metadata)
            
        else:
            # Handle file upload
            if 'file' not in request.files:
                print("❌ No file in request")
                return jsonify({
                    "success": False,
                    "error": "No file provided"
                }), 400
            
            file = request.files['file']
            
            if file.filename == '':
                print("❌ Empty filename")
                return jsonify({
                    "success": False,
                    "error": "No file selected"
                }), 400
            
            print(f"📁 Received file: {file.filename}")
            
            # Save to temporary file
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(file.filename).suffix) as tmp:
                file.save(tmp.name)
                tmp_path = tmp.name
            
            # Upload the file
            result = cloudinary_storage.upload_image(tmp_path)
            
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except:
                pass
        
        if result["success"]:
            print(f"✅ Upload successful!")
            print(f"🔗 Secure URL: {result['secure_url']}")
            print("="*60 + "\n")
            return jsonify(result), 200
        else:
            print(f"❌ Upload failed: {result.get('error')}")
            print("="*60 + "\n")
            return jsonify(result), 500
    
    except Exception as e:
        print(f"❌ Exception during upload: {str(e)}")
        import traceback
        traceback.print_exc()
        print("="*60 + "\n")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/mega/proxy", methods=["GET"])
def mega_proxy():
    """
    Proxy endpoint to serve images from Mega.nz
    This allows embedding Mega images in <img> tags
    
    Usage: /mega/proxy?url=https://mega.nz/#!...
    """
    mega_url = request.args.get("url")
    
    if not mega_url:
        return jsonify({
            "success": False,
            "error": "Missing 'url' parameter"
        }), 400
    
    print(f"\n{'='*60}")
    print(f"MEGA PROXY REQUEST")
    print(f"{'='*60}")
    print(f"URL: {mega_url}")
    
    try:
        from mega_storage import download_from_mega_url
        
        # Download the file from Mega
        file_data = download_from_mega_url(mega_url)
        
        if not file_data:
            print(f"Failed to download from Mega")
            print(f"{'='*60}\n")
            return jsonify({
                "success": False,
                "error": "Failed to download from Mega"
            }), 500
        
        print(f"Successfully proxied {len(file_data)} bytes")
        print(f"{'='*60}\n")
        
        # Return the image with appropriate headers
        from flask import Response
        return Response(
            file_data,
            mimetype='image/png',
            headers={
                'Content-Type': 'image/png',
                'Cache-Control': 'public, max-age=31536000',  # Cache for 1 year
                'Access-Control-Allow-Origin': '*'
            }
        )
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        print(f"{'='*60}\n")
        
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "cached_url": cached_url,
        "has_url": cached_url is not None
    }), 200


# Telegram endpoints removed - using direct Monetag postback only


@app.route("/clear-cache", methods=["POST"])
def clear_cache():
    """Clear the cached URL and force fresh fetch on next request"""
    global cached_url, cached_url_timestamp
    cached_url = None
    cached_url_timestamp = None
    print("🗑️ Cache cleared - next request will fetch fresh URL from Discord")
    return jsonify({
        "success": True,
        "message": "Cache cleared - next request will fetch fresh URL"
    }), 200


# ============================================================================
# COIN SYSTEM ENDPOINTS
# ============================================================================

@app.route("/coins/balance", methods=["GET"])
@require_auth
def get_coins_balance():
    """
    Get user's coin balance and statistics
    
    Returns:
        200: Success with balance, lifetime_earned, lifetime_spent, generations_available
        401: Unauthorized
        500: Server error
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["user_id"]
        print(f"💰 Getting coin balance for user: {user_id}")
        
        stats = coins.get_coin_stats(user_id)
        
        if stats is None:
            return jsonify({
                "success": False,
                "error": "Failed to fetch coin balance"
            }), 500
        
        return jsonify({
            "success": True,
            **stats
        }), 200
        
    except Exception as e:
        print(f"❌ Error in /coins/balance: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/coins/history", methods=["GET"])
@require_auth
def get_coins_history():
    """
    Get user's coin transaction history
    
    Query params:
        limit: Number of transactions (default: 100)
        offset: Offset for pagination (default: 0)
    
    Returns:
        200: Success with transactions array
        401: Unauthorized
        500: Server error
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["id"]
        limit = int(request.args.get('limit', 100))
        offset = int(request.args.get('offset', 0))
        
        print(f"📜 Getting transaction history for user: {user_id} (limit: {limit}, offset: {offset})")
        
        transactions = coins.get_transaction_history(user_id, limit=limit, offset=offset)
        
        if transactions is None:
            return jsonify({
                "success": False,
                "error": "Failed to fetch transaction history"
            }), 500
        
        return jsonify({
            "success": True,
            "transactions": transactions,
            "total": len(transactions)
        }), 200
        
    except Exception as e:
        print(f"❌ Error in /coins/history: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============================================================================
# AD SESSION ENDPOINTS (Monetag Verified)
# ============================================================================

@app.route("/ads/start-session", methods=["POST"])
@require_auth
def start_ad_session():
    """
    Start a new ad session - creates tracking record before showing ad
    
    Request body:
        zone_id: Monetag zone ID (required)
        ad_type: Type of ad (default: 'onclick')
    
    Returns:
        200: Success with session_id and monetag_click_id
        401: Unauthorized
        402: Daily limit reached
        500: Server error
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["user_id"]
        data = request.get_json() or {}
        
        zone_id = data.get('zone_id', monetag_api.MONETAG_ZONE_ID)
        ad_type = data.get('ad_type', 'onclick')
        
        print(f"\n{'='*60}")
        print(f"📺 STARTING AD SESSION")
        print(f"{'='*60}")
        print(f"User ID: {user_id}")
        print(f"Zone ID: {zone_id}")
        print(f"Ad Type: {ad_type}")
        
        # Check daily limit
        if coins.check_daily_ad_limit(user_id):
            print(f"⚠️ Daily ad limit reached")
            return jsonify({
                "success": False,
                "error": "daily_limit_reached",
                "message": f"You've reached the daily limit of {coins.MAX_ADS_PER_DAY} ads. Come back tomorrow!"
            }), 402
        
        # Generate unique click ID for Monetag tracking
        monetag_click_id = monetag_api.generate_monetag_click_id(user_id)
        
        # Create ad session in database
        import uuid
        from datetime import datetime
        session_id = str(uuid.uuid4())
        
        session_data = {
            'id': session_id,
            'user_id': user_id,
            'monetag_click_id': monetag_click_id,
            'zone_id': zone_id,
            'ad_type': ad_type,
            'status': 'pending',
            'monetag_verified': False,
            'created_at': datetime.utcnow().isoformat(),
            'ip_address': request.remote_addr,
            'user_agent': request.headers.get('User-Agent')
        }
        
        # Insert into ad_sessions table
        response = supabase.table('ad_sessions').insert(session_data).execute()
        
        if not response.data:
            print(f"❌ Failed to create ad session")
            return jsonify({
                "success": False,
                "error": "Failed to create ad session"
            }), 500
        
        print(f"✅ Ad session created: {session_id}")
        print(f"🆔 Monetag click ID: {monetag_click_id}")
        print(f"{'='*60}\n")
        
        return jsonify({
            "success": True,
            "session_id": session_id,
            "monetag_click_id": monetag_click_id
        }), 200
        
    except Exception as e:
        print(f"❌ Error in /ads/start-session: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/ads/check-session/<session_id>", methods=["GET"])
@require_auth
def check_ad_session(session_id):
    """
    Check if an ad session has been verified by Monetag
    
    Returns:
        200: Status of the session (verified or not)
        404: Session not found
        401: Unauthorized
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        # Fetch session
        response = supabase.table('ad_sessions').select('*').eq('id', session_id).execute()
        
        if not response.data:
            return jsonify({
                "success": False,
                "error": "Session not found"
            }), 404
        
        session = response.data[0]
        
        # Verify user owns this session
        if session['user_id'] != user['user_id']:
            return jsonify({
                "success": False,
                "error": "Unauthorized"
            }), 401
        
        return jsonify({
            "success": True,
            "session_id": session_id,
            "status": session['status'],
            "verified": session.get('monetag_verified', False),
            "created_at": session['created_at']
        }), 200
        
    except Exception as e:
        print(f"❌ Error in /ads/check-session: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/ads/claim-reward", methods=["POST"])
@require_auth
def claim_ad_reward():
    """
    Claim reward for a verified ad session
    Only awards coins if Monetag has verified the ad completion
    
    Request body:
        session_id: Ad session ID (required)
    
    Returns:
        200: Success with coins_earned
        400: Session not verified
        401: Unauthorized
        404: Session not found
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["user_id"]
        data = request.get_json() or {}
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({
                "success": False,
                "error": "session_id is required"
            }), 400
        
        print(f"\n{'='*60}")
        print(f"💰 CLAIMING AD REWARD")
        print(f"{'='*60}")
        print(f"Session ID: {session_id}")
        print(f"User ID: {user_id}")
        
        # Fetch session
        response = supabase.table('ad_sessions').select('*').eq('id', session_id).execute()
        
        if not response.data:
            print(f"❌ Session not found")
            return jsonify({
                "success": False,
                "error": "Session not found"
            }), 404
        
        session = response.data[0]
        
        # Verify user owns this session
        if session['user_id'] != user_id:
            print(f"❌ User doesn't own this session")
            return jsonify({
                "success": False,
                "error": "Unauthorized"
            }), 401
        
        # Check if already claimed
        if session['status'] == 'completed':
            print(f"⚠️ Session already claimed")
            return jsonify({
                "success": False,
                "error": "Reward already claimed for this session"
            }), 400
        
        # CRITICAL: Check if Monetag verified this ad
        if not session.get('monetag_verified', False):
            print(f"⚠️ Session not verified by Monetag")
            return jsonify({
                "success": False,
                "error": "Ad not verified by Monetag. Please watch the complete ad.",
                "verified": False
            }), 400
        
        # All checks passed - award coins!
        print(f"✅ Session verified by Monetag - awarding coins")
        
        # Update session status
        supabase.table('ad_sessions').update({
            'status': 'completed',
            'completed_at': datetime.utcnow().isoformat()
        }).eq('id', session_id).execute()
        
        # Record in ad_completions for audit
        ad_completion_id = coins.record_ad_completion(
            user_id=user_id,
            ad_network_id=session['monetag_click_id'],
            ad_type=session.get('ad_type', 'onclick'),
            coins_awarded=coins.AD_REWARD,
            ip_address=session.get('ip_address'),
            user_agent=session.get('user_agent'),
            metadata={'session_id': session_id, 'monetag_verified': True}
        )
        
        # Award coins
        success = coins.award_coins(
            user_id=user_id,
            coins_amount=coins.AD_REWARD,
            source='ad_watched',
            reference_id=ad_completion_id,
            description=f"Watched Monetag ad (verified)",
            metadata={'session_id': session_id, 'monetag_click_id': session['monetag_click_id']}
        )
        
        if not success:
            print(f"❌ Failed to award coins")
            return jsonify({
                "success": False,
                "error": "Failed to award coins"
            }), 500
        
        # Get updated balance
        stats = coins.get_coin_stats(user_id)
        
        print(f"✅ Reward claimed! User now has {stats['balance']} coins")
        print(f"{'='*60}\n")
        
        return jsonify({
            "success": True,
            "coins_earned": coins.AD_REWARD,
            "total_balance": stats['balance'],
            "generations_available": stats['generations_available']
        }), 200
        
    except Exception as e:
        print(f"❌ Error in /ads/claim-reward: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/ads/verify-and-reward", methods=["POST"])
@require_auth
def verify_and_reward():
    """
    Verify ad completion and reward ONLY when Monetag postback is received
    STRICTLY REQUIRES postback confirmation before awarding coins
    
    MULTI-AD NETWORK FLOW:
    1. Frontend: User clicks "Watch Ad" button
    2. Frontend: Shows Monetag rewarded interstitial video
    3. Monetag: Sends postback to backend when video completes
    4. Backend: Marks session as monetag_verified=true in database
    5. Frontend: Calls this endpoint (/ads/verify-and-reward)
    6. Backend: Awards coins after Monetag postback received ✅
    7. Frontend: Opens HillTop Ads (https://creamymouth.com/d.moFlzBdOGqNHvCZOGsUa/...) in new window
    8. User watches HillTop ad (optional, for additional monetization)
    9. Total coins earned: From Monetag verification
    
    Request body:
        session_id: Ad session ID (required)
    
    Returns:
        200: Success if postback verified and rewarded
        202: Pending - waiting for Monetag postback
        400: Postback not received - cannot award coins
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["user_id"]
        data = request.get_json() or {}
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({
                "success": False,
                "error": "session_id is required"
            }), 400
        
        print(f"\n{'='*60}")
        print(f"🔍 MANUAL AD VERIFICATION (STRICT POSTBACK REQUIRED)")
        print(f"{'='*60}")
        print(f"Session ID: {session_id}")
        
        # Fetch session
        response = supabase.table('ad_sessions').select('*').eq('id', session_id).execute()
        
        if not response.data:
            return jsonify({
                "success": False,
                "error": "Session not found"
            }), 404
        
        session = response.data[0]
        
        # Verify user owns this session
        if session['user_id'] != user_id:
            return jsonify({
                "success": False,
                "error": "Unauthorized"
            }), 401
        
        # Check if already completed
        if session['status'] == 'completed':
            return jsonify({
                "success": False,
                "error": "Reward already claimed"
            }), 400
        
        # STRICT CHECK: Only award if postback has been received and verified
        if session.get('monetag_verified', False):
            print(f"✅ Monetag postback verified - claiming reward")
            return claim_ad_reward_internal(user_id, session_id, session)
        
        # If not verified by postback yet, wait a moment and re-check
        # Postback may take a few seconds to arrive from Monetag servers
        import time
        max_retries = 3
        for attempt in range(max_retries):
            print(f"🔄 Checking for Monetag postback (attempt {attempt + 1}/{max_retries})...")
            time.sleep(2)  # Wait 2 seconds between checks
            
            # Re-fetch session to check if postback arrived
            session_check = supabase.table('ad_sessions').select('*').eq('id', session_id).execute()
            if session_check.data and session_check.data[0].get('monetag_verified', False):
                print(f"✅ Monetag postback received - claiming reward")
                return claim_ad_reward_internal(user_id, session_id, session_check.data[0])
        
        # STRICT REQUIREMENT: Postback must be received to award coins
        print(f"❌ POSTBACK NOT RECEIVED - CANNOT AWARD COINS")
        print(f"⚠️ Waiting for Monetag server postback notification")
        print(f"{'='*60}\n")
        
        return jsonify({
            "success": False,
            "status": "pending",
            "error": "Waiting for Monetag postback verification",
            "message": "Coins will be awarded once Monetag confirms the ad completion on their servers",
            "session_id": session_id
        }), 202
        
    except Exception as e:
        print(f"❌ Error in /ads/verify-and-reward: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/ads/check-postback-status", methods=["POST"])
@require_auth
def check_postback_status():
    """
    Check if Monetag postback has been received for an ad session
    Use this to poll and wait for postback without claiming reward yet
    
    Request body:
        session_id: Ad session ID (required)
    
    Returns:
        200: {postback_received: true} - Ready to claim reward
        202: {postback_received: false, waiting: true} - Still waiting
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["user_id"]
        data = request.get_json() or {}
        session_id = data.get('session_id')
        
        if not session_id:
            return jsonify({
                "success": False,
                "error": "session_id is required"
            }), 400
        
        # Fetch session
        response = supabase.table('ad_sessions').select('*').eq('id', session_id).execute()
        
        if not response.data:
            return jsonify({
                "success": False,
                "error": "Session not found"
            }), 404
        
        session = response.data[0]
        
        # Verify user owns this session
        if session['user_id'] != user_id:
            return jsonify({
                "success": False,
                "error": "Unauthorized"
            }), 401
        
        # Check postback status
        postback_received = session.get('monetag_verified', False)
        
        if postback_received:
            return jsonify({
                "success": True,
                "postback_received": True,
                "message": "Postback received - ready to claim reward",
                "session_id": session_id
            }), 200
        else:
            return jsonify({
                "success": True,
                "postback_received": False,
                "waiting": True,
                "message": "Waiting for Monetag postback",
                "session_id": session_id
            }), 202
            
    except Exception as e:
        print(f"❌ Error in /ads/check-postback-status: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


def claim_ad_reward_internal(user_id, session_id, session):
    """Internal function to claim ad reward after verification"""
    from datetime import datetime
    
    # Update session status
    supabase.table('ad_sessions').update({
        'status': 'completed',
        'completed_at': datetime.utcnow().isoformat()
    }).eq('id', session_id).execute()
    
    # Record in ad_completions
    ad_completion_id = coins.record_ad_completion(
        user_id=user_id,
        ad_network_id=session['monetag_click_id'],
        ad_type=session.get('ad_type', 'onclick'),
        coins_awarded=coins.AD_REWARD,
        ip_address=session.get('ip_address'),
        user_agent=session.get('user_agent'),
        metadata={'session_id': session_id, 'monetag_verified': True}
    )
    
    # Award coins
    success = coins.award_coins(
        user_id=user_id,
        coins_amount=coins.AD_REWARD,
        source='ad_watched',
        reference_id=ad_completion_id,
        description=f"Watched Monetag ad (verified)",
        metadata={'session_id': session_id}
    )
    
    if not success:
        return jsonify({
            "success": False,
            "error": "Failed to award coins"
        }), 500
    
    stats = coins.get_coin_stats(user_id)
    
    return jsonify({
        "success": True,
        "coins_earned": coins.AD_REWARD,
        "total_balance": stats['balance'],
        "generations_available": stats['generations_available']
    }), 200


# ============================================================================
# LEGACY AD REWARD (Deprecated - kept for backward compatibility)
# ============================================================================

@app.route("/ads/reward", methods=["POST"])
@require_auth
def reward_ad_completion():
    """
    DEPRECATED: Legacy endpoint - rewards without Monetag verification
    ⚠️ DO NOT USE FOR MONETAG ADS - Use /ads/start-session + /ads/verify-and-reward flow
    
    This endpoint is maintained for backward compatibility only and does NOT
    require Monetag postback verification. It should only be used for testing
    or non-Monetag ad networks.
    
    For production Monetag ads, ALWAYS use the verified flow:
    1. POST /ads/start-session (get session_id)
    2. User watches ad (SDK verifies completion)
    3. Wait for Monetag postback
    4. POST /ads/verify-and-reward (only awards if postback received)
    
    Request body:
        ad_network_id: Unique ID from ad network (required)
        ad_type: Type of ad (default: 'rewarded')
        duration_seconds: How long user watched
    
    Returns:
        200: Success with coins_earned, total_balance
        400: Bad request (missing ad_network_id)
        401: Unauthorized
        402: Payment Required (duplicate ad or daily limit reached)
        500: Server error
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        user_id = user["user_id"]
        data = request.get_json() or {}
        
        ad_network_id = data.get('ad_network_id')
        if not ad_network_id:
            return jsonify({
                "success": False,
                "error": "ad_network_id is required"
            }), 400
        
        ad_type = data.get('ad_type', 'rewarded')
        duration_seconds = data.get('duration_seconds')
        
        print(f"⚠️ LEGACY /ads/reward called - consider using new Monetag-verified flow")
        print(f"📹 Processing ad reward for user {user_id}, ad {ad_network_id}")
        
        # Fraud check 1: Duplicate ad in last 5 minutes?
        if coins.check_duplicate_ad(user_id, ad_network_id):
            print(f"⚠️ Duplicate ad detected - rejecting reward")
            return jsonify({
                "success": False,
                "error": "duplicate_reward",
                "message": "You already watched this ad recently. Please wait a few minutes."
            }), 402
        
        # Fraud check 2: Daily limit reached?
        if coins.check_daily_ad_limit(user_id):
            print(f"⚠️ Daily ad limit reached - rejecting reward")
            return jsonify({
                "success": False,
                "error": "daily_limit_reached",
                "message": f"You've reached the daily limit of {coins.MAX_ADS_PER_DAY} ads. Come back tomorrow!"
            }), 402
        
        # Get IP and user agent for fraud detection
        ip_address = request.remote_addr
        user_agent = request.headers.get('User-Agent')
        
        # Record ad completion
        ad_completion_id = coins.record_ad_completion(
            user_id=user_id,
            ad_network_id=ad_network_id,
            ad_type=ad_type,
            coins_awarded=coins.AD_REWARD,
            duration_seconds=duration_seconds,
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        if not ad_completion_id:
            return jsonify({
                "success": False,
                "error": "Failed to record ad completion"
            }), 500
        
        # Award coins
        success = coins.award_coins(
            user_id=user_id,
            coins_amount=coins.AD_REWARD,
            source='ad_watched',
            reference_id=ad_completion_id,
            description=f"Watched {ad_type} ad",
            metadata={'ad_network_id': ad_network_id}
        )
        
        if not success:
            return jsonify({
                "success": False,
                "error": "Failed to award coins"
            }), 500
        
        # Get updated balance
        stats = coins.get_coin_stats(user_id)
        
        print(f"✅ Ad reward successful! User now has {stats['balance']} coins")
        
        return jsonify({
            "success": True,
            "coins_earned": coins.AD_REWARD,
            "total_balance": stats['balance'],
            "generations_available": stats['generations_available'],
            "message": f"Ad completed! You earned {coins.AD_REWARD} coins."
        }), 200
        
    except Exception as e:
        print(f"❌ Error in /ads/reward: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============================================================================
# MONETAG API INTEGRATION ENDPOINTS
# ============================================================================

@app.route("/api/monetag/postback", methods=["POST"])
def monetag_postback():
    """
    Direct Monetag postback handler - receives & validates ad completions
    
    Configure this URL in your Monetag dashboard under Settings → Postback URL:
    https://api.rasenai.qzz.io/api/monetag/postback
    
    Postback parameters (sent by Monetag):
        - click_id: Unique click identifier
        - zone_id: Ad zone identifier
        - revenue: Revenue generated from ad
        - status: 'completed' or 'failed'
    
    Returns:
        200: Success - ad recorded
        403: Forbidden - invalid signature
        400: Bad request - missing required fields
    """
    try:
        # Handle both JSON and form-encoded data
        if request.is_json:
            data = request.json or {}
        else:
            data = request.form.to_dict()
        
        signature = request.headers.get('X-Monetag-Signature', '')
        
        print(f"\n{'='*80}")
        print(f"💰 MONETAG POSTBACK RECEIVED - DIRECT VALIDATION")
        print(f"{'='*80}")
        print(f"📨 Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📦 Payload: {data}")
        if signature:
            print(f"🔐 Signature: {signature[:20]}...")
        
        # Extract required fields
        click_id = data.get('click_id')
        zone_id = data.get('zone_id')
        user_id = data.get('user_id')  # Custom parameter if provided
        revenue = float(data.get('revenue', 0))
        status = data.get('status', 'completed')
        
        # Validation: Require click_id
        if not click_id:
            print(f"❌ REJECTED: Missing click_id")
            print(f"{'='*80}\n")
            return jsonify({"error": "Missing click_id"}), 400
        
        print(f"\n📋 Data Extraction:")
        print(f"   ✓ Click ID: {click_id}")
        print(f"   ✓ Zone ID: {zone_id}")
        print(f"   ✓ Revenue: ${revenue}")
        print(f"   ✓ Status: {status}")
        
        # 1. Signature verification (optional - skip in dev)
        if signature:
            is_valid = monetag_api.verify_monetag_signature(data, signature)
            if not is_valid:
                print(f"\n🔐 Signature Validation: FAILED")
                print(f"{'='*80}\n")
                return jsonify({"error": "Invalid signature"}), 403
            else:
                print(f"✅ Signature Validation: PASSED")
        else:
            print(f"⚠️  No signature provided (dev/test mode)")
        
        # 2. Validate zone ID if configured
        if zone_id:
            is_valid_zone = monetag_api.validate_zone_id(zone_id)
            if not is_valid_zone:
                print(f"\n❌ Zone Validation: FAILED - Invalid zone_id: {zone_id}")
                print(f"{'='*80}\n")
                return jsonify({"error": "Invalid zone_id"}), 400
            else:
                print(f"✅ Zone Validation: PASSED")
        
        # 3. Try to find and update session in database
        ad_processed = False
        try:
            session_response = supabase.table('ad_sessions').select('*').eq('monetag_click_id', click_id).execute()
            
            if session_response.data:
                session = session_response.data[0]
                print(f"\n📋 Database Lookup: FOUND")
                print(f"   Session ID: {session['id']}")
                
                # Update session status
                update_data = {
                    'monetag_verified': True,
                    'monetag_revenue': revenue,
                    'postback_timestamp': time.time()
                }
                
                if status != 'completed':
                    update_data['status'] = 'failed'
                
                supabase.table('ad_sessions').update(update_data).eq('monetag_click_id', click_id).execute()
                
                print(f"   ✅ Status Updated: VERIFIED")
                print(f"   💰 Revenue Recorded: ${revenue}")
                ad_processed = True
            else:
                print(f"\n📋 Database Lookup: NOT FOUND")
                print(f"   ⚠️  No session with click_id: {click_id}")
                print(f"   💡 Will still accept postback (click_id may be custom format)")
                
                # Create new ad record if needed
                try:
                    supabase.table('ad_completions').insert({
                        'click_id': click_id,
                        'zone_id': zone_id,
                        'user_id': user_id,
                        'revenue': revenue,
                        'status': status,
                        'received_at': time.time()
                    }).execute()
                    print(f"   ✅ New ad_completions record created")
                    ad_processed = True
                except Exception as insert_err:
                    print(f"   ⚠️  Could not create record: {insert_err}")
                    ad_processed = True  # Still accept the postback
                    
        except Exception as db_error:
            print(f"\n⚠️  Database error: {db_error}")
            print(f"   💡 Will still accept postback (validation passed)")
            ad_processed = True  # Accept regardless
        
        # 4. Success response
        print(f"\n✅ POSTBACK ACCEPTED & PROCESSED")
        print(f"{'='*80}\n")
        
        return jsonify({
            "success": True,
            "message": "Postback received and validated",
            "click_id": click_id,
            "revenue": revenue,
            "processed": ad_processed
        }), 200
        
    except Exception as e:
        print(f"\n❌ ERROR in /api/monetag/postback: {e}")
        import traceback
        traceback.print_exc()
        print(f"{'='*80}\n")
        return jsonify({"error": str(e)}), 500


@app.route("/api/monetag/verify/<click_id>", methods=["GET"])
@require_auth
def monetag_verify_click(click_id):
    """
    Verify ad completion with MoneyTag API
    
    This endpoint queries the MoneyTag API to verify if an ad was completed.
    Used as a double-check in addition to the postback.
    
    Args:
        click_id: The MoneyTag click ID to verify
    
    Returns:
        200: Verification result with completion status
        401: Unauthorized
        500: Server error
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        print(f"\n🔍 Verifying MoneyTag ad completion for click_id: {click_id}")
        
        # Query MoneyTag API
        verification = monetag_api.verify_ad_completion_with_api(click_id)
        
        if verification:
            print(f"✅ MoneyTag verification result: {verification}")
            return jsonify({
                "success": True,
                "verified": verification['completed'],
                "revenue": verification['revenue'],
                "status": verification['status'],
                "timestamp": verification['timestamp']
            }), 200
        else:
            print(f"⚠️ MoneyTag API verification failed or timed out")
            return jsonify({
                "success": False,
                "error": "Failed to verify with MoneyTag API"
            }), 500
            
    except Exception as e:
        print(f"❌ Error in /api/monetag/verify: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/api/monetag/stats", methods=["GET"])
@require_auth
def monetag_get_stats():
    """
    Get MoneyTag statistics for date range
    
    Query params:
        date_from: Start date (YYYY-MM-DD), defaults to today
        date_to: End date (YYYY-MM-DD), defaults to today
    
    Returns:
        200: Statistics from MoneyTag
        401: Unauthorized
        500: Server error
    """
    try:
        user = get_current_user()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        
        date_from = request.args.get('date_from')
        date_to = request.args.get('date_to')
        
        print(f"\n📊 Fetching MoneyTag statistics from {date_from} to {date_to}")
        
        stats = monetag_api.get_monetag_statistics(date_from, date_to)
        
        if stats:
            return jsonify({
                "success": True,
                "stats": stats
            }), 200
        else:
            return jsonify({
                "success": False,
                "error": "Failed to fetch statistics from MoneyTag"
            }), 500
            
    except Exception as e:
        print(f"❌ Error in /api/monetag/stats: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route("/api/monetag/config", methods=["GET"])
def monetag_get_config():
    """
    Get MoneyTag configuration status (public endpoint for frontend)
    
    Returns:
        200: Configuration status
    """
    try:
        config = monetag_api.check_monetag_config()
        
        # Add zone ID for frontend
        config['zone_id'] = monetag_api.MONETAG_ZONE_ID
        
        return jsonify({
            "success": True,
            "config": config
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == "__main__":
    print("\n" + "="*60)
    print("FLASK BACKEND STARTING")
    print("="*60)
    print(f"Discord Channel ID: {CHANNEL_ID}")
    masked_token = f"{BOT_TOKEN[:4]}...{BOT_TOKEN[-4:]}"
    print(f"Bot Token: {masked_token}")
    print(f"CORS enabled for frontend access")
    print(f"Server: http://localhost:5000")
    
    # Initialize and start Telegram polling
    print("\n📱 Initializing Telegram Bot Polling...")
    if test_telegram_api():
        init_telegram_polling(supabase)
        start_telegram_polling(interval=5)  # Poll every 5 seconds
        print("✅ Telegram polling started successfully\n")
    else:
        print("⚠️ Telegram API test failed - polling disabled\n")
    print("\nAvailable Endpoints:")
    print(f"   LEGACY:")
    print(f"   - GET  /get-url         : Fetch latest ngrok URL from Discord")
    print(f"   - POST /generate        : Generate AI content via ComfyUI")
    print(f"   - GET  /list-models     : List available AI models")
    print(f"   - GET  /health          : Check backend status")
    print(f"   - POST /clear-cache     : Clear cached URL")
    print(f"\n   AUTHENTICATION:")
    print(f"   - POST /auth/magic-link : Send magic link to email")
    print(f"   - GET  /auth/verify     : Verify magic link token")
    print(f"   - GET  /auth/me         : Get current user info")
    print(f"   - POST /auth/logout     : Logout current user")
    print(f"\n   JOBS:")
    print(f"   - POST   /jobs          : Create new job")
    print(f"   - GET    /jobs          : Get user's jobs")
    print(f"   - GET    /jobs/<id>     : Get specific job")
    print(f"   - PATCH  /jobs/<id>     : Update job status")
    print(f"   - DELETE /jobs/<id>     : Cancel job")
    print(f"   - GET    /jobs/stats    : Get job statistics")
    print(f"\n   WORKER (Internal):")
    print(f"   - GET  /worker/next-job       : Get next pending job")
    print(f"   - POST /worker/job/<id>/complete : Mark job complete")
    print(f"\n   MEGA STORAGE:")
    print(f"   - POST /mega/upload-image     : Upload image to Mega cloud")
    print(f"\n   TELEGRAM:")
    print(f"   - Polling Telegram for postback messages every 5s")
    print("="*60)
    print("Debug mode enabled - all requests will be logged")
    print("="*60 + "\n")
    
    try:
        # Production: Northflank / Cloud deployment
        # Northflank handles HTTPS automatically - Flask runs on HTTP
        port = int(os.getenv("PORT", "8000"))
        debug = os.getenv("FLASK_ENV") == "development"
        
        print(f"🚀 Starting Flask backend on port {port}")
        print(f"   Debug mode: {debug}")
        print(f"   Northflank HTTPS: automatic")
        
        app.run(
            host="0.0.0.0",
            port=port,
            debug=debug,
            use_reloader=False  # Disable reloader in production
        )
    finally:
        # Cleanup: stop Telegram polling on shutdown
        print("\n🛑 Shutting down Telegram polling...")
        stop_telegram_polling()

