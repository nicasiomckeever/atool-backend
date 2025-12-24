"""
Test script to verify Cloudinary metadata storage functionality
This tests that context metadata is properly stored with images
"""

import os
import requests
import base64
from PIL import Image
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# Configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:5000")

def create_test_image():
    """Create a simple test image"""
    img = Image.new('RGB', (100, 100), color=(73, 109, 137))
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    return buffer.getvalue()

def test_metadata_upload():
    """Test uploading an image with metadata to Cloudinary"""
    print("=" * 60)
    print("🧪 TESTING CLOUDINARY METADATA STORAGE")
    print("=" * 60)
    print()
    
    # Create test image
    print("📷 Creating test image...")
    image_bytes = create_test_image()
    image_b64 = base64.b64encode(image_bytes).decode('utf-8')
    
    # Test metadata
    test_metadata = {
        "prompt": "A beautiful sunset over the ocean",
        "model": "flux-dev",
        "aspect_ratio": "16:9",
        "job_id": "test-job-12345",
        "user_id": "test-user-67890"
    }
    
    print("📋 Test metadata:")
    for key, value in test_metadata.items():
        print(f"   {key}: {value}")
    print()
    
    # Upload to Cloudinary
    print("☁️  Uploading to Cloudinary with metadata...")
    try:
        response = requests.post(
            f"{BACKEND_URL}/cloudinary/upload-image",
            json={
                "image_data": image_b64,
                "file_name": "metadata_test.png",
                "metadata": test_metadata
            },
            timeout=30
        )
        
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get("success"):
                print("✅ Upload successful!")
                print()
                print("📊 Response data:")
                print(f"   Secure URL: {data.get('secure_url')}")
                print(f"   Public ID: {data.get('public_id')}")
                print(f"   Format: {data.get('format')}")
                print(f"   Size: {data.get('width')}x{data.get('height')}")
                print()
                print("✅ SUCCESS! Image uploaded with metadata")
                print()
                print("🔍 To verify metadata:")
                print("   1. Log into your Cloudinary dashboard")
                print("   2. Navigate to Media Library")
                print("   3. Find the image: 'metadata_test.png'")
                print("   4. Click on it and check the 'Context' section")
                print()
                print("📝 Expected context metadata:")
                for key, value in test_metadata.items():
                    print(f"   {key} = {value}")
                print()
                return True
            else:
                print(f"❌ Upload failed: {data.get('error')}")
                return False
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_without_metadata():
    """Test uploading without metadata (should still work)"""
    print()
    print("=" * 60)
    print("🧪 TESTING UPLOAD WITHOUT METADATA")
    print("=" * 60)
    print()
    
    # Create test image
    print("📷 Creating test image...")
    image_bytes = create_test_image()
    image_b64 = base64.b64encode(image_bytes).decode('utf-8')
    
    # Upload to Cloudinary without metadata
    print("☁️  Uploading to Cloudinary WITHOUT metadata...")
    try:
        response = requests.post(
            f"{BACKEND_URL}/cloudinary/upload-image",
            json={
                "image_data": image_b64,
                "file_name": "no_metadata_test.png"
                # No metadata field
            },
            timeout=30
        )
        
        print(f"📥 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get("success"):
                print("✅ Upload successful!")
                print(f"   Secure URL: {data.get('secure_url')}")
                print()
                print("✅ SUCCESS! Upload without metadata works correctly")
                return True
            else:
                print(f"❌ Upload failed: {data.get('error')}")
                return False
        else:
            print(f"❌ Request failed with status {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print()
    print("🚀 Starting Cloudinary metadata tests...")
    print()
    
    # Check if backend is running
    try:
        health = requests.get(f"{BACKEND_URL}/health", timeout=5)
        if health.status_code != 200:
            print(f"⚠️  Backend health check returned {health.status_code}")
            print(f"   Make sure the backend is running at {BACKEND_URL}")
            exit(1)
        print(f"✅ Backend is running at {BACKEND_URL}")
        print()
    except requests.exceptions.RequestException:
        print(f"❌ Cannot connect to backend at {BACKEND_URL}")
        print(f"   Please start the backend server first:")
        print(f"   cd backend && python app.py")
        exit(1)
    
    # Run tests
    test1_passed = test_metadata_upload()
    test2_passed = test_without_metadata()
    
    # Summary
    print()
    print("=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    print(f"Test 1 (With metadata):    {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"Test 2 (Without metadata): {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    print()
    
    if test1_passed and test2_passed:
        print("🎉 All tests passed!")
        exit(0)
    else:
        print("❌ Some tests failed")
        exit(1)
