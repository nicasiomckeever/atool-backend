"""
MoneyTag API Integration Module
Purpose: Server-side verification of ad completions using MoneyTag API
Date: 2025-12-12
"""

import os
import hmac
import hashlib
import logging
import requests
from typing import Optional, Dict, Any
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# MoneyTag Configuration
# ============================================================================

MONETAG_API_TOKEN = os.getenv("MONETAG_API_TOKEN", "7af2c5a17ff1215654af16dfd2af96960603f33d15992192")
MONETAG_ZONE_ID = os.getenv("MONETAG_ZONE_ID", "10305830")
MONETAG_PUBLISHER_ID = os.getenv("MONETAG_PUBLISHER_ID", "")

# MoneyTag API Base URLs
MONETAG_API_BASE = "https://publishers.monetag.com/api"

# ============================================================================
# Signature Verification Functions
# ============================================================================

def verify_monetag_signature(data: Dict[str, Any], signature: str) -> bool:
    """
    Verify that a postback request actually came from MoneyTag
    
    Args:
        data: The JSON payload from MoneyTag
        signature: The signature header from the request
        
    Returns:
        True if signature is valid, False otherwise
    """
    try:
        # Create expected signature using API token as secret
        expected_signature = hmac.new(
            MONETAG_API_TOKEN.encode(),
            str(data).encode(),
            hashlib.sha256
        ).hexdigest()
        
        # Compare signatures using timing-attack-safe comparison
        is_valid = hmac.compare_digest(expected_signature, signature)
        
        if is_valid:
            logger.info("✅ MoneyTag signature verified successfully")
        else:
            logger.warning("⚠️ MoneyTag signature verification failed")
            
        return is_valid
        
    except Exception as e:
        logger.error(f"❌ Error verifying MoneyTag signature: {e}")
        return False


# ============================================================================
# MoneyTag API Query Functions
# ============================================================================

def verify_ad_completion_with_api(click_id: str) -> Optional[Dict[str, Any]]:
    """
    Query MoneyTag API to verify if an ad was actually completed
    
    Args:
        click_id: The unique click/session ID to verify
        
    Returns:
        Dict with completion status and revenue, or None if request failed
        {
            'completed': bool,
            'revenue': float,
            'timestamp': str,
            'status': str
        }
    """
    try:
        logger.info(f"🔍 Verifying ad completion with MoneyTag API for click_id: {click_id}")
        
        # Prepare authorization header
        headers = {
            'Authorization': f'Bearer {MONETAG_API_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        # Query MoneyTag API for click details
        # Note: Adjust endpoint based on actual MoneyTag API documentation
        url = f"{MONETAG_API_BASE}/clicks/{click_id}"
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            result = {
                'completed': data.get('status') == 'completed',
                'revenue': float(data.get('revenue', 0)),
                'timestamp': data.get('completed_at'),
                'status': data.get('status', 'unknown')
            }
            
            logger.info(f"✅ MoneyTag API verification result: {result}")
            return result
            
        elif response.status_code == 404:
            logger.warning(f"⚠️ Click ID not found in MoneyTag: {click_id}")
            return {
                'completed': False,
                'revenue': 0,
                'timestamp': None,
                'status': 'not_found'
            }
        else:
            logger.error(f"❌ MoneyTag API error: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.Timeout:
        logger.error("❌ MoneyTag API request timed out")
        return None
    except Exception as e:
        logger.error(f"❌ Error querying MoneyTag API: {e}")
        return None


def get_monetag_statistics(date_from: str = None, date_to: str = None) -> Optional[Dict[str, Any]]:
    """
    Get statistics from MoneyTag for a date range
    
    Args:
        date_from: Start date (YYYY-MM-DD format), defaults to today
        date_to: End date (YYYY-MM-DD format), defaults to today
        
    Returns:
        Dict with statistics (impressions, clicks, revenue, etc.)
    """
    try:
        logger.info(f"📊 Fetching MoneyTag statistics from {date_from} to {date_to}")
        
        headers = {
            'Authorization': f'Bearer {MONETAG_API_TOKEN}',
            'Content-Type': 'application/json'
        }
        
        # Default to today if dates not provided
        if not date_from:
            date_from = datetime.now().strftime('%Y-%m-%d')
        if not date_to:
            date_to = datetime.now().strftime('%Y-%m-%d')
        
        params = {
            'date_from': date_from,
            'date_to': date_to,
            'zone_id': MONETAG_ZONE_ID
        }
        
        url = f"{MONETAG_API_BASE}/stats"
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ MoneyTag statistics retrieved successfully")
            return data
        else:
            logger.error(f"❌ MoneyTag API error: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching MoneyTag statistics: {e}")
        return None


# ============================================================================
# Helper Functions
# ============================================================================

def generate_monetag_click_id(user_id: str) -> str:
    """
    Generate a unique click ID for tracking with MoneyTag
    
    Args:
        user_id: User ID to associate with the click
        
    Returns:
        Unique click ID string
    """
    import uuid
    timestamp = int(datetime.now().timestamp())
    random_part = str(uuid.uuid4())[:8]
    
    # Format: mt_<timestamp>_<random>_<user_hash>
    user_hash = hashlib.md5(user_id.encode()).hexdigest()[:8]
    click_id = f"mt_{timestamp}_{random_part}_{user_hash}"
    
    logger.info(f"🆔 Generated MoneyTag click ID: {click_id} for user: {user_id}")
    return click_id


def validate_zone_id(zone_id: str) -> bool:
    """
    Validate that the zone ID matches our configured zone
    
    Args:
        zone_id: Zone ID from postback
        
    Returns:
        True if valid, False otherwise
    """
    is_valid = str(zone_id) == str(MONETAG_ZONE_ID)
    
    if not is_valid:
        logger.warning(f"⚠️ Invalid zone ID: {zone_id} (expected: {MONETAG_ZONE_ID})")
        
    return is_valid


# ============================================================================
# Configuration Check
# ============================================================================

def check_monetag_config() -> Dict[str, Any]:
    """
    Check if MoneyTag is properly configured
    
    Returns:
        Dict with configuration status
    """
    config_status = {
        'api_token_set': bool(MONETAG_API_TOKEN and MONETAG_API_TOKEN != 'your_monetag_api_token_here'),
        'zone_id_set': bool(MONETAG_ZONE_ID),
        'publisher_id_set': bool(MONETAG_PUBLISHER_ID and MONETAG_PUBLISHER_ID != 'your_publisher_id_here'),
        'api_base_url': MONETAG_API_BASE
    }
    
    if all([config_status['api_token_set'], config_status['zone_id_set']]):
        config_status['status'] = 'ready'
        logger.info("✅ MoneyTag API is properly configured")
    else:
        config_status['status'] = 'incomplete'
        logger.warning("⚠️ MoneyTag API configuration incomplete")
    
    return config_status


# Log configuration on module load
if __name__ != "__main__":
    config = check_monetag_config()
    logger.info(f"MoneyTag Configuration: {config}")
