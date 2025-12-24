"""
Telegram Bot Polling Module
Polls Telegram for postback messages from Monetag
Validates, parses, and processes reward messages
"""

import os
import requests
import time
import threading
from datetime import datetime
from collections import deque
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_SECRET = os.getenv("TELEGRAM_SECRET")

# In-memory storage for processed transaction IDs (prevent duplicates)
processed_transactions = set()
max_processed_size = 10000
update_offset = 0  # Telegram offset for avoiding duplicate messages


class TelegramPoller:
    """Handle Telegram bot polling and message processing"""
    
    def __init__(self, supabase_client=None):
        self.supabase = supabase_client
        self.offset = 0
        self.processed_txs = set()
        self.running = False
        self.polling_thread = None
        
    def get_updates(self):
        """
        Fetch updates from Telegram API
        Returns list of update objects
        """
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {
                "offset": self.offset + 1,  # Skip already processed messages
                "timeout": 30,  # Long polling timeout
                "allowed_updates": ["message"]
            }
            
            response = requests.get(url, params=params, timeout=40)
            data = response.json()
            
            # Debug: Show API response - INCLUDE ERROR MESSAGE
            is_ok = data.get("ok", False)
            print(f"   API Response: ok={is_ok}, total_updates={len(data.get('result', []))}")
            
            if is_ok:
                updates = data.get("result", [])
                if updates:
                    print(f"   📨 Updates found: {len(updates)}")
                    for i, u in enumerate(updates):
                        msg = u.get('message', {}).get('text', 'NO TEXT')[:80]
                        print(f"      [{i+1}] Update ID: {u.get('update_id')}, Text: {msg}")
                return updates
            else:
                error_desc = data.get('description', 'Unknown error')
                error_code = data.get('error_code', 'UNKNOWN')
                print(f"   ❌ API ERROR CODE {error_code}: {error_desc}")
                
                # Show the full response for debugging
                print(f"   Full response: {data}")
                
                return []
                
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            print(f"   ❌ Request error: {e}")
            return []
    
    def parse_message(self, text):
        """
        Parse Telegram message with Monetag macros:
        TGID/SOURCE:{telegram_id}|ZONE:{zone_id}|REWARD:{reward_event_type}|
        PRICE:{estimated_price}|YMID:{ymid}|SEC:{secret}
        
        Accepts both old format (TGID) and new format (SOURCE)
        Returns dict with extracted values or None if invalid
        """
        if not text:
            return None
        
        # Quick check - if doesn't look like postback, silently ignore
        if "|" not in text:
            return None
        
        # Check for either TGID (old) or SOURCE (new) format
        if not ("TGID:" in text or "SOURCE:" in text):
            return None
        
        try:
            parts = {}
            for segment in text.split("|"):
                if ":" in segment:
                    key, value = segment.split(":", 1)
                    parts[key.strip()] = value.strip()
            
            # Normalize: accept both TGID and SOURCE
            if "SOURCE" not in parts and "TGID" in parts:
                parts["SOURCE"] = parts["TGID"]
            
            # Validate required fields
            required = ["SOURCE", "ZONE", "REWARD", "PRICE", "YMID", "SEC"]
            missing = [key for key in required if key not in parts]
            
            if missing:
                # Silently skip - probably a user message, not postback
                return None
            
            # Accept all reward types: yes, no, yes_valued, non_valued, impression, click
            # All are valid postback events
            return parts
                
        except Exception as e:
            print(f"❌ Error parsing message: {e}")
            return None
    
    def validate_secret(self, secret):
        """Validate that the secret matches"""
        return secret == TELEGRAM_SECRET
    
    def is_duplicate(self, tx_id):
        """Check if transaction already processed"""
        return tx_id in self.processed_txs
    
    def mark_processed(self, tx_id):
        """Mark transaction as processed"""
        self.processed_txs.add(tx_id)
        
        # Prevent memory leak - keep only last N transactions
        if len(self.processed_txs) > max_processed_size:
            # Keep most recent - this is a simple approach
            oldest = sorted(list(self.processed_txs))[:-max_processed_size]
            for tx in oldest:
                self.processed_txs.discard(tx)
    
    def process_message(self, text, update_id):
        """
        Process a single Telegram message with Monetag postback
        Extract postback data, validate, and update database
        """
        print(f"   🔍 Parsing message for postback data...")
        parsed = self.parse_message(text)
        if not parsed:
            print(f"   ⚠️ Message doesn't contain valid postback format")
            return False
        
        print(f"   ✅ Parsed postback: {list(parsed.keys())}")
        
        # Validate secret
        secret_valid = self.validate_secret(parsed.get("SEC"))
        if not secret_valid:
            print(f"   ❌ Invalid secret - rejecting. Got: {parsed.get('SEC')}, Expected: {TELEGRAM_SECRET}")
            return False
        
        print(f"   ✅ Secret valid")
        
        # Check for duplicates (using YMID as unique transaction ID)
        ymid = parsed.get("YMID")
        if self.is_duplicate(ymid):
            print(f"   ⚠️ Duplicate YMID {ymid} - skipping")
            return False
        
        print(f"   ✅ Not a duplicate")
        # Extract official Monetag macro data
        telegram_id = parsed.get("SOURCE") or parsed.get("TGID")  # Accept both SOURCE and TGID
        zone_id = parsed.get("ZONE")
        reward_event_type = parsed.get("REWARD")  # yes, no, yes_valued, non_valued, etc.
        estimated_price = parsed.get("PRICE")
        
        # Accept all reward types - even non_valued events should be verified
        # This allows demo/test postbacks to be processed
        print(f"\n{'='*60}")
        print(f"💰 TELEGRAM POSTBACK RECEIVED - MONETAG")
        print(f"{'='*60}")
        print(f"Telegram ID: {telegram_id}")
        print(f"Zone ID: {zone_id}")
        print(f"Event ID (YMID): {ymid}")
        print(f"Reward Type: {reward_event_type}")
        print(f"Price: ${estimated_price}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        
        # Update database if supabase is available
        if self.supabase:
            try:
                # Try to find session by YMID (contains the monetag_click_id we passed to SDK)
                # YMID format: mt_1765688858_26191fc2_659aaa08
                print(f"\n🔍 DATABASE UPDATE:")
                print(f"   Looking for session with monetag_click_id: {ymid}")
                
                # First try exact YMID match
                session_response = self.supabase.table('ad_sessions').select('*').eq(
                    'monetag_click_id', ymid
                ).execute()
                
                # If no exact match, try partial match (YMID might be in the click_id)
                if not session_response.data:
                    print(f"   No exact match, trying partial match...")
                    session_response = self.supabase.table('ad_sessions').select('*').ilike(
                        'monetag_click_id', f'%{ymid}%'
                    ).order('created_at', desc=True).limit(1).execute()
                
                found_count = len(session_response.data) if session_response.data else 0
                print(f"   Found {found_count} session(s)")
                
                if session_response.data:
                    session = session_response.data[0]
                    print(f"   ✅ Matching session ID: {session['id']}")
                    
                    # Mark session as verified - ALL postback types are valid
                    print(f"   💾 Setting monetag_verified=true in database...")
                    update_response = self.supabase.table('ad_sessions').update({
                        'monetag_verified': True,
                        'monetag_revenue': float(estimated_price) if estimated_price else 0,
                        'monetag_ymid': ymid,
                        'monetag_zone_id': int(zone_id) if zone_id else None,
                        'monetag_reward_type': reward_event_type,
                        'updated_at': datetime.now().isoformat()
                    }).eq('id', session['id']).execute()
                    
                    print(f"   ✅ Session updated successfully!")
                    print(f"   ✅ monetag_verified=true | revenue=${estimated_price} | reward_type={reward_event_type}")
                    print(f"   ⏰ Frontend can now award coins on next verification check!")
                else:
                    # No existing session - log the postback for tracking
                    print(f"   ⚠️ No session found for YMID: {ymid}")
                    print(f"   📝 This might be an orphaned postback - checking for user...")
                    
                    # Try to find user by telegram_id
                    user_response = self.supabase.table('users').select('*').eq(
                        'telegram_id', telegram_id
                    ).execute()
                    
                    if user_response.data:
                        user = user_response.data[0]
                        user_id = user['user_id']
                        print(f"   ✅ Found user: {user_id}")
                        
                        # Log this postback for manual claim later
                        try:
                            # Try to insert into telegram_postbacks table if it exists
                            self.supabase.table('telegram_postbacks').insert({
                                'user_id': user_id,
                                'telegram_id': telegram_id,
                                'ymid': ymid,
                                'zone_id': int(zone_id) if zone_id else None,
                                'estimated_price': float(estimated_price) if estimated_price else 0,
                                'processed': True
                            }).execute()
                            print(f"   ✅ Logged Monetag postback for user {user_id}")
                        except:
                            # Table might not exist - that's ok
                            print(f"   (telegram_postbacks table may not exist yet)")
                    else:
                        print(f"   ❌ User not found for Telegram ID: {telegram_id}")
                    
            except Exception as db_error:
                print(f"   ❌ Database error: {db_error}")
                import traceback
                traceback.print_exc()
        
        # Mark as processed
        self.mark_processed(ymid)
        print(f"{'='*60}\n")
        
        return True
    
    def poll_once(self):
        """Poll Telegram once for new messages"""
        print(f"\n🔔 Polling Telegram for new messages (offset: {self.offset})...")
        updates = self.get_updates()
        
        if not updates:
            print(f"   ✓ No new messages")
            return 0
        
        print(f"   ✅ Fetched {len(updates)} message(s) from Telegram")
        processed_count = 0
        for update in updates:
            update_id = update.get('update_id', 0)
            self.offset = max(self.offset, update_id)
            print(f"   Processing update {update_id}...")
            
            message = update.get('message')
            if not message:
                print(f"   ⚠️ Update {update_id} has no message")
                continue
            
            text = message.get('text')
            if not text:
                print(f"   ⚠️ Message from {message.get('from', {}).get('id')} has no text")
                continue
            
            # Debug: show all messages
            print(f"   📨 Message (length: {len(text)}): {text[:150]}")
            
            # Process the message
            if self.process_message(text, update.get('update_id')):
                processed_count += 1
                print(f"   ✅ Message successfully processed")
            else:
                print(f"   ⚠️ Message failed to process")
        
        if processed_count > 0:
            print(f"✅ Processed {processed_count}/{len(updates)} Telegram messages\n")
        else:
            print(f"⚠️ Fetched {len(updates)} messages but none were valid postbacks\n")
        
        return processed_count
    
    def get_latest_updates(self):
        """
        Get updates without offset to catch any missed messages
        This is useful for recovering from offset desync
        """
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {
                "offset": 0,  # Get from start to recover any missed messages
                "limit": 100,  # Get up to 100 messages
                "timeout": 5
            }
            
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            if data.get("ok"):
                return data.get("result", [])
            return []
        except:
            return []

    def start_polling(self, interval=5):
        """
        Start polling in background thread
        interval: seconds between polls (default 5)
        
        ⚠️ DISABLED: Telegram only allows ONE polling connection per bot
        If you get 409 errors, it means another instance is running
        """
        print("\n" + "="*60)
        print("⚠️ TELEGRAM POLLING DISABLED")
        print("="*60)
        print("Reason: Telegram only allows ONE bot instance to poll at a time")
        print("\nAlternative: Messages arrive via webhook when postback sent")
        print("To process postbacks manually, call /telegram/process-postback")
        print("="*60 + "\n")
        
        # DO NOT START POLLING - prevents 409 conflicts
        self.running = False
        return
    
    def stop_polling(self):
        """Stop the polling thread"""
        self.running = False
        print("🛑 Stopping Telegram polling")
        if self.polling_thread:
            self.polling_thread.join(timeout=5)


# Global instance
telegram_poller = None


def init_telegram_polling(supabase_client):
    """Initialize Telegram poller with supabase client"""
    global telegram_poller
    telegram_poller = TelegramPoller(supabase_client)
    return telegram_poller


def start_telegram_polling(interval=5):
    """Start polling in background"""
    global telegram_poller
    if telegram_poller:
        telegram_poller.start_polling(interval)
    else:
        print("❌ Telegram poller not initialized")


def stop_telegram_polling():
    """Stop polling"""
    global telegram_poller
    if telegram_poller:
        telegram_poller.stop_polling()


# Test function
def test_telegram_api():
    """Test if Telegram API is working"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        if data.get("ok"):
            bot_info = data.get("result", {})
            print(f"✅ Telegram Bot API Working")
            print(f"   Bot: @{bot_info.get('username')}")
            print(f"   Chat ID: {TELEGRAM_CHAT_ID}")
            return True
        else:
            print(f"❌ Telegram API error: {data.get('description')}")
            return False
            
    except Exception as e:
        print(f"❌ Telegram API test failed: {e}")
        return False


def test_telegram_raw_messages():
    """
    Test - directly fetch raw messages from Telegram API without offset
    This bypasses the poller to see the actual API response
    """
    print("\n" + "="*60)
    print("🔧 DIRECT TELEGRAM API TEST (Raw Message Fetch)")
    print("="*60)
    print(f"Bot Token: {TELEGRAM_BOT_TOKEN[:20]}...")
    print(f"Chat ID: {TELEGRAM_CHAT_ID}")
    
    try:
        # Test 1: Check bot status
        print("\n1️⃣ Checking bot status...")
        me_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe"
        me_response = requests.get(me_url, timeout=5)
        me_data = me_response.json()
        
        if me_data.get("ok"):
            print(f"   ✅ Bot is working: @{me_data['result']['username']}")
        else:
            print(f"   ❌ Bot error: {me_data.get('description')}")
            return
        
        # Test 2: Get ALL messages without offset
        print("\n2️⃣ Fetching ALL messages from Telegram (offset=0)...")
        updates_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {
            "offset": 0,
            "limit": 100,
            "timeout": 5
        }
        
        response = requests.get(updates_url, params=params, timeout=10)
        data = response.json()
        
        print(f"   Response OK: {data.get('ok')}")
        updates = data.get("result", [])
        print(f"   Total messages found: {len(updates)}")
        
        if not updates:
            print(f"\n   ⚠️ NO MESSAGES IN TELEGRAM API")
            print(f"   This means:")
            print(f"   - Monetag postback is NOT reaching Telegram bot, OR")
            print(f"   - Messages were already fetched and deleted, OR")
            print(f"   - Messages are in different chat")
            return
        
        # Print all messages
        print(f"\n   📨 Messages in Telegram:")
        for i, update in enumerate(updates):
            update_id = update.get('update_id')
            message = update.get('message', {})
            from_user = message.get('from', {})
            text = message.get('text', 'NO TEXT')
            chat_id = message.get('chat', {}).get('id')
            
            print(f"\n   Message {i+1}:")
            print(f"      Update ID: {update_id}")
            print(f"      Chat ID: {chat_id}")
            print(f"      From: {from_user.get('id')} ({from_user.get('first_name')})")
            print(f"      Text: {text[:200]}")
            
        print(f"\n" + "="*60)
        print(f"✅ TEST COMPLETE")
        print(f"="*60)
        
    except Exception as e:
        print(f"\n❌ Error during test: {e}")
        import traceback
        traceback.print_exc()


def get_monetag_postback_url():
    """
    Generate the Monetag postback URL to configure in Monetag dashboard
    Uses official Monetag macros
    
    Monetag official macros:
    - {telegram_id}: Telegram user ID
    - {zone_id}: Zone ID  
    - {reward_event_type}: yes or no
    - {estimated_price}: Revenue amount
    - {ymid}: Unique event ID (passed from our app via SDK)
    """
    base_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    params = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": "SOURCE:{telegram_id}|ZONE:{zone_id}|REWARD:{reward_event_type}|PRICE:{estimated_price}|YMID:{ymid}|SEC:" + TELEGRAM_SECRET
    }
    
    # Build URL with parameters
    from urllib.parse import urlencode
    url = base_url + "?" + urlencode(params)
    return url
