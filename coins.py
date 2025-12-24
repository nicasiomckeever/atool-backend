"""
Coin System Module - Monetization Backend
Purpose: Handle coin balance, transactions, and ad rewards
Date: 2025-12-07
"""

import os
import logging
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from supabase_client import supabase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Constants
# ============================================================================

GENERATION_COST = 5          # Coins per generation
AD_REWARD = 5                # Coins per ad watched
MAX_ADS_PER_DAY = 50         # Daily earning limit (250 coins max)
DUPLICATE_CHECK_WINDOW = 5   # Minutes (prevent duplicate rewards)

# ============================================================================
# Core Coin Functions
# ============================================================================

def get_coin_balance(user_id: str) -> Optional[int]:
    """
    Get current coin balance for a user
    
    Args:
        user_id: UUID of the user
        
    Returns:
        Current coin balance (int) or None if user not found
    """
    try:
        logger.info(f"💰 Fetching coin balance for user: {user_id}")
        
        # Query user_coins table
        response = supabase.table('user_coins').select('balance').eq('user_id', user_id).execute()
        
        if not response.data or len(response.data) == 0:
            logger.warning(f"⚠️ No coin wallet found for user {user_id}, initializing...")
            # Initialize wallet if not exists
            initialize_user_wallet(user_id)
            return 0
        
        balance = response.data[0]['balance']
        logger.info(f"✅ User {user_id} has {balance} coins")
        return balance
        
    except Exception as e:
        logger.error(f"❌ Error fetching coin balance: {e}")
        return None


def get_coin_stats(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get comprehensive coin statistics for a user
    
    Args:
        user_id: UUID of the user
        
    Returns:
        Dict with balance, lifetime_earned, lifetime_spent, generations_available
    """
    try:
        logger.info(f"📊 Fetching coin stats for user: {user_id}")
        
        response = supabase.table('user_coins').select('*').eq('user_id', user_id).execute()
        
        if not response.data or len(response.data) == 0:
            logger.warning(f"⚠️ No coin wallet found for user {user_id}")
            initialize_user_wallet(user_id)
            return {
                'balance': 0,
                'lifetime_earned': 0,
                'lifetime_spent': 0,
                'generations_available': 0
            }
        
        data = response.data[0]
        balance = data['balance']
        
        stats = {
            'balance': balance,
            'lifetime_earned': data['lifetime_earned'],
            'lifetime_spent': data['lifetime_spent'],
            'generations_available': balance // GENERATION_COST  # How many generations they can afford
        }
        
        logger.info(f"✅ Coin stats for {user_id}: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"❌ Error fetching coin stats: {e}")
        return None


def deduct_coins(user_id: str, coins_amount: int, reference_id: Optional[str] = None, description: Optional[str] = None) -> bool:
    """
    Deduct coins from user's balance
    
    Args:
        user_id: UUID of the user
        coins_amount: Number of coins to deduct (positive number)
        reference_id: UUID of related job/transaction
        description: Optional description for transaction log
        
    Returns:
        True if successful, False if insufficient balance or error
    """
    try:
        logger.info(f"💸 Deducting {coins_amount} coins from user {user_id}")
        
        # Get current balance
        current_balance = get_coin_balance(user_id)
        if current_balance is None:
            logger.error(f"❌ Could not fetch balance for user {user_id}")
            return False
        
        # Check sufficient balance
        if current_balance < coins_amount:
            logger.warning(f"⚠️ Insufficient balance: has {current_balance}, needs {coins_amount}")
            return False
        
        # Calculate new balance
        new_balance = current_balance - coins_amount
        
        # Update user_coins table
        supabase.table('user_coins').update({
            'balance': new_balance,
            'lifetime_spent': supabase.table('user_coins').select('lifetime_spent').eq('user_id', user_id).execute().data[0]['lifetime_spent'] + coins_amount,
            'last_updated': datetime.utcnow().isoformat()
        }).eq('user_id', user_id).execute()
        
        # Log transaction
        log_transaction(
            user_id=user_id,
            transaction_type='generation_used',
            coins_delta=-coins_amount,
            balance_after=new_balance,
            reference_id=reference_id,
            description=description or f"Spent {coins_amount} coins on generation"
        )
        
        logger.info(f"✅ Successfully deducted {coins_amount} coins. New balance: {new_balance}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error deducting coins: {e}")
        return False


def award_coins(user_id: str, coins_amount: int, source: str = 'ad_watched', reference_id: Optional[str] = None, description: Optional[str] = None, metadata: Optional[Dict] = None) -> bool:
    """
    Award coins to user's balance
    
    IMPORTANT: For 'ad_watched' source, this function should ONLY be called
    after Monetag postback has been verified (monetag_verified = True in ad_sessions).
    Do not award coins based on SDK completion alone.
    
    Args:
        user_id: UUID of the user
        coins_amount: Number of coins to award (positive number)
        source: Transaction type ('ad_watched', 'admin_bonus', 'refund', 'initial_bonus')
        reference_id: UUID of related ad_completion or other reference
        description: Optional description for transaction log
        metadata: Optional additional data to store
        
    Returns:
        True if successful, False on error
    """
    try:
        logger.info(f"🎁 Awarding {coins_amount} coins to user {user_id} (source: {source})")
        
        # Get current balance
        current_balance = get_coin_balance(user_id)
        if current_balance is None:
            logger.error(f"❌ Could not fetch balance for user {user_id}")
            return False
        
        # Calculate new balance
        new_balance = current_balance + coins_amount
        
        # Get current lifetime_earned
        current_stats = supabase.table('user_coins').select('lifetime_earned').eq('user_id', user_id).execute()
        current_earned = current_stats.data[0]['lifetime_earned'] if current_stats.data else 0
        
        # Update user_coins table
        supabase.table('user_coins').update({
            'balance': new_balance,
            'lifetime_earned': current_earned + coins_amount,
            'last_updated': datetime.utcnow().isoformat()
        }).eq('user_id', user_id).execute()
        
        # Log transaction
        log_transaction(
            user_id=user_id,
            transaction_type=source,
            coins_delta=coins_amount,
            balance_after=new_balance,
            reference_id=reference_id,
            description=description or f"Earned {coins_amount} coins from {source}",
            metadata=metadata
        )
        
        logger.info(f"✅ Successfully awarded {coins_amount} coins. New balance: {new_balance}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error awarding coins: {e}")
        return False


def log_transaction(user_id: str, transaction_type: str, coins_delta: int, balance_after: int, reference_id: Optional[str] = None, description: Optional[str] = None, metadata: Optional[Dict] = None) -> bool:
    """
    Log a coin transaction to the audit log
    
    Args:
        user_id: UUID of the user
        transaction_type: Type of transaction (ad_watched, generation_used, etc.)
        coins_delta: Change in coins (positive or negative)
        balance_after: Balance after this transaction
        reference_id: Optional reference to job/ad
        description: Optional description
        metadata: Optional additional data (JSONB)
        
    Returns:
        True if logged successfully
    """
    try:
        supabase.table('coin_transactions').insert({
            'user_id': user_id,
            'transaction_type': transaction_type,
            'coins_delta': coins_delta,
            'balance_after': balance_after,
            'reference_id': reference_id,
            'description': description,
            'metadata': metadata,
            'created_at': datetime.utcnow().isoformat()
        }).execute()
        
        logger.debug(f"📝 Logged transaction: {transaction_type} ({coins_delta:+d} coins)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error logging transaction: {e}")
        return False


def get_transaction_history(user_id: str, limit: int = 100, offset: int = 0) -> Optional[List[Dict]]:
    """
    Get paginated transaction history for a user
    
    Args:
        user_id: UUID of the user
        limit: Number of transactions to return (default: 100)
        offset: Offset for pagination (default: 0)
        
    Returns:
        List of transaction dicts or None on error
    """
    try:
        logger.info(f"📜 Fetching transaction history for user {user_id} (limit: {limit}, offset: {offset})")
        
        response = supabase.table('coin_transactions') \
            .select('*') \
            .eq('user_id', user_id) \
            .order('created_at', desc=True) \
            .range(offset, offset + limit - 1) \
            .execute()
        
        transactions = response.data if response.data else []
        logger.info(f"✅ Found {len(transactions)} transactions")
        return transactions
        
    except Exception as e:
        logger.error(f"❌ Error fetching transaction history: {e}")
        return None


def initialize_user_wallet(user_id: str, initial_balance: int = 0) -> bool:
    """
    Initialize coin wallet for a new user
    
    Args:
        user_id: UUID of the user
        initial_balance: Starting balance (default: 0)
        
    Returns:
        True if successful
    """
    try:
        logger.info(f"🆕 Initializing coin wallet for user {user_id} with {initial_balance} coins")
        
        supabase.table('user_coins').insert({
            'user_id': user_id,
            'balance': initial_balance,
            'lifetime_earned': initial_balance if initial_balance > 0 else 0,
            'lifetime_spent': 0,
            'created_at': datetime.utcnow().isoformat(),
            'last_updated': datetime.utcnow().isoformat()
        }).execute()
        
        # Log initial bonus if applicable
        if initial_balance > 0:
            log_transaction(
                user_id=user_id,
                transaction_type='initial_bonus',
                coins_delta=initial_balance,
                balance_after=initial_balance,
                description=f"Welcome bonus: {initial_balance} coins"
            )
        
        logger.info(f"✅ Wallet initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error initializing wallet: {e}")
        return False


# ============================================================================
# Ad Reward Functions
# ============================================================================

def check_duplicate_ad(user_id: str, ad_network_id: str, window_minutes: int = DUPLICATE_CHECK_WINDOW) -> bool:
    """
    Check if user watched this ad recently (fraud prevention)
    
    Args:
        user_id: UUID of the user
        ad_network_id: Ad network's unique ID for this ad
        window_minutes: Time window to check (default: 5 minutes)
        
    Returns:
        True if duplicate found (should reject), False if OK to reward
    """
    try:
        cutoff_time = datetime.utcnow() - timedelta(minutes=window_minutes)
        
        response = supabase.table('ad_completions') \
            .select('id') \
            .eq('user_id', user_id) \
            .eq('ad_network_id', ad_network_id) \
            .gte('watched_at', cutoff_time.isoformat()) \
            .execute()
        
        is_duplicate = len(response.data) > 0 if response.data else False
        
        if is_duplicate:
            logger.warning(f"⚠️ Duplicate ad detected: user {user_id}, ad {ad_network_id}")
        
        return is_duplicate
        
    except Exception as e:
        logger.error(f"❌ Error checking duplicate ad: {e}")
        return False  # Allow if check fails (fail open)


def check_daily_ad_limit(user_id: str, max_ads: int = MAX_ADS_PER_DAY) -> bool:
    """
    Check if user has reached daily ad watching limit
    
    Args:
        user_id: UUID of the user
        max_ads: Maximum ads per day (default: 50)
        
    Returns:
        True if limit reached (should reject), False if OK
    """
    try:
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        response = supabase.table('ad_completions') \
            .select('id', count='exact') \
            .eq('user_id', user_id) \
            .gte('watched_at', today_start.isoformat()) \
            .execute()
        
        ad_count = len(response.data) if response.data else 0
        limit_reached = ad_count >= max_ads
        
        if limit_reached:
            logger.warning(f"⚠️ Daily ad limit reached: user {user_id} watched {ad_count}/{max_ads} ads today")
        
        return limit_reached
        
    except Exception as e:
        logger.error(f"❌ Error checking daily ad limit: {e}")
        return False  # Allow if check fails


def record_ad_completion(
    user_id: str,
    ad_network_id: str,
    ad_type: str = 'rewarded',
    coins_awarded: int = AD_REWARD,
    duration_seconds: Optional[int] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    device_fingerprint: Optional[str] = None,
    metadata: Optional[Dict] = None
) -> Optional[str]:
    """
    Record ad completion in database
    
    Args:
        user_id: UUID of the user
        ad_network_id: Ad network's unique ID
        ad_type: Type of ad (default: 'rewarded')
        coins_awarded: Coins given (default: 5)
        duration_seconds: How long user watched
        ip_address: User's IP (for fraud detection)
        user_agent: Browser info
        device_fingerprint: Device ID
        metadata: Additional ad network data
        
    Returns:
        UUID of ad_completion record, or None on error
    """
    try:
        logger.info(f"📹 Recording ad completion: user {user_id}, ad {ad_network_id}")
        
        response = supabase.table('ad_completions').insert({
            'user_id': user_id,
            'ad_network_id': ad_network_id,
            'ad_type': ad_type,
            'coins_awarded': coins_awarded,
            'watched_at': datetime.utcnow().isoformat(),
            'duration_seconds': duration_seconds,
            'ip_address': ip_address,
            'user_agent': user_agent,
            'device_fingerprint': device_fingerprint,
            'verified': False,  # Will be set to True if ad network confirms
            'metadata': metadata,
            'created_at': datetime.utcnow().isoformat()
        }).execute()
        
        if response.data and len(response.data) > 0:
            ad_completion_id = response.data[0]['id']
            logger.info(f"✅ Ad completion recorded: {ad_completion_id}")
            return ad_completion_id
        
        return None
        
    except Exception as e:
        logger.error(f"❌ Error recording ad completion: {e}")
        return None


# ============================================================================
# Utility Functions
# ============================================================================

def has_sufficient_coins(user_id: str, required_coins: int = GENERATION_COST) -> bool:
    """
    Check if user has enough coins for a generation
    
    Args:
        user_id: UUID of the user
        required_coins: Coins needed (default: 5)
        
    Returns:
        True if user has enough coins
    """
    balance = get_coin_balance(user_id)
    return balance is not None and balance >= required_coins


def get_coins_needed(user_id: str, required_coins: int = GENERATION_COST) -> int:
    """
    Calculate how many more coins user needs
    
    Args:
        user_id: UUID of the user
        required_coins: Coins needed (default: 5)
        
    Returns:
        Number of coins needed (0 if they have enough)
    """
    balance = get_coin_balance(user_id)
    if balance is None:
        return required_coins
    
    shortage = required_coins - balance
    return max(0, shortage)  # Return 0 if they have enough


# ============================================================================
# Admin Functions (for future use)
# ============================================================================

def admin_adjust_balance(user_id: str, coins_delta: int, reason: str) -> bool:
    """
    Admin function to manually adjust user's coin balance
    
    Args:
        user_id: UUID of the user
        coins_delta: Coins to add (positive) or remove (negative)
        reason: Description of why adjustment was made
        
    Returns:
        True if successful
    """
    try:
        logger.info(f"👑 Admin adjusting balance for user {user_id}: {coins_delta:+d} coins")
        
        if coins_delta > 0:
            return award_coins(user_id, coins_delta, source='admin_bonus', description=reason)
        else:
            return deduct_coins(user_id, abs(coins_delta), description=reason)
        
    except Exception as e:
        logger.error(f"❌ Error in admin balance adjustment: {e}")
        return False
