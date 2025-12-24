"""
Authentication Module
Handles magic link authentication, JWT tokens, and user sessions
"""

import os
import uuid
import jwt
import resend
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase_client import supabase

load_dotenv()

# Configuration
JWT_SECRET = os.getenv("JWT_SECRET")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_FROM = os.getenv("EMAIL_FROM", "noreply@yourdomain.com")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

# Initialize Resend
resend.api_key = RESEND_API_KEY


def send_magic_link(email: str) -> dict:
    """
    Generate a magic link token and send it via email
    
    Args:
        email: User's email address
        
    Returns:
        dict with success status and message
    """
    try:
        # Generate unique token
        token = str(uuid.uuid4())
        expires_at = datetime.utcnow() + timedelta(minutes=15)
        
        # Store token in database
        supabase.table("magic_links").insert({
            "token": token,
            "email": email,
            "expires_at": expires_at.isoformat(),
            "used": False
        }).execute()
        
        # Create magic link URL
        magic_link = f"{FRONTEND_URL}/auth/verify?token={token}"
        
        # Send email via Resend with improved deliverability
        email_params = {
            "from": f"AI Image Generator <{EMAIL_FROM}>",
            "to": [email],
            "subject": "Sign in to AI Image Generator",
            "headers": {
                "X-Entity-Ref-ID": token[:8],  # Unique reference for tracking
            },
            "html": f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Sign in to AI Image Generator</title>
            </head>
            <body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #f5f5f5;">
                <table role="presentation" style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td align="center" style="padding: 40px 0;">
                            <table role="presentation" style="width: 600px; max-width: 100%; border-collapse: collapse; background-color: white; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                                <!-- Header -->
                                <tr>
                                    <td style="padding: 40px 30px 30px; text-align: center; border-bottom: 1px solid #eee;">
                                        <h1 style="margin: 0; color: #333; font-size: 24px; font-weight: 600;">🎨 AI Image Generator</h1>
                                    </td>
                                </tr>
                                
                                <!-- Content -->
                                <tr>
                                    <td style="padding: 30px;">
                                        <h2 style="color: #333; margin: 0 0 20px; font-size: 20px; font-weight: 600;">Sign in to your account</h2>
                                        <p style="color: #666; font-size: 16px; line-height: 1.6; margin: 0 0 20px;">
                                            Click the button below to securely sign in to your AI Image Generator account. This link will expire in 15 minutes for your security.
                                        </p>
                                        
                                        <!-- CTA Button -->
                                        <table role="presentation" style="margin: 30px 0;">
                                            <tr>
                                                <td align="center">
                                                    <a href="{magic_link}" 
                                                       style="background-color: #4F46E5; color: white; padding: 14px 28px; text-decoration: none; border-radius: 6px; font-weight: 600; display: inline-block; font-size: 16px;">
                                                        Sign In Now →
                                                    </a>
                                                </td>
                                            </tr>
                                        </table>
                                        
                                        <p style="color: #999; font-size: 14px; line-height: 1.5; margin: 20px 0 0;">
                                            If you didn't request this sign-in link, you can safely ignore this email. No changes will be made to your account.
                                        </p>
                                    </td>
                                </tr>
                                
                                <!-- Alternative Link -->
                                <tr>
                                    <td style="padding: 0 30px 30px; border-top: 1px solid #eee;">
                                        <p style="color: #999; font-size: 12px; line-height: 1.5; margin: 20px 0 0;">
                                            <strong>Having trouble with the button?</strong><br>
                                            Copy and paste this link into your browser:<br>
                                            <a href="{magic_link}" style="color: #4F46E5; word-break: break-all;">{magic_link}</a>
                                        </p>
                                    </td>
                                </tr>
                                
                                <!-- Footer -->
                                <tr>
                                    <td style="padding: 20px 30px; background-color: #f9f9f9; border-top: 1px solid #eee; border-radius: 0 0 10px 10px;">
                                        <p style="color: #999; font-size: 12px; line-height: 1.5; margin: 0; text-align: center;">
                                            This is an automated message from AI Image Generator.<br>
                                            © 2024 AI Image Generator. All rights reserved.
                                        </p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
            </html>
            """,
            "text": f"""
Sign in to AI Image Generator

Click the link below to securely sign in to your account:
{magic_link}

This link will expire in 15 minutes for your security.

If you didn't request this sign-in link, you can safely ignore this email.

---
AI Image Generator
© 2024 All rights reserved.
            """
        }
        
        resend.Emails.send(email_params)
        
        print(f"[OK] Magic link sent to: {email}")
        return {
            "success": True,
            "message": "Magic link sent! Check your email.",
            "email": email
        }
        
    except Exception as e:
        print(f"[ERROR] Error sending magic link: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to send magic link"
        }


def verify_magic_link(token: str) -> dict:
    """
    Verify magic link token and create user session
    
    Args:
        token: Magic link token from URL
        
    Returns:
        dict with user data and JWT token
    """
    try:
        # Get magic link from database
        response = supabase.table("magic_links").select("*").eq("token", token).execute()
        
        if not response.data:
            return {
                "success": False,
                "error": "Invalid or expired magic link"
            }
        
        magic_link = response.data[0]
        
        # Check if already used
        if magic_link["used"]:
            return {
                "success": False,
                "error": "This magic link has already been used"
            }
        
        # Check if expired
        # Handle timestamps with irregular microsecond precision
        expires_str = magic_link["expires_at"].replace('Z', '+00:00')
        # Pad microseconds to 6 digits if needed (e.g., .85283 -> .852830)
        import re
        expires_str = re.sub(r'\.(\d{1,5})(\+|$)', lambda m: f'.{m.group(1).ljust(6, "0")}{m.group(2)}', expires_str)
        expires_at = datetime.fromisoformat(expires_str)
        if datetime.utcnow().replace(tzinfo=expires_at.tzinfo) > expires_at:
            return {
                "success": False,
                "error": "This magic link has expired"
            }
        
        email = magic_link["email"]
        
        # Mark magic link as used
        supabase.table("magic_links").update({
            "used": True,
            "used_at": datetime.utcnow().isoformat()
        }).eq("token", token).execute()
        
        # Get or create user
        user_response = supabase.table("users").select("*").eq("email", email).execute()
        
        if user_response.data:
            user = user_response.data[0]
            # Update last login
            supabase.table("users").update({
                "last_login": datetime.utcnow().isoformat()
            }).eq("id", user["id"]).execute()
        else:
            # Create new user
            new_user = supabase.table("users").insert({
                "email": email,
                "credits": 100,
                "is_active": True
            }).execute()
            user = new_user.data[0]
            
            # Initialize coin wallet for new user
            try:
                import coins
                coins.initialize_user_wallet(user["id"], initial_balance=0)
                print(f"[OK] Coin wallet initialized for new user: {user['id']}")
            except Exception as coin_error:
                print(f"[WARN] Failed to initialize coin wallet: {coin_error}")
                # Don't fail login if coin wallet init fails
        
        # Create JWT token
        jwt_token = create_jwt_token(user["id"], email)
        
        # Create session in database
        session_data = {
            "user_id": user["id"],
            "token": jwt_token,
            "expires_at": (datetime.utcnow() + timedelta(days=7)).isoformat()
        }
        supabase.table("sessions").insert(session_data).execute()
        
        print(f"[OK] User authenticated: {email}")
        
        return {
            "success": True,
            "user": {
                "id": user["id"],
                "email": user["email"],
                "credits": user["credits"],
                "created_at": user["created_at"]
            },
            "token": jwt_token,
            "expires_in": 7 * 24 * 60 * 60  # 7 days in seconds
        }
        
    except Exception as e:
        print(f"❌ Error verifying magic link: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to verify magic link"
        }


def create_jwt_token(user_id: str, email: str) -> str:
    """
    Create JWT token for user session
    
    Args:
        user_id: User's UUID
        email: User's email
        
    Returns:
        JWT token string
    """
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(days=7),
        "iat": datetime.utcnow()
    }
    
    token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    return token


def verify_jwt_token(token: str) -> dict:
    """
    Verify JWT token using STATELESS validation (no database lookup)
    
    This is a high-performance authentication method that validates JWT tokens
    using only cryptographic verification - no Supabase queries required!
    
    Performance: ~50% reduction in Supabase API calls
    
    Args:
        token: JWT token string
        
    Returns:
        dict with user data or error
    """
    try:
        # Decode and verify JWT token cryptographically
        # This checks:
        # 1. Token signature is valid (signed with JWT_SECRET)
        # 2. Token is not expired (exp claim)
        # 3. Token structure is correct
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        
        # Token is valid! Return user data from JWT payload
        return {
            "success": True,
            "user_id": payload["user_id"],
            "email": payload["email"]
        }
        
    except jwt.ExpiredSignatureError:
        return {
            "success": False,
            "error": "Token expired"
        }
    except jwt.InvalidTokenError:
        return {
            "success": False,
            "error": "Invalid token"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def verify_jwt_token_with_session(token: str) -> dict:
    """
    Verify JWT token WITH database session lookup (legacy method)
    
    Use this ONLY for logout functionality where you need to verify
    the session still exists in the database.
    
    For regular authentication, use verify_jwt_token() instead (stateless).
    
    Args:
        token: JWT token string
        
    Returns:
        dict with user data or error
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        
        # Verify session exists in database
        session_response = supabase.table("sessions").select("*").eq("token", token).execute()
        
        if not session_response.data:
            return {
                "success": False,
                "error": "Session not found"
            }
        
        session = session_response.data[0]
        
        # Check if session expired
        expires_at = datetime.fromisoformat(session["expires_at"].replace('Z', '+00:00'))
        if datetime.utcnow().replace(tzinfo=expires_at.tzinfo) > expires_at:
            return {
                "success": False,
                "error": "Session expired"
            }
        
        return {
            "success": True,
            "user_id": payload["user_id"],
            "email": payload["email"]
        }
        
    except jwt.ExpiredSignatureError:
        return {
            "success": False,
            "error": "Token expired"
        }
    except jwt.InvalidTokenError:
        return {
            "success": False,
            "error": "Invalid token"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def logout(token: str) -> dict:
    """
    Logout user by invalidating session
    
    Args:
        token: JWT token string
        
    Returns:
        dict with success status
    """
    try:
        # Delete session from database
        supabase.table("sessions").delete().eq("token", token).execute()
        
        return {
            "success": True,
            "message": "Logged out successfully"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def get_user_from_token(token: str) -> dict:
    """
    Get full user data from JWT token
    
    Args:
        token: JWT token string
        
    Returns:
        dict with user data
    """
    verification = verify_jwt_token(token)
    
    if not verification["success"]:
        return verification
    
    try:
        # Get user from database
        user_response = supabase.table("users").select("*").eq("id", verification["user_id"]).execute()
        
        if not user_response.data:
            return {
                "success": False,
                "error": "User not found"
            }
        
        user = user_response.data[0]
        
        return {
            "success": True,
            "user": {
                "id": user["id"],
                "email": user["email"],
                "credits": user["credits"],
                "created_at": user["created_at"],
                "last_login": user["last_login"]
            }
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
