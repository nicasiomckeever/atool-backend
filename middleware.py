"""
Middleware Module
Provides authentication decorators and request utilities
"""

from functools import wraps
from flask import request, jsonify
from auth import verify_jwt_token, get_user_from_token


def require_auth(func):
    """
    Decorator to require authentication for a route
    Supports token from Authorization header OR query parameter (for SSE)
    
    Usage:
        @app.route('/protected')
        @require_auth
        def protected_route():
            user = get_current_user()
            return jsonify(user)
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Allow OPTIONS preflight requests through without requiring JWT
        if request.method == 'OPTIONS':
            return ('', 200)
        # Try to get token from Authorization header first
        auth_header = request.headers.get('Authorization')
        token = None
        
        if auth_header:
            # Extract token (format: "Bearer <token>")
            try:
                token = auth_header.split(' ')[1]
            except IndexError:
                pass
        
        # Fallback: Try to get token from query parameter (for SSE which doesn't support custom headers)
        if not token:
            token = request.args.get('token')
        
        if not token:
            return jsonify({
                "success": False,
                "error": "Missing authentication token"
            }), 401
        
        # Verify token
        verification = verify_jwt_token(token)
        
        if not verification["success"]:
            return jsonify({
                "success": False,
                "error": verification["error"]
            }), 401
        
        # Store user info in request context
        request.user_id = verification["user_id"]
        request.user_email = verification["email"]
        
        return func(*args, **kwargs)
    
    return wrapper


def get_current_user() -> dict:
    """
    Get current authenticated user from request context
    
    Must be used within a route decorated with @require_auth
    
    Returns:
        dict with user_id and email
    """
    if not hasattr(request, 'user_id'):
        return {
            "success": False,
            "error": "No authenticated user in request context"
        }
    
    return {
        "success": True,
        "user_id": request.user_id,
        "email": request.user_email
    }


def extract_token() -> str:
    """
    Extract JWT token from Authorization header
    
    Returns:
        Token string or None
    """
    auth_header = request.headers.get('Authorization')
    
    if not auth_header:
        return None
    
    try:
        token = auth_header.split(' ')[1]
        return token
    except IndexError:
        return None
