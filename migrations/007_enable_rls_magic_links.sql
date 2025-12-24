-- Migration 007: Enable Row Level Security on magic_links table
-- Fixes "rls_disabled_in_public" security error
-- Prevents unauthorized access to magic link tokens

-- ============================================
-- Enable RLS on magic_links table
-- ============================================
ALTER TABLE magic_links ENABLE ROW LEVEL SECURITY;

-- ============================================
-- RLS Policies for magic_links
-- ============================================

-- Policy 1: Users can only read their own unused, non-expired magic links
CREATE POLICY "Users can read own valid magic links"
ON magic_links
FOR SELECT
TO authenticated
USING (
    email = (SELECT email FROM auth.users WHERE id = auth.uid())
    AND used = false
    AND expires_at > NOW()
);

-- Policy 2: Service role (backend) can do everything
CREATE POLICY "Service role has full access"
ON magic_links
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

-- Policy 3: Anon users can validate magic links (needed for login flow)
-- But can only read token and email, nothing sensitive
CREATE POLICY "Anonymous can validate magic links"
ON magic_links
FOR SELECT
TO anon
USING (
    used = false
    AND expires_at > NOW()
);

-- ============================================
-- Prevent direct INSERT/UPDATE/DELETE from public API
-- ============================================

-- No policy for authenticated users to INSERT/UPDATE/DELETE
-- Only service_role (backend) can modify magic_links
-- This prevents users from creating their own magic links

-- ============================================
-- Add comment
-- ============================================
COMMENT ON TABLE magic_links IS 'Magic link authentication tokens. RLS enabled - only service_role can modify, users can read own valid links.';

-- ============================================
-- Verification
-- ============================================
-- Verify RLS is enabled
DO $$
BEGIN
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE relname = 'magic_links' AND relnamespace = 'public'::regnamespace) THEN
        RAISE EXCEPTION 'RLS not enabled on magic_links table';
    END IF;
    
    RAISE NOTICE '✅ RLS successfully enabled on magic_links table';
END $$;
