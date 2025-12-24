-- Migration 014: Create modal_deployments table
-- Purpose: Unified table for Modal deployments with paired image/video URLs
-- Date: 2025-11-30
-- Author: Droid

-- Drop existing table if it exists (for clean migration)
DROP TABLE IF EXISTS modal_deployments CASCADE;

-- Create modal_deployments table
-- One row = One Modal deployment with both image and video endpoints
CREATE TABLE modal_deployments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deployment_number INTEGER UNIQUE NOT NULL,
    image_url TEXT NOT NULL,
    video_url TEXT NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used_at TIMESTAMP WITH TIME ZONE,
    notes TEXT,
    metadata JSONB,
    
    -- Constraints
    CONSTRAINT chk_image_url_not_empty CHECK (image_url <> ''),
    CONSTRAINT chk_video_url_not_empty CHECK (video_url <> ''),
    CONSTRAINT chk_deployment_number_positive CHECK (deployment_number > 0)
);

-- Create indexes for faster queries
CREATE INDEX idx_modal_deployments_active ON modal_deployments(is_active, created_at);
CREATE INDEX idx_modal_deployments_number ON modal_deployments(deployment_number);
CREATE INDEX idx_modal_deployments_created ON modal_deployments(created_at DESC);
CREATE INDEX idx_modal_deployments_last_used ON modal_deployments(last_used_at DESC NULLS LAST);

-- Add comments for documentation
COMMENT ON TABLE modal_deployments IS 'Stores Modal deployment information with paired image and video URLs';
COMMENT ON COLUMN modal_deployments.deployment_number IS 'Sequential deployment number for easy reference';
COMMENT ON COLUMN modal_deployments.image_url IS 'Modal URL for image generation endpoint';
COMMENT ON COLUMN modal_deployments.video_url IS 'Modal URL for video generation endpoint';
COMMENT ON COLUMN modal_deployments.is_active IS 'Whether this deployment is active and should be used';
COMMENT ON COLUMN modal_deployments.last_used_at IS 'Last time this deployment was used for a job';
COMMENT ON COLUMN modal_deployments.metadata IS 'Additional metadata (startup time, models, etc.)';

-- Insert example data (REPLACE WITH YOUR ACTUAL URLS)
-- You can add your actual Modal deployment URLs here
INSERT INTO modal_deployments (deployment_number, image_url, video_url, is_active, created_at, notes) VALUES
(1, 
 'https://selomamamaman--comfyui-api-image-serve.modal.run', 
 'https://selomamamaman--comfyui-api-video-serve.modal.run', 
 false,
 '2025-11-29 07:48:11+00',
 'First deployment - currently stopped'),
(2, 
 'https://uxgbuwbxwuxnwixnwknxwxwx--comfyui-api-image-serve.modal.run',
 'https://uxgbuwbxwuxnwixnwknxwxwx--comfyui-api-video-serve.modal.run',
 true,
 '2025-11-29 06:47:38+00',
 'Second deployment - active'),
(3, 
 'https://acatasvchrikgbmgkgkkg--comfyui-api-image-serve.modal.run',
 'https://acatasvchrikgbmgkgkkg--comfyui-api-video-serve.modal.run',
 true,
 '2025-11-25 12:50:56+00',
 'Third deployment - active');

-- Create function to auto-update updated_at timestamp
CREATE OR REPLACE FUNCTION update_modal_deployments_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update updated_at
CREATE TRIGGER trigger_update_modal_deployments_timestamp
    BEFORE UPDATE ON modal_deployments
    FOR EACH ROW
    EXECUTE FUNCTION update_modal_deployments_updated_at();

-- Verification queries
-- Run these after migration to verify:

-- 1. Check all deployments
-- SELECT * FROM modal_deployments ORDER BY deployment_number;

-- 2. Check active deployments only
-- SELECT * FROM modal_deployments WHERE is_active = true ORDER BY created_at ASC;

-- 3. Count by status
-- SELECT is_active, COUNT(*) as count FROM modal_deployments GROUP BY is_active;

-- Migration complete!
