-- =====================================================
-- Migration 013: Add video_url column to jobs table
-- =====================================================
-- This migration adds support for storing video URLs
-- separately from image URLs for video generation jobs
-- =====================================================

-- Add video_url column to jobs table
ALTER TABLE jobs 
ADD COLUMN IF NOT EXISTS video_url TEXT;

-- Create index for faster video job queries
CREATE INDEX IF NOT EXISTS idx_jobs_video_url ON jobs(video_url) WHERE video_url IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN jobs.video_url IS 'Cloudinary URL for generated video (used for video generation jobs)';

-- =====================================================
-- Completed!
-- =====================================================
-- Run this migration in your Supabase SQL Editor
-- Or execute: psql -U postgres -d your_database -f 013_add_video_url_column.sql
