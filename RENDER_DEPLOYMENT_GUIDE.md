# 🚀 Render.com Deployment Guide
## Deploy Atool Backend (FREE - No Credit Card Required)

---

## ✅ Why Render?

- **100% FREE** tier (no credit card needed initially)
- Deploy web services + background workers
- Auto-deploy on git push
- Free SSL certificates
- Better than Railway's limited free tier

**⚠️ Free Tier Limitation:**
- Services sleep after 15 minutes of inactivity
- Cold start takes 30-60 seconds when waking up
- Perfect for development/testing!

---

## 📋 What You Need

1. ✅ **GitHub/GitLab account** (your Atool repo)
2. ✅ **Supabase account** with credentials
3. ✅ **Cloudinary account** (optional but recommended)
4. ✅ **Render.com account** (free signup)

---

## 🚀 STEP 1: Create Render Account

### 1.1 Sign Up

1. Go to **[render.com](https://render.com)**
2. Click **"Get Started"** or **"Sign Up"**
3. **Sign up with GitHub** (recommended - easiest deployment)
4. Authorize Render to access your repositories

### 1.2 Complete Profile

- Verify your email
- Complete profile setup
- No credit card required for free tier!

✅ **Checkpoint:** You now have a Render account!

---

## 📦 STEP 2: Deploy Web Service (app.py)

### 2.1 Create New Web Service

1. **In Render Dashboard:**
   - Click **"New +"** button (top right)
   - Select **"Web Service"**

2. **Connect Repository:**
   - If using GitHub: Select **"Atool"** repository
   - If using GitLab: Connect GitLab first, then select repo
   - Click **"Connect"**

### 2.2 Configure Web Service

Fill in the following settings:

**Basic Settings:**
```
Name: atool-backend
Region: Oregon (or closest to you)
Branch: main
Root Directory: backend
```

**Build & Deploy:**
```
Runtime: Python 3
Build Command: pip install -r requirements.txt
Start Command: python app.py
```

**Instance Type:**
```
Plan: Free
```

### 2.3 Add Environment Variables

Click **"Advanced"** → **"Add Environment Variable"**

Add these one by one:

```bash
# Python Version
PYTHON_VERSION=3.11.0

# App Settings
PORT=10000
FLASK_ENV=production
DEBUG=False

# Supabase (Required)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=eyJhbGci...
SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...

# Discord (if you have it)
DISCORD_BOT_TOKEN=your_discord_bot_token
DISCORD_CHANNEL_ID=your_discord_channel_id

# Cloudinary (Optional)
CLOUDINARY_CLOUD_NAME=your-cloud-name
CLOUDINARY_API_KEY=your-api-key
CLOUDINARY_API_SECRET=your-api-secret
```

**Where to get these:**

- **SUPABASE_URL & Keys:**
  1. Go to [supabase.com/dashboard](https://supabase.com/dashboard)
  2. Select your project
  3. Settings → API
  4. Copy Project URL and keys

- **CLOUDINARY credentials:**
  1. Go to [cloudinary.com/console](https://cloudinary.com/console)
  2. Dashboard shows all credentials

### 2.4 Create Web Service

1. Click **"Create Web Service"**
2. Render will start building
3. **First deploy takes 2-5 minutes**

**What happens:**
```
Building...
└─ Installing Python 3.11
└─ Installing dependencies from requirements.txt
└─ Build complete!

Deploying...
└─ Starting: python app.py
└─ Service live at: https://atool-backend.onrender.com
└─ ✅ Deploy successful!
```

### 2.5 Get Your URL

After deployment:
- Your service URL: `https://atool-backend.onrender.com`
- **Copy this URL** - you'll need it for the worker service

### 2.6 Test Your Backend

```bash
# Test health endpoint
curl https://atool-backend.onrender.com/health

# Expected response:
{
  "status": "healthy",
  "cached_url": null,
  "has_url": false
}
```

✅ **Checkpoint:** Web service is deployed and responding!

---

## 👷 STEP 3: Deploy Worker Service (job_worker_realtime.py)

### 3.1 Create Background Worker

1. **In Render Dashboard:**
   - Click **"New +"** button
   - Select **"Background Worker"**

2. **Connect Same Repository:**
   - Select **"Atool"** repository
   - Click **"Connect"**

### 3.2 Configure Worker Service

**Basic Settings:**
```
Name: atool-worker
Region: Oregon (same as web service)
Branch: main
Root Directory: backend
```

**Build & Deploy:**
```
Runtime: Python 3
Build Command: pip install -r requirements.txt
Start Command: python job_worker_realtime.py
```

**Instance Type:**
```
Plan: Free
```

### 3.3 Add Environment Variables

Click **"Advanced"** → **"Add Environment Variable"**

Add these (same as web service + BACKEND_URL):

```bash
# Python Version
PYTHON_VERSION=3.11.0

# App Settings
FLASK_ENV=production

# Backend URL (from web service)
BACKEND_URL=https://atool-backend.onrender.com

# Supabase (Required - same as web service)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=eyJhbGci...
SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...

# Cloudinary (Optional - same as web service)
CLOUDINARY_CLOUD_NAME=your-cloud-name
CLOUDINARY_API_KEY=your-api-key
CLOUDINARY_API_SECRET=your-api-secret
```

**⚠️ Important:** Make sure `BACKEND_URL` points to your web service URL!

### 3.4 Create Worker Service

1. Click **"Create Background Worker"**
2. Render will build and deploy
3. **Takes 2-5 minutes**

### 3.5 Check Worker Logs

After deployment:

1. Click on **"atool-worker"** service
2. Go to **"Logs"** tab
3. You should see:

```
🤖 JOB WORKER STARTING (REALTIME MODE)
📡 Backend URL: https://atool-backend.onrender.com
🔗 Supabase URL: https://xxxxx.supabase.co
✅ Connected to Supabase Realtime
🎧 Listening for new jobs...
```

✅ **Checkpoint:** Both services are running!

---

## 🧪 STEP 4: Verify Deployment

### 4.1 Check Both Services

**In Render Dashboard, you should see:**

1. **atool-backend** (Web Service)
   - Status: 🟢 Live
   - URL: `https://atool-backend.onrender.com`

2. **atool-worker** (Background Worker)  
   - Status: 🟢 Live
   - No public URL (runs in background)

### 4.2 Test Web Service

```bash
# Health check
curl https://atool-backend.onrender.com/health

# Should return:
{
  "status": "healthy",
  "cached_url": null,
  "has_url": false
}
```

### 4.3 Check Worker Logs

1. Go to **atool-worker** service
2. Click **"Logs"** tab
3. Should show connection messages

### 4.4 Test End-to-End (Optional)

If you have your frontend deployed:
1. Update frontend `API_URL` to your Render URL
2. Try creating a job (image generation)
3. Watch worker logs for job processing

✅ **Checkpoint:** Full system is working!

---

## 🔄 STEP 5: Auto-Deploy Setup

### 5.1 Enable Auto-Deploy

Both services are already set to auto-deploy when you push to `main` branch!

**To deploy updates:**

```bash
# Make changes to your code
git add .
git commit -m "Update: your changes"
git push origin main

# Render automatically:
# 1. Detects push
# 2. Builds new version
# 3. Deploys both services
# ✅ Takes 2-3 minutes
```

### 5.2 Watch Deployment

In Render Dashboard:
- Click on service
- Go to **"Events"** tab
- See real-time deployment status

---

## ⚙️ STEP 6: Environment Variable Management

### 6.1 Update Variables

**To change environment variables:**

1. Go to service (web or worker)
2. Click **"Environment"** tab
3. Click **"Edit"** next to variable
4. Update value
5. Click **"Save Changes"**
6. **Service auto-restarts** with new value

### 6.2 Shared Variables

**Tip:** For variables used by both services:
- Update in both places (web + worker)
- Or use Render's "Environment Groups" (Pro feature)

---

## 📊 STEP 7: Monitoring

### 7.1 View Logs

**Real-time logs:**
1. Go to service
2. Click **"Logs"** tab
3. See live output

**Filter logs:**
- Use search box to filter
- Click timestamps to see details

### 7.2 Check Metrics

**On Free tier you get:**
- CPU usage graph
- Memory usage graph
- Request count
- Response times

**Access:**
- Click service → **"Metrics"** tab

### 7.3 Set Up Alerts (Optional - Paid)

**Upgrade to paid plan for:**
- Email alerts
- Slack notifications
- PagerDuty integration

---

## 🐛 STEP 8: Troubleshooting

### Issue 1: Build Fails

**Symptoms:** Build shows errors

**Check:**
1. **Logs** tab for error details
2. **Common issues:**
   - Missing dependencies in `requirements.txt`
   - Wrong Python version
   - Syntax errors

**Fix:**
```bash
# Update requirements.txt locally
pip freeze > requirements.txt

# Push changes
git add requirements.txt
git commit -m "Update dependencies"
git push origin main
```

### Issue 2: Service Crashes on Start

**Symptoms:** Build succeeds, but service crashes

**Check logs for:**
```
❌ ValueError: Supabase credentials not found
❌ Connection refused
❌ ModuleNotFoundError
```

**Fix:**
1. Go to **"Environment"** tab
2. Verify all required variables are set
3. Check for typos in variable names
4. Click **"Manual Deploy"** → **"Clear build cache & deploy"**

### Issue 3: Worker Not Processing Jobs

**Symptoms:** Web service works, jobs stay pending

**Check worker logs:**
1. Go to **atool-worker** service
2. **"Logs"** tab
3. Look for connection errors

**Common fixes:**

**A) BACKEND_URL not set:**
```bash
# Add in worker's Environment:
BACKEND_URL=https://atool-backend.onrender.com
```

**B) Wrong Supabase key:**
```bash
# Worker needs SERVICE_ROLE_KEY (not anon key)
SUPABASE_SERVICE_ROLE_KEY=eyJhbGci...
```

**C) Realtime connection failed:**
- Check Supabase project is accessible
- Verify SERVICE_ROLE_KEY is correct

### Issue 4: Free Tier Sleep

**Symptoms:** Service slow to respond after inactivity

**This is normal on free tier:**
- Services sleep after 15 minutes of no traffic
- First request takes 30-60 seconds to wake up
- Subsequent requests are fast

**Solutions:**
1. **Upgrade to paid plan** ($7/month - no sleep)
2. **Keep-alive pings** (use external service like UptimeRobot)
3. **Accept it** (fine for development)

### Issue 5: Out of Memory

**Symptoms:** Service crashes with memory errors

**Free tier limits:**
- 512MB RAM

**Fix:**
- Upgrade to paid plan (more RAM)
- Optimize your code (reduce memory usage)

---

## 💰 STEP 9: Understand Costs

### Free Tier Limits

**Web Service:**
- 750 hours/month (enough for 1 service 24/7)
- 512MB RAM
- Shared CPU
- Services sleep after 15 min inactivity

**Background Worker:**
- 750 hours/month
- 512MB RAM
- Shared CPU
- No sleep (always running)

### When to Upgrade?

**Consider paid plan ($7/month per service) if you need:**
- ✅ No sleep/downtime
- ✅ More RAM (1GB+)
- ✅ Dedicated CPU
- ✅ Custom domains
- ✅ More bandwidth

**Total for both services:**
- Free tier: $0/month (with sleep)
- Starter plan: $14/month (web + worker, no sleep)

---

## 🌐 STEP 10: Connect Frontend

### 10.1 Get Backend URL

Your backend URL is:
```
https://atool-backend.onrender.com
```

### 10.2 Update Frontend Config

**In your frontend code (Atool web):**

```javascript
// src/config.js
const API_URL = import.meta.env.VITE_API_URL || 
  (window.location.hostname === 'localhost' 
    ? 'http://localhost:5000'
    : 'https://atool-backend.onrender.com');

export default { API_URL };
```

### 10.3 Set Frontend Environment Variable

**If using Cloudflare Pages:**
1. Go to Pages project → Settings
2. Environment variables
3. Add:
   ```
   VITE_API_URL=https://atool-backend.onrender.com
   ```

### 10.4 Update CORS

**In your backend `app.py`:**

Make sure CORS includes your frontend URL:

```python
CORS(app, resources={
    r"/*": {
        "origins": [
            "http://localhost:5173",  # Local dev
            "https://your-frontend.pages.dev",  # Cloudflare Pages
            # Add your actual frontend URL
        ],
        "methods": ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})
```

Push changes to trigger redeploy!

---

## ✅ Deployment Complete Checklist

- [ ] ✅ Render account created
- [ ] ✅ Web service deployed and live
- [ ] ✅ Worker service deployed and running
- [ ] ✅ All environment variables set
- [ ] ✅ Both services showing "Live" status
- [ ] ✅ Health endpoint responding
- [ ] ✅ Worker logs show Supabase connection
- [ ] ✅ Auto-deploy enabled
- [ ] ✅ Frontend connected to backend
- [ ] ✅ Test job processed successfully

---

## 🎉 Success!

Your Atool backend is now deployed on Render!

### What You Have:

✅ **Web Service (app.py):**
- URL: `https://atool-backend.onrender.com`
- Handles API requests
- Manages auth, jobs, coins

✅ **Worker Service (job_worker_realtime.py):**
- Processes AI generation jobs
- Listens via Supabase Realtime
- Updates job status in real-time

### Next Steps:

1. **Test thoroughly** - Create jobs, watch processing
2. **Monitor logs** - Check for any errors
3. **Update frontend** - Point to Render backend
4. **Consider upgrade** - If you need no-sleep service

---

## 📚 Quick Reference

### Service URLs

```
Web Service: https://atool-backend.onrender.com
Dashboard: https://dashboard.render.com
Docs: https://render.com/docs
```

### Important Commands

```bash
# Deploy updates
git push origin main  # Auto-deploys to Render

# Manual redeploy (in Render Dashboard)
# Click service → "Manual Deploy" → "Deploy latest commit"

# Clear build cache
# Click service → "Manual Deploy" → "Clear build cache & deploy"
```

### Support

- Render Docs: https://render.com/docs
- Render Community: https://community.render.com
- Status: https://status.render.com

---

**Last Updated:** December 2024  
**Version:** 1.0  
**Status:** Ready to Deploy! 🚀
