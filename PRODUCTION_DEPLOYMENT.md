# Production Deployment Guide

## ✅ Current Status
Your Docker container is working perfectly locally on port 8009:
- ✅ API responds correctly with JSON data  
- ✅ AI reasoning uses Gemini instead of "Rule-based"
- ✅ Distance calculation working properly
- ✅ Provider ID 6053290 returns valid recommendations

## 🚀 Deploy to Production Server

### 1. Deploy the Docker Container to Your Production Server

**Copy your files to the production server:**
```bash
# On your production server (10.20.35.68)
cd /path/to/fox_irs_staging/
```

**Build and run the container:**
```bash
# Build the image
docker build -t ai-patient-recommender .

# Run with proper configuration for your staging environment
docker run -d \
  --name ai-patient-recommender \
  -p 8008:8008 \
  --env-file .env \
  ai-patient-recommender
```

### 2. Configure Reverse Proxy/Web Server

Since your API is at `http://10.20.35.68/fox_irs_staging/`, you need to configure your web server (Apache/Nginx) to proxy requests to the Docker container.

**For Apache (.htaccess or VirtualHost):**
```apache
# Add to your Apache configuration
ProxyPass /fox_irs_staging/ http://localhost:8008/
ProxyPassReverse /fox_irs_staging/ http://localhost:8008/
ProxyPreserveHost On
```

**For Nginx:**
```nginx
location /fox_irs_staging/ {
    proxy_pass http://localhost:8008/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

### 3. Test the Production Deployment

Once deployed, test these endpoints:

**Web Interface:**
```
http://10.20.35.68/fox_irs_staging/
```

**API Test:**
```
http://10.20.35.68/fox_irs_staging/test/6053290?radius=25
```

**Recommendations:**
```
http://10.20.35.68/fox_irs_staging/recommendations/6053290?radius=25
```

### 4. Environment Variables

Make sure your `.env` file on the production server contains:
```env
GOOGLE_API_KEY=your_actual_api_key_here
PORT=8008
PRACTICE_CODE=1012714
```

### 5. Troubleshooting

**If you get 404 errors:**
1. Check that the Docker container is running: `docker ps`
2. Check container logs: `docker logs ai-patient-recommender`
3. Verify the proxy configuration is correct
4. Test direct access to the container: `curl http://localhost:8008/test/6053290?radius=25`

**If you get JSON parsing errors:**
- The fixed JavaScript code now handles non-JSON responses better
- Check the browser console for detailed error messages

## 📝 Current Container Status (Local)

Your container is running locally and ready for deployment:
- **Local URL:** http://localhost:8009
- **Status:** ✅ Working perfectly 
- **AI Integration:** ✅ Using Gemini AI properly
- **Distance Calculation:** ✅ Working with radius filtering
- **JSON Output:** ✅ Proper structured responses

## 🎯 Next Steps

1. **Deploy the container to your production server** (10.20.35.68)
2. **Configure your web server** to proxy `/fox_irs_staging/` to the container
3. **Test the endpoints** to ensure they work from the staging URL
4. **Monitor the logs** to ensure AI scoring is working properly

The application is production-ready and the "404 Not Found" issue should be resolved once the proper proxy configuration is in place!