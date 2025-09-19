# AI Patient Recommender - Docker Deployment

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Google AI API key
- Required JSON credentials file (if using service account)

### 1. Build and Run

```bash
# Clone or navigate to project directory
cd v2

# Build the Docker image
docker build -t ai-patient-recommender .

# Run the container
docker run -d \
  --name ai-recommender \
  -p 8008:8008 \
  -e GOOGLE_API_KEY="your_api_key_here" \
  ai-patient-recommender
```

### 2. Using Docker Compose (Recommended)

```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your API key
nano .env

# Start with docker-compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## 📦 Docker Image Details

### Base Image
- **python:3.11-slim** - Optimized for size and security
- **Size**: ~200MB (vs 1GB+ with full Python image)
- **Security**: Non-root user, minimal system packages

### Optimizations Applied
- ✅ Multi-stage caching for faster builds
- ✅ Minimal system dependencies
- ✅ Non-root user execution
- ✅ Efficient layer caching
- ✅ Health checks included
- ✅ Resource limits configured

## 🔧 Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GOOGLE_API_KEY` | Yes | - | Google AI API key for recommendations |
| `PORT` | No | 8008 | Server port |
| `PRACTICE_CODE` | No | 1012714 | API practice code |
| `WORKERS` | No | 1 | Number of uvicorn workers |

## 📋 API Endpoints

Once running, access:
- **Web Interface**: http://localhost:8008/
- **API Docs**: http://localhost:8008/docs
- **Health Check**: http://localhost:8008/recommendations/test?radius=25

## 🔍 Monitoring and Logs

```bash
# View container logs
docker logs ai-recommender -f

# Check container health
docker inspect ai-recommender | grep Health -A 20

# Container resource usage
docker stats ai-recommender
```

## 📊 Performance

### Resource Requirements
- **RAM**: 512MB minimum, 1GB recommended
- **CPU**: 0.5 cores minimum, 1 core recommended
- **Storage**: ~500MB for image + data

### Expected Performance
- **Cold start**: ~5-10 seconds
- **AI recommendations**: 10-30 seconds (depending on candidate count)
- **Concurrent requests**: 10-20 users with 1GB RAM

## 🛠️ Development

### Local Development
```bash
# Install dependencies locally
pip install -r requirements.txt

# Run development server
python main.py

# Run with auto-reload
uvicorn main:app --reload --host 0.0.0.0 --port 8008
```

### Debugging Container
```bash
# Access container shell
docker exec -it ai-recommender bash

# Check Python environment
docker exec ai-recommender python -c "import pandas; print('✅ Pandas working')"

# Test AI connection
docker exec ai-recommender python -c "
import os
print('API Key present:', bool(os.getenv('GOOGLE_API_KEY')))
"
```

## 🔒 Security Features

- ✅ Non-root user execution
- ✅ Minimal system packages
- ✅ No sensitive data in image
- ✅ Environment-based configuration
- ✅ Health checks for monitoring

## 🚀 Production Deployment

### Docker Hub/Registry
```bash
# Tag for registry
docker tag ai-patient-recommender your-registry/ai-patient-recommender:v1.0

# Push to registry
docker push your-registry/ai-patient-recommender:v1.0
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ai-recommender
spec:
  replicas: 2
  selector:
    matchLabels:
      app: ai-recommender
  template:
    metadata:
      labels:
        app: ai-recommender
    spec:
      containers:
      - name: ai-recommender
        image: your-registry/ai-patient-recommender:v1.0
        ports:
        - containerPort: 8008
        env:
        - name: GOOGLE_API_KEY
          valueFrom:
            secretKeyRef:
              name: ai-secrets
              key: google-api-key
        resources:
          limits:
            memory: 1Gi
            cpu: 1000m
          requests:
            memory: 512Mi
            cpu: 500m
```

## 📝 Troubleshooting

### Common Issues

1. **Container fails to start**
   ```bash
   # Check logs
   docker logs ai-recommender
   
   # Verify environment variables
   docker exec ai-recommender env | grep GOOGLE
   ```

2. **API key not working**
   ```bash
   # Test API key manually
   docker exec ai-recommender python -c "
   import google.generativeai as genai
   import os
   genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))
   print('✅ API key valid')
   "
   ```

3. **High memory usage**
   - Reduce concurrent requests
   - Use smaller candidate limits
   - Monitor with `docker stats`

### Support
- Check container logs first: `docker logs ai-recommender`
- Verify environment variables are set
- Ensure Google AI API key is valid and has sufficient quota