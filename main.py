import json
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from recommender import recommend_patients

app = FastAPI(title="AI Patient Recommendation API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", response_class=HTMLResponse)
def home():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>AI Patient Recommendation</title>
        <style>
            body { 
                font-family: Arial, sans-serif; 
                background: linear-gradient(135deg, #f0f8ff, #f9f9f9); 
                display: flex; 
                justify-content: center; 
                align-items: center; 
                height: 100vh; 
                margin: 0; 
            }
            .container {
                background: white; 
                padding: 30px; 
                border-radius: 16px; 
                box-shadow: 0 6px 25px rgba(0,0,0,0.1); 
                width: 100%; 
                max-width: 650px;
            }
            h1 { 
                text-align: center; 
                color: #222; 
                margin-bottom: 25px; 
                font-size: 26px;
            }
            label { 
                font-weight: bold; 
                display: block; 
                margin-bottom: 6px; 
            }
            input { 
                padding: 12px; 
                margin-bottom: 15px; 
                width: 100%; 
                border-radius: 8px; 
                border: 1px solid #bbb; 
                font-size: 16px; 
            }
            button { 
                padding: 12px; 
                width: 100%; 
                background: #009688; 
                color: white; 
                border: none; 
                border-radius: 8px; 
                font-size: 16px; 
                cursor: pointer; 
                transition: 0.3s;
            }
            button:hover { background: #00796b; }
            .result { 
                margin-top: 20px; 
                padding: 15px; 
                border-radius: 8px; 
                background: #e6fffa; 
                border: 1px solid #a7d8d8; 
                font-family: monospace; 
                max-height: 300px; 
                overflow-y: auto;
                display: none;
            }
            .result.active {
                display: block;
            }
            .error { color: #d32f2f; font-weight: bold; }
            .loading { text-align: center; }
            .spinner {
                border: 4px solid #f3f3f3;
                border-top: 4px solid #009688;
                border-radius: 50%;
                width: 30px;
                height: 30px;
                animation: spin 1s linear infinite;
                margin: 10px auto;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>AI Patient Recommendation</h1>
            <label for="provider_id">Provider ID (FOX_PROVIDER_ID)</label>
            <input type="text" id="provider_id" placeholder="e.g. 6053290">
            
            <label for="radius">Maximum Distance (miles)</label>
            <input type="number" id="radius" placeholder="e.g. 25" value="25" min="1" max="200" step="0.1">
            
            <label for="limit">Number of Recommendations</label>
            <input type="number" id="limit" placeholder="e.g. 10" value="10" min="1" max="100">
            
            <button onclick="getRecommendations()">Get Recommendations</button>

            <div id="result" class="result"></div>
        </div>

        <script>
            async function getRecommendations() {
                const providerId = document.getElementById("provider_id").value.trim();
                const radius = document.getElementById("radius").value.trim() || 25;
                const limit = document.getElementById("limit").value.trim() || 10;
                const resultDiv = document.getElementById("result");

                if (!providerId) {
                    resultDiv.style.display = "block";
                    resultDiv.innerHTML = "<p class='error'>⚠️ Please enter a Provider ID.</p>";
                    return;
                }

                if (!radius || radius <= 0) {
                    resultDiv.style.display = "block";
                    resultDiv.innerHTML = "<p class='error'>⚠️ Please enter a valid radius.</p>";
                    return;
                }

                resultDiv.style.display = "block";
                resultDiv.innerHTML = "<div class='loading'><div class='spinner'></div><p>Fetching recommendations within radius...</p></div>";

                try {
                    const response = await fetch(`/recommend/${providerId}?radius=${radius}&limit=${limit}`);
                    let data;
                    const contentType = response.headers.get("content-type");
                    if (contentType && contentType.includes("application/json")) {
                        data = await response.json();
                    } else {
                        const text = await response.text();
                        data = { detail: `Server returned non-JSON response: ${text.substring(0, 100)}...` };
                    }
                    
                    if (!response.ok) {
                        resultDiv.innerHTML = `<p class='error'>❌ Error: ${data.detail || "Unknown error"}</p>`;
                        return;
                    }
                    resultDiv.innerHTML = "<h3>✅ Recommendations:</h3><pre>" + JSON.stringify(data, null, 2) + "</pre>";
                } catch (error) {
                    resultDiv.innerHTML = `<p class='error'>❌ Error fetching data: ${error}</p>`;
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/test/{provider_id}")
async def test_recommendations(provider_id: str, radius: float = 50.0, limit: int = 5):
    """
    Test endpoint to validate priority rules implementation with radius filtering
    """
    try:
        print(f"Test endpoint called with provider_id: {provider_id}, radius: {radius}, limit: {limit}")
        result = recommend_patients(provider_id, radius=radius, top_k=limit)
        print(f"Recommend_patients returned: {type(result)}")
        
        if isinstance(result, dict) and "error" in result:
            print(f"Error in result: {result['error']}")
            return JSONResponse(
                status_code=400, 
                content={"detail": result["error"]}
            )
        
        # Extract key information for testing
        test_info = {
            "clinician": result.get("clinician", {}),
            "priority_rules": result.get("priority_rules", {}),
            "ai_status": result.get("ai", {}),
            "summary": {
                "total_candidates": result.get("total_candidates"),
                "recommendations_count": result.get("top_k"),
                "previous_provider_matches": result.get("priority_rules", {}).get("previous_provider_matches", 0)
            },
            "top_3_recommendations": []
        }
        
        # Show top 3 with key priority fields
        for rec in result.get("recommendations", [])[:3]:
            test_info["top_3_recommendations"].append({
                "patient_name": rec.get("NAME"),
                "patient_id": rec.get("ID"),
                "match_score": rec.get("Match_Score"),
                "is_previous_provider_match": rec.get("Is_Previous_Provider_Match"),
                "distance_to_clinician": rec.get("Distance_to_Clinician"),
                "primary_distance": rec.get("Primary_Distance"),
                "distance_type_used": rec.get("Distance_Type_Used"),
                "reason": rec.get("Reason")
            })
        
        print(f"Test info prepared successfully")
        return test_info
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Exception in test endpoint: {str(e)}")
        print(f"Full traceback: {error_details}")
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}", "traceback": error_details}
        )


@app.get("/recommend/{provider_id}")
@app.get("/recommendations/{provider_id}")
async def get_recommendations(provider_id: str, radius: float = 50.0, limit: int = 50):
    """
    Get patient recommendations for a clinician within a specified radius
    
    Args:
        provider_id: The FOX_PROVIDER_ID of the clinician to recommend patients for
        radius: Maximum distance in miles to include patients (default: 50.0)
        limit: The maximum number of patients to recommend (default: 50)
    """
    try:
        print(f"Recommendation endpoint called with provider_id: {provider_id}, radius: {radius}, limit: {limit}")
        result = recommend_patients(provider_id, radius=radius, top_k=limit)
        print(f"Recommend_patients completed successfully")
        
        if isinstance(result, dict) and "error" in result:
            print(f"Error in result: {result['error']}")
            return JSONResponse(
                status_code=400, 
                content={"detail": result["error"]}
            )
        return result
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Exception in recommendation endpoint: {str(e)}")
        print(f"Full traceback: {error_details}")
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}", "traceback": error_details}
        )
