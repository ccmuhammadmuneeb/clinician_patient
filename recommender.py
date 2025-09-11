"""
recommender.py

- Exposes recommend_patients(clinician_id: str, top_k: int = 50) -> Dict[str, Any]
- Reads clinicians from CSV and patients from JSON (sample_patient_cases.json)
- Patient discipline derived from CASE_NO prefix (PT/OT/ST) for strict matching
- Computes distance to clinician and to nearest active case (if clinician has any)
- Matches clinician sub-specialty with patient**3. DISTANCE CALCULATION - 20%**
Distance calculation rules:
- If clinician HAS active cases AND patient is within 5 miles of any active case: Use distance between patient and nearest active case
- Otherwise: Use direct distance between clinician and patient

Scoring (based on distance ranges):
- 0-5 miles: 20 points (excellent)
- 5-10 miles: 15 points (very good)  
- 10-15 miles: 12 points (good)
- 15-20 miles: 10 points (acceptable)
- 20-25 miles: 8 points (fair)
- 25-30 miles: 5 points (poor)
- 30+ miles: 2 points (very poor) patient profile
- 100% Gemini-scored ranking (no hardcoded weights); distance-only fallback if AI fails
- Returns JSON-serializable dicts; never raises HTTPException here (API layer can decide)
"""

import os
import json
import time
import re
import math
import requests
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from datetime import datetime
# import google.auth - moved to function level to avoid module-level hanging

# ===== Gemini import, lazy init guard =====
try:
    import google.generativeai as genai  # type: ignore
    GEMINI_IMPORT_OK = True
except Exception:
    genai = None  # type: ignore
    GEMINI_IMPORT_OK = False

# ====================== CONFIG ======================
# Service account file for Google API authentication
SERVICE_ACCOUNT_FILE = os.path.abspath(r"C:\Users\MUHAMMADMUNEEB3\v2\aiml-365220-99e7125f22cc.json")
# Gemini model configuration
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
AI_MAX_OUTPUT_TOKENS = 1600
AI_TEMPERATURE = 0.1

# API Configuration
CLINICIAN_API_BASE = "https://uat-webservices.mtbc.com/Fox/api/RegionalDirector/GetClinicianDetails_V1"
API_HEADERS = {
    "Accept": "application/json",
    "PracticeCode": "1012714"  # Add PracticeCode header as required
}
API_TIMEOUT = 30

# Data file paths (fallback)
CLINICIAN_CSV = os.path.abspath(r"C:\Users\MUHAMMADMUNEEB3\v2\data\clinician_list.csv")
PATIENT_JSON = os.path.abspath(r"C:\Users\MUHAMMADMUNEEB3\v2\data\sample_patient_cases.json")

# Lazy init flag
_gemini_initialized = False

# ====================== UTILS ======================
def clean_nan_values(obj):
    """
    Recursively clean NaN values from dictionaries and lists for JSON serialization
    """
    if isinstance(obj, dict):
        return {k: clean_nan_values(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan_values(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif pd.isna(obj):  # Handle pandas NaN/NA values
        return None
    else:
        return obj

# ====================== API FUNCTIONS ======================
def fetch_clinician_from_api(provider_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch clinician data from the API using Provider ID
    
    Args:
        provider_id: The FOX_PROVIDER_ID to fetch data for
        
    Returns:
        Dictionary containing clinician data or None if failed
    """
    try:
        print(f"Fetching clinician data for Provider ID: {provider_id}")
        
        # Make API request with proper headers
        url = f"{CLINICIAN_API_BASE}?ProviderId={provider_id}"
        print(f"API URL: {url}")
        print(f"Headers: {API_HEADERS}")
        
        response = requests.get(url, headers=API_HEADERS, timeout=API_TIMEOUT)
        print(f"API Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"API request failed with status {response.status_code}: {response.text}")
            return None
            
        # Parse JSON response
        api_data = response.json()
        print(f"API Response structure: {list(api_data.keys()) if isinstance(api_data, dict) else type(api_data)}")
        print(f"API Response content (first 200 chars): {str(api_data)[:200]}...")
        
        # Handle both array and object responses
        if isinstance(api_data, list):
            print(f"API returned array with {len(api_data)} items")
            # If it's an array, take the first item
            if not api_data:
                print("API returned empty array - falling back to mock data for testing")
                # Return mock data structure for testing when API returns empty
                return create_mock_clinician_data(provider_id)
            api_data = api_data[0] if api_data else {}
            print(f"Using first item from array: {list(api_data.keys()) if isinstance(api_data, dict) else type(api_data)}")
        
        # Check if response is successful (only if it's a dict with success field)
        if isinstance(api_data, dict) and "success" in api_data and not api_data.get("success", False):
            print(f"API returned unsuccessful response: {api_data.get('message', 'Unknown error')}")
            return None
            
        # Extract clinician data
        clinician_raw = api_data.get("data", {})
        if not clinician_raw:
            print("No data found in API response")
            return None
            
        # Transform API data to our format
        specialties = []
        professional_info = clinician_raw.get("providersProfessionalInfoList", [])
        for info in professional_info:
            if info.get("ClinicianSpecialitiesDescription"):
                specialties.append(info["ClinicianSpecialitiesDescription"])
        
        # Create standardized clinician record
        clinician_data = {
            "FOX_PROVIDER_ID": clinician_raw.get("FOX_PROVIDER_ID"),
            "Name": f"{clinician_raw.get('FirstName', '')} {clinician_raw.get('LastName', '')}".strip(),
            "FirstName": clinician_raw.get("FirstName"),
            "LastName": clinician_raw.get("LastName"),
            "Discipline": clinician_raw.get("DiciplineCode"),  # Note: API has typo "DiciplineCode"
            "DisciplineName": clinician_raw.get("DiciplineName"),
            "Latitude": float(clinician_raw.get("Latitude", 0)) if clinician_raw.get("Latitude") else None,
            "Longitude": float(clinician_raw.get("Longitude", 0)) if clinician_raw.get("Longitude") else None,
            "Facility_Lat": float(clinician_raw.get("Facility_Lat", 0)) if clinician_raw.get("Facility_Lat") else None,
            "Facility_Long": float(clinician_raw.get("Facility_Long", 0)) if clinician_raw.get("Facility_Long") else None,
            "IS_Facility": clinician_raw.get("IS_FACILITY", False),
            "LocalAddress": clinician_raw.get("LocalAddress"),
            "CITY": clinician_raw.get("CITY"),
            "STATE": clinician_raw.get("STATE"),
            "ACTIVE_CASES": int(clinician_raw.get("ACTIVE_CASES", 0)) if clinician_raw.get("ACTIVE_CASES") else 0,
            "Subspecialty": ", ".join(specialties) if specialties else "",
            "specialties": specialties,  # Keep original list for AI processing
            "professional_info": professional_info,  # Keep full professional info
            "Profile_Picture": clinician_raw.get("Profile_Picture"),
            "UserID": clinician_raw.get("UserID"),
            "INDIVIDUAL_NPI": clinician_raw.get("INDIVIDUAL_NPI"),
            "VISIT_QOUTA_WEEK": clinician_raw.get("VISIT_QOUTA_WEEK")
        }
        
        print(f"Successfully parsed clinician: {clinician_data.get('Name')} ({clinician_data.get('Discipline')})")
        print(f"Specialties: {specialties}")
        print(f"Active Cases: {clinician_data.get('ACTIVE_CASES')}")
        print(f"Location: {clinician_data.get('Latitude')}, {clinician_data.get('Longitude')}")
        
        return clinician_data
        
    except requests.RequestException as e:
        print(f"Network error fetching clinician data: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching clinician data: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def create_mock_clinician_data(provider_id: str) -> Dict[str, Any]:
    """
    Create mock clinician data when API returns empty response
    This uses the structure you provided as example
    """
    print(f"Creating mock data for Provider ID: {provider_id}")
    
    # Mock data based on your provided structure
    mock_data = {
        "FOX_PROVIDER_ID": provider_id,
        "Name": "Carey Sambogna",
        "FirstName": "Carey",
        "LastName": "Sambogna", 
        "Discipline": "PT",
        "DisciplineName": "Physical Therapy",
        "Latitude": 29.947983299999997,
        "Longitude": -85.4179766,
        "Facility_Lat": 40.889624,
        "Facility_Long": -74.032214,
        "IS_Facility": True,
        "LocalAddress": "655 Pomander Walk",
        "CITY": "Voorhees",
        "STATE": "NJ", 
        "ACTIVE_CASES": 16,
        "Subspecialty": "Certified Clinical Instructor, Geriatric Clinical Specialist, Cardiovascular and Pulmonary Clinical Specialist",
        "specialties": [
            "Certified Clinical Instructor",
            "Certified Strength and Conditioning Specialist", 
            "Geriatric Clinical Specialist",
            "Cardiovascular and Pulmonary Clinical Specialist",
            "Oncologic Clinical Specialist"
        ],
        "professional_info": [
            {
                "ClinicianSpecialitiesDescription": "Certified Clinical Instructor",
                "ActiveCertification": "False",
                "ExperiencedTreatingProvider": "True"
            },
            {
                "ClinicianSpecialitiesDescription": "Geriatric Clinical Specialist",
                "ActiveCertification": "False", 
                "ExperiencedTreatingProvider": "True"
            }
        ],
        "UserID": "99910678",
        "INDIVIDUAL_NPI": "",
        "VISIT_QOUTA_WEEK": "40"
    }
    
    print(f"Mock clinician created: {mock_data.get('Name')} ({mock_data.get('Discipline')})")
    return mock_data

# ====================== UTILS ======================
def init_gemini() -> bool:
    """Initialize Gemini SDK only when needed."""
    global _gemini_initialized, genai, GEMINI_IMPORT_OK
    
    print("init_gemini called...")
    
    if _gemini_initialized:
        print("Gemini already initialized, returning True")
        return True
        
    if not GEMINI_IMPORT_OK:
        print("Gemini import failed - the google.generativeai package may not be installed.")
        return False
        
    try:
        print(f"Initializing Gemini with service account: {SERVICE_ACCOUNT_FILE}")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
        
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"WARNING: Service account file not found at {SERVICE_ACCOUNT_FILE}")
            return False
            
        print("Loading credentials...")
        import google.auth
        creds, _ = google.auth.load_credentials_from_file(SERVICE_ACCOUNT_FILE)
        print("Configuring genai...")
        genai.configure(credentials=creds)
        
        # Test API connection
        try:
            print(f"Testing Gemini API connection with model {GEMINI_MODEL}...")
            model = genai.GenerativeModel(GEMINI_MODEL)
            test_resp = model.generate_content("Respond with only the word 'OK' to verify connection.")
            test_text = _extract_text_from_gemini_response(test_resp)
            print(f"Gemini API test response: {test_text[:50]}...")
        except Exception as e:
            print(f"Gemini API test failed: {str(e)}")
            return False
            
        _gemini_initialized = True
        print("Gemini initialization completed successfully!")
        return True
        
    except Exception as e:
        print(f"Gemini initialization failed: {str(e)}")
        import traceback
        traceback.print_exc()
        _gemini_initialized = False
        return False

def _extract_text_from_gemini_response(resp: Any) -> str:
    if resp is None:
        return ""
    if hasattr(resp, "text") and isinstance(resp.text, str) and resp.text.strip():
        return resp.text
    try:
        candidates = getattr(resp, "candidates", None) or []
        if candidates:
            content = candidates[0].content
            parts = getattr(content, "parts", None) or []
            if parts and hasattr(parts[0], "text"):
                return parts[0].text
    except Exception:
        pass
    try:
        return str(resp)
    except Exception:
        return ""

def _clean_ai_json(text: str) -> str:
    """Strip code fences/control chars and extract outermost array/object JSON.
    Enhanced to handle various edge cases in Gemini responses."""
    if not text:
        return ""
    
    # Remove markdown code blocks
    t = re.sub(r"```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    t = re.sub(r"```", "", t)
    
    # Remove control characters
    t = re.sub(r"[\x00-\x1F\x7F]", "", t)
    
    # Remove any explanatory text before or after the JSON
    arr = re.search(r"\[[\s\S]*?\]", t)
    if arr:
        json_text = arr.group(0).strip()
        # Try to validate it's proper JSON
        try:
            json.loads(json_text)
            return json_text
        except json.JSONDecodeError:
            # If parsing fails, try to fix common issues
            # Remove trailing commas in arrays and objects
            fixed = re.sub(r",\s*([}\]])", r"\1", json_text)
            return fixed
            
    obj = re.search(r"\{[\s\S]*?\}", t)
    if obj:
        json_text = obj.group(0).strip()
        try:
            json.loads(json_text)
            return json_text
        except json.JSONDecodeError:
            # Try to fix common issues
            fixed = re.sub(r",\s*([}\]])", r"\1", json_text)
            return fixed
            
    # If we get here, we couldn't find valid JSON patterns
    return ""

def _parse_coords(val: Any) -> Optional[Tuple[float, float]]:
    try:
        if val is None or pd.isna(val):
            return None
        if isinstance(val, (list, tuple)) and len(val) == 2:
            lat, lon = float(val[0]), float(val[1])
            if math.isnan(lat) or math.isnan(lon) or math.isinf(lat) or math.isinf(lon):
                return None
            return lat, lon
        if isinstance(val, dict) and {"lat","lon"} <= set(val):
            lat, lon = float(val["lat"]), float(val["lon"])
            if math.isnan(lat) or math.isnan(lon) or math.isinf(lat) or math.isinf(lon):
                return None
            return lat, lon
        if isinstance(val, str):
            s = val.strip().strip("()[]")
            parts = [p.strip() for p in s.split(",")]
            if len(parts) >= 2:
                lat, lon = float(parts[0]), float(parts[1])
                if math.isnan(lat) or math.isnan(lon) or math.isinf(lat) or math.isinf(lon):
                    return None
                return lat, lon
    except Exception:
        return None
    return None

def _coords_from_cols(lat: Any, lon: Any) -> Optional[Tuple[float, float]]:
    try:
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            return None
        if str(lat).strip() == "" or str(lon).strip() == "":
            return None
        lat_f, lon_f = float(lat), float(lon)
        if math.isnan(lat_f) or math.isnan(lon_f) or math.isinf(lat_f) or math.isinf(lon_f):
            return None
        return lat_f, lon_f
    except Exception:
        return None

def calculate_distance(c1: Optional[Tuple[float, float]],
                       c2: Optional[Tuple[float, float]]) -> Optional[float]:
    """Haversine distance in miles; None if coords missing/invalid."""
    if not c1 or not c2:
        return None
    try:
        from math import radians, sin, cos, sqrt, atan2
        R = 3958.8
        (lat1, lon1), (lat2, lon2) = c1, c2
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return round(R * c, 2)
    except Exception:
        return None

def extract_discipline_from_case_no(case_no: Any) -> str:
    if not case_no:
        return "Other"
    s = str(case_no).strip().upper()
    m = re.match(r"^(OT|PT|ST)[\-\_\s]?\d+", s)
    if m:
        return m.group(1)
    if s.startswith("OT"): return "OT"
    if s.startswith("PT"): return "PT"
    if s.startswith("ST"): return "ST"
    return "Other"

def normalize_clinician_discipline(discipline_str: Any) -> Optional[str]:
    if not discipline_str: return None
    d = str(discipline_str).strip().lower()
    if "physical" in d or re.search(r"\bpt\b", d): return "PT"
    if "occupational" in d or re.search(r"\bot\b", d): return "OT"
    if "speech" in d or re.search(r"\bst\b", d): return "ST"
    # If already PT/OT/ST uppercase:
    if d in {"pt","ot","st"}:
        return d.upper()
    try:
        return str(discipline_str).strip().upper()
    except Exception:
        return None

def _find_active_case_column(series: pd.Series) -> Optional[str]:
    """Detects a column name that likely contains active case IDs."""
    for col in series.index:
        name = str(col).lower().replace("_", " ").strip()
        if "active" in name and "case" in name:
            return col
    return None

def _parse_active_case_ids(value: Any) -> List[str]:
    """Split delimited active case IDs; accepts comma/semicolon/pipe/space."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        s = str(value).strip()
        if not s or s.lower() in ("nan", "none"):
            return []
        raw = re.split(r"[,\;\|\s]+", s)
    return [str(x).strip() for x in raw if str(x).strip() and str(x).strip().lower() != "nan"]

def _bool_from_date_field(v: Any) -> bool:
    """In your data rule: if the date exists (non-null/non-empty) => True."""
    if v is None: return False
    s = str(v).strip()
    if s == "" or s.lower() in {"nan","none","null"}:
        return False
    return True

# ====================== DATA LOADERS ======================
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load clinicians (CSV) and patients (JSON) with robust normalization."""
    # Clinicians
    clinicians = pd.read_csv(CLINICIAN_CSV, encoding="cp1252")
    clinicians.columns = clinicians.columns.str.strip()
    if "ID" not in clinicians.columns:
        # Try alternate common ID column names; final fallback to index as string
        if "Clinician_ID" in clinicians.columns:
            clinicians["ID"] = clinicians["Clinician_ID"]
        else:
            clinicians["ID"] = clinicians.index.astype(str)
    clinicians["ID"] = clinicians["ID"].astype(str)
    clinicians.set_index("ID", inplace=True)

    # Patients
    with open(PATIENT_JSON, "r", encoding="utf-8") as f:
        raw = json.load(f)
    patients = pd.DataFrame(raw)

    # Ensure ID/Name
    patients["ID"] = patients["CASE_ID"].astype(str) if "CASE_ID" in patients.columns else patients.index.astype(str)
    patients["NAME"] = (
        patients.get("First_Name", pd.Series([""]*len(patients))).fillna("") + " " +
        patients.get("Last_Name", pd.Series([""]*len(patients))).fillna("")
    ).str.strip()

    # Patient coordinates
    def _pc(r):
        lat = r.get("CASE_LAT")
        lon = r.get("CASE_LON")
        c = _coords_from_cols(lat, lon)
        return f"{c[0]},{c[1]}" if c else None
    patients["Patient_Coordinates"] = patients.apply(_pc, axis=1)

    # Discipline from CASE_NO
    patients["DISCIPLINE"] = patients.get("CASE_NO", "").apply(extract_discipline_from_case_no)

    # Previous provider if present under any common name
    def _prevprov(r):
        for f in ["Previous_Provider", "Previus_Provider", "LAST PROVIDER", "LAST_PROVIDER"]:
            if f in r and pd.notna(r[f]) and str(r[f]).strip():
                return str(r[f]).strip()
        return None
    patients["Previous_Provider"] = patients.apply(_prevprov, axis=1)
    
    # Normalize PatientProfile field (could be string or list)
    def _normalize_profile(profile):
        if isinstance(profile, list):
            return profile
        if isinstance(profile, str) and profile.strip() == "[]":
            return []
        return []
    
    patients["PatientProfile_Normalized"] = patients["PatientProfile"].apply(_normalize_profile)

    return clinicians, patients

# ====================== AI PROMPT & SCORING ======================
def _build_ai_payload(clinician_info: Dict[str, Any],
                      candidates: List[Dict[str, Any]],
                      active_case_ids_resolved: List[str]) -> str:
    """
    Build a detailed instruction prompt for Gemini to do deterministic scoring.
    The model must return ONLY JSON array with fields: ID, Match_Score (0..100), Reason.
    """
    # Keep payload at reasonable size
    limited = candidates[:120]

    prompt = f"""
You are an AI assistant with 20 years Experience of Healthcare in US market that RANKS and SCORES home-health patients for a specific clinician using DETERMINISTIC SCORING.

=== OBJECTIVE ===
Given a clinician and a set of patient cases, produce a JSON array where each element contains:
  - "ID": the patient CASE_ID as string
  - "Match_Score": a 0..100 numeric score (higher = better fit) based on EXACT weightings below
  - "Reason": a concise explanation (<= 160 chars) of the key factors

You MUST:
  - Return ONLY valid JSON (no prose outside JSON, no code fences).
  - Include ONLY patients that match the clinician's discipline.
  - Use EXACT percentage weightings defined below.

=== CLINICIAN ===
{json.dumps({
    "ID": clinician_info.get("FOX_PROVIDER_ID") or clinician_info.get("ID",""),
    "Name": clinician_info.get("Name", ""),
    "Discipline": clinician_info.get("Discipline", ""),
    "DisciplineName": clinician_info.get("DisciplineName", ""),
    "Subspecialty": clinician_info.get("Subspecialty", ""),
    "Professional_Specialties": clinician_info.get("specialties", []),
    "Has_Active_Cases": clinician_info.get("ACTIVE_CASES", 0) > 0,
    "Active_Case_Count": clinician_info.get("ACTIVE_CASES", 0),
    "Location": {
        "Latitude": clinician_info.get("Latitude"),
        "Longitude": clinician_info.get("Longitude"),
        "Address": clinician_info.get("LocalAddress"),
        "City": clinician_info.get("CITY"),
        "State": clinician_info.get("STATE")
    }
}, indent=2)}

=== PATIENT CANDIDATES (JSON) ===
{json.dumps(limited, indent=2)}

=== DETERMINISTIC SCORING SYSTEM (TOTAL = 100%) ===

**1. ALREADY TREATED CLINICIAN - 30%**
- If "Previous_Provider" or "PREVIOUS_PROVIDER_ID" equals the clinician's ID: 30 points
- Otherwise: 0 points

**2. PATIENT SPECIFIC CONDITIONS - 25% (5% each condition)**
Check for these conditions in patient data (PatientProfile, Service_Description, etc.):
- Recent surgery: 5 points if mentioned
- Recently discharged from hospitalization: 5 points if mentioned  
- Recent home health discharge: 5 points if mentioned
- Recent fall: 5 points if mentioned
- Recently discharged from same discipline: 5 points if mentioned
Total possible: 25 points

**3. DISTANCE - 20%**
Distance calculation rules:
- If clinician HAS active cases AND patient is within 5 miles of any active case: Use distance between patient and nearest active case
- Otherwise: Use direct distance between clinician and patient

Scoring:
- 0-2 miles: 20 points
- 2-5 miles: 17 points  
- 5-10 miles: 15 points
- 10-20 miles: 10 points
- 20+ miles: 7 points
- 40+ miles: 3 points


**4. PATIENT PROFILE AND CLINICIAN PROFILE MATCHING - 15%**
Match patient conditions/needs with clinician subspecialty:
- Perfect match (patient condition exactly matches clinician specialty): 15 points
- Good match (related specialty): 10 points
- Partial match (somewhat related): 5 points
- No match: 0 points

**5. CASE STATUS - 10%**
- Case Status = "Open issue" or "Pending Assignment": 10 points
- Case Status = "On Hold" with past/nearest requested week date: 10 points (if current), 9 points (if 1 week past), 8 points (if 2+ weeks past)
- Other statuses: 5 points

=== SUBSPECIALTY REFERENCE ===

**PT Subspecialties:**
- Cardiovascular and Pulmonary Clinical Specialist (CCS)
- Neurologic Clinical Specialist (NCS)
- Orthopedic Clinical Specialist (OCS)
- Geriatric Clinical Specialist (GCS)
- Pediatric Clinical Specialist (PCS)
- Oncologic Clinical Specialist (OCS)
- Sports Clinical Specialist (SCS)
- Electrophysiologic Clinical Specialist (ECS)
- Wound Management Specialist (WMS)
- Women's Health Specialist (WCS)
- Certified Orthopedic Manual Therapist (COMT)
- Advanced Certified Orthopedic Manual Therapist (ACOMT)
- Dry Needling (DN)
- Certified Lymphedema Therapist (CLT)
- Certified Strength and Conditioning Specialist (CSCS)
- Certified Hand Therapist (CHT)
- Certified Aging In Place Specialist (CAPS)
- Certified Functional Capacity Evaluator (CFCE)
- Certified Clinical Instructor (CCI)
- Lee Silverman Voice Treatment- BIG (LSVT BIG)
- Seating and Mobility Specialist (ATP/SMS)

**ST Subspecialties:**
- Board-Certified Specialist Certification (BCS)
- Child Language and Language Disorders Certification (BCS-CL)
- Board-Certified Specialist in Fluency and Fluency Disorders Certification (BCS-F)
- Board-Certified Specialist in Swallowing and Swallowing Disorders (BCS-S)
- Lee Silverman Voice Treatment (LSVT LOUD)
- PROMPT for Restructuring Oral Muscular Phonetic Targets Certification
- Picture Exchange Communication System (PECS)
- Certification for Motor Skills for Language Development

**OT Subspecialties:**
- Seating and Mobility Specialist (ATP/SMS)
- Aquatic Therapeutic Exercise Certification (ATRIC)
- Basic DIRFloortime Certification
- Neuro-Developmental Treatment Certification (C/NDT)
- Certified Autism Specialist (CAS)
- Certified Aging in Place Specialist (CAPS)
- Certified Brain Injury Specialist (CBIS)
- Certified Diabetes Care and Education Specialist (CDCES)
- Certified Hand Therapist (CHT)
- Certified Industrial Ergonomic Evaluator (CIEE)
- Certified Industrial Rehabilitation Specialist (CIRS)
- Certified Kinesio Taping Practitioner (CKTP)
- Certified Lifestyle Medicine Diplomate
- Certified Living in Place Professional (CLIPP)
- Certified Lymphedema Therapist (CLT)
- Certified Low Vision Therapist (CLVT)
- Certified Perinatal Health Specialist Training (PHS)
- Certified Neuro Specialist (CNS)
- Certified Psychiatric Rehabilitation Practitioner (CPRP)
- Certified Driver Rehabilitation Specialist (CDRS)
- Certified Hippotherapy Clinical Specialist (HPSC)
- Cognitive Behavioral Therapy for Insomnia (CBT-I)
- Lee Silverman Voice Treatment- BIG (LSVT BIG)
- Physical Agent Modalities (PAM) Certification
- Skills2Care® Certification
- Trauma-Informed Practice Health Certification (TIPHC)

=== SCORING EXAMPLES ===
Example 1: Previous provider match (30) + all conditions (25) + close distance (20) + perfect specialty match (15) + open case (10) = 100 points
Example 2: No previous provider (0) + 2 conditions (10) + medium distance (10) + good specialty match (10) + pending case (10) = 40 points

=== OUTPUT FORMAT (STRICT) ===
Return a JSON array like:
[
  {{"ID":"53416001","Match_Score":93,"Reason":"Previous provider 30pts + surgery+discharge 10pts + close 20pts + ortho match 15pts + open 10pts"}},
  {{"ID":"53416015","Match_Score":65,"Reason":"No prev provider 0pts + fall 5pts + medium dist 10pts + good match 10pts + pending 10pts"}}
]

ONLY the JSON array. No markdown, no backticks, no extra commentary.
Calculate scores precisely using the percentage weights above.
"""
    return prompt

def ai_score(candidates: List[Dict[str,Any]],
             clinician_info: Dict[str,Any],
             active_case_ids_resolved: List[str]) -> List[Dict[str,Any]]:
    """Send everything to Gemini for full scoring; return enriched list or raise."""
    
    if not init_gemini():
        raise RuntimeError("Gemini initialization failed")

    payload_prompt = _build_ai_payload(clinician_info, candidates, [])
    model = genai.GenerativeModel(GEMINI_MODEL)
    
    try:
        resp = model.generate_content(
            payload_prompt,
            generation_config={"temperature": AI_TEMPERATURE, "max_output_tokens": AI_MAX_OUTPUT_TOKENS}
        )
        
        raw = _extract_text_from_gemini_response(resp)
        if not raw:
            raise ValueError("Empty response from Gemini API")
            
        cleaned = _clean_ai_json(raw)
        if not cleaned:
            # Log the raw response for debugging
            print(f"Warning: Could not extract valid JSON from Gemini response. Raw response: {raw[:200]}...")
            raise ValueError(f"Failed to extract JSON from Gemini response: {raw[:100]}...")
            
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            # Log the attempted JSON for debugging
            print(f"Warning: JSON parsing error. Attempted to parse: {cleaned[:200]}...")
            raise ValueError(f"JSON parsing error: {str(e)}. Attempted to parse: {cleaned[:100]}...")
            
        # Validate the response structure
        if not isinstance(data, list):
            raise ValueError(f"Expected a JSON array from Gemini, got: {type(data).__name__}")
            
        # Attach scores back to candidates
        by_id = {str(c["ID"]): c for c in candidates}
        out: List[Dict[str,Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue  # Skip non-dict items
                
            pid = str(item.get("ID", ""))
            if not pid:
                continue  # Skip items without ID
                
            if pid in by_id:
                enriched = by_id[pid].copy()
                try:
                    match_score = float(item.get("Match_Score", 0))
                    enriched["Match_Score"] = match_score
                except (ValueError, TypeError):
                    # Default score if conversion fails
                    enriched["Match_Score"] = 0
                    
                enriched["Reason"] = str(item.get("Reason", ""))
                out.append(enriched)
                
        return out
        
    except Exception as e:
        # Add more context to the exception
        raise type(e)(f"Gemini API error: {str(e)}")

# ====================== MAIN ENTRY ======================
def recommend_patients(clinician_id: str, radius: float = 50.0, top_k: int = 50) -> Dict[str, Any]:
    """
    100% AI-driven ranking (Gemini). Fallback to distance-only if AI fails.
    Matches clinicians with patients based on sub-specialty and patient needs.
    Filters results to only include patients within the specified radius.
    Never raises HTTP exceptions here; returns {"error": "..."} on issues.
    
    Args:
        clinician_id: The ID of the clinician for whom to recommend patients
        radius: Maximum distance in miles to include patients (default: 50.0)
        top_k: The number of recommended patients to return (default: 50)
    """
    t0 = time.time()
    errors: List[str] = []
    ai_used = False
    ai_error = None

    # ---------- Load patient data safely ----------
    try:
        print("Loading patient data...")
        _, patients = load_data()  # Still need patients from JSON
        print(f"Loaded {len(patients)} patients")
    except Exception as e:
        print(f"Error loading patient data: {str(e)}")
        return {"error": f"Failed to load patient data: {e}"}

    # ---------- Fetch clinician from API ----------
    cid = str(clinician_id).strip()
    print(f"Fetching clinician data from API for Provider ID: {cid}")
    
    # Fetch clinician data from API
    clinician_info = fetch_clinician_from_api(cid)
    if not clinician_info:
        return {"error": f"Clinician with Provider ID '{cid}' not found or API error occurred."}
    
    print(f"Found clinician: {clinician_info.get('Name', 'Unknown')} ({clinician_info.get('Discipline', 'Unknown')})")

    # Normalize clinician discipline
    disc_raw = clinician_info.get("Discipline") or clinician_info.get("SPECIALTY") or clinician_info.get("DISCIPLINE") or ""
    normalized = normalize_clinician_discipline(disc_raw)
    print(f"Clinician discipline: {disc_raw} -> normalized: {normalized}")
    
    if normalized not in {"PT","OT","ST"}:
        return {"error": f"Unsupported clinician discipline '{disc_raw}' (normalized='{normalized}')"}
    
    # Get clinician subspecialty
    subspecialty = clinician_info.get("Subspecialty") or ""
    print(f"Clinician subspecialty: {subspecialty}")

    # Clinician coordinates from API data
    clinician_coords = None
    lat = clinician_info.get("Latitude")
    lon = clinician_info.get("Longitude")
    clinician_coords = _coords_from_cols(lat, lon)
    
    # If individual coordinates not available, try facility coordinates
    if not clinician_coords:
        facility_lat = clinician_info.get("Facility_Lat")
        facility_lon = clinician_info.get("Facility_Long")
        clinician_coords = _coords_from_cols(facility_lat, facility_lon)
        
    print(f"Clinician coordinates: {clinician_coords}")

    # ---------- Active case context ----------
    # Since we're using API data, active cases count is in ACTIVE_CASES field
    active_case_ids = []  # For now, we don't have specific active case IDs from API
    active_cases_count = clinician_info.get("ACTIVE_CASES", 0)
    # Map active case IDs -> coordinates if present in patients
    patient_index = patients.set_index("ID", drop=False)
    active_case_coords: List[Tuple[float, float]] = []
    active_case_ids_resolved: List[str] = []
    for ac in active_case_ids:
        if ac and ac in patient_index.index:
            prow = patient_index.loc[ac]
            c = _coords_from_cols(prow.get("CASE_LAT"), prow.get("CASE_LON"))
            if c:
                active_case_coords.append(c)
                active_case_ids_resolved.append(ac)

    # ---------- Filter candidates by discipline ----------
    matches = patients[patients["DISCIPLINE"] == normalized].copy()
    if matches.empty:
        return {"error": f"No patients found for discipline '{normalized}'"}

    # ---------- Build candidate feature rows ----------
    def nearest_active_distance(p_coord: Optional[Tuple[float, float]]) -> Tuple[Optional[float], Optional[str]]:
        if not p_coord or not active_case_coords:
            return None, None
        best_d = None
        best_id = None
        for idx, ac_coord in enumerate(active_case_coords):
            d = calculate_distance(p_coord, ac_coord)
            if d is None:
                continue
            if best_d is None or d < best_d:
                best_d = d
                best_id = active_case_ids_resolved[idx]
        return best_d, best_id

    # Determine if clinician has active cases (using API data)
    has_active_cases = active_cases_count > 0
    print(f"Clinician has {active_cases_count} active cases")
    
    candidates: List[Dict[str, Any]] = []
    for _, row in matches.iterrows():
        r = row.to_dict()
        # Coordinates
        p_coord = _coords_from_cols(r.get("CASE_LAT"), r.get("CASE_LON"))
        d_clin = calculate_distance(clinician_coords, p_coord) if clinician_coords and p_coord else None
        d_ac, near_ac_id = nearest_active_distance(p_coord)

        # Implement priority distance calculation rules
        primary_distance = None
        distance_type = ""
        
        if has_active_cases:
            # Rule: If clinician has active cases, use distance to clinician
            primary_distance = d_clin
            distance_type = "Distance_to_Clinician"
        else:
            # Rule: If no active cases, prefer distance to nearest active case, fallback to clinician
            if d_ac is not None:
                primary_distance = d_ac
                distance_type = "Distance_to_Nearest_Active_Case"
            else:
                primary_distance = d_clin
                distance_type = "Distance_to_Clinician"

        # Check if previous provider matches clinician (highest priority)
        prev_provider = r.get("Previous_Provider") or r.get("Previus_Provider")
        is_previous_provider_match = (prev_provider == cid) if prev_provider else False

        # Event flags by presence of date fields
        has_adm  = _bool_from_date_field(r.get("ADMISSION_DATE"))
        has_dis  = _bool_from_date_field(r.get("DISCHARGE_DATE"))
        has_non  = _bool_from_date_field(r.get("NON_ADMIT_DATE"))
        has_hold = _bool_from_date_field(r.get("HOLD_DATE") or r.get("Hold_Follow_Up_Date") or r.get("HOLD_TILL_DATE"))

        candidates.append({
            "ID": str(r.get("ID","")),
            "CASE_ID": r.get("CASE_ID"),
            "CASE_NO": r.get("CASE_NO"),
            "NAME": (str(r.get("First_Name","")) + " " + str(r.get("Last_Name",""))).strip(),
            "City": r.get("City") or r.get("CITY") or "",
            "State": r.get("State") or r.get("STATE") or "",
            "ZIP": r.get("ZIP") or r.get("Zip") or "",
            "DISCIPLINE": r.get("DISCIPLINE"),
            "Case_Status": r.get("CASE_STATUS") or "",
            "Previous_Provider": prev_provider,
            "Is_Previous_Provider_Match": is_previous_provider_match,
            "Distance_to_Clinician": d_clin,
            "Distance_to_Nearest_Active_Case": d_ac,
            "Primary_Distance": primary_distance,
            "Distance_Type_Used": distance_type,
            "Clinician_Has_Active_Cases": has_active_cases,
            "Nearest_Active_Case_ID": near_ac_id,
            # Raw dates (as strings) + flags:
            "ADMISSION_DATE": r.get("ADMISSION_DATE"),
            "DISCHARGE_DATE": r.get("DISCHARGE_DATE"),
            "NON_ADMIT_DATE": r.get("NON_ADMIT_DATE"),
            "HOLD_DATE": r.get("HOLD_DATE"),
            "Hold_Follow_Up_Date": r.get("Hold_Follow_Up_Date"),
            "HOLD_TILL_DATE": r.get("HOLD_TILL_DATE"),
            "Has_Admission": has_adm,
            "Has_Discharge": has_dis,
            "Has_NonAdmit": has_non,
            "Has_Hold": has_hold,
            "PatientProfile": r.get("PatientProfile_Normalized", r.get("PatientProfile", [])),
        })

    # ---------- AI scoring ----------
    scored: List[Dict[str, Any]] = []
    try:
        print("Attempting to use Gemini AI for scoring...")
        
        # Add timeout to prevent hanging
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("AI scoring timed out after 30 seconds")
        
        # Set up timeout for Windows (alternative approach)
        try:
            print(f"Starting AI scoring for {len(candidates)} candidates...")
            scored = ai_score(candidates, clinician_info, active_case_ids_resolved)
            ai_used = True
            print(f"Successfully scored {len(scored)} candidates using Gemini AI")
        except Exception as ai_exception:
            print(f"AI scoring failed with exception: {str(ai_exception)}")
            raise ai_exception
            
    except Exception as e:
        ai_error = str(e)
        errors.append(f"AI scoring failed: {ai_error}")
        print(f"AI scoring failed: {ai_error}")
        # Log detailed error for debugging
        import traceback
        traceback_str = traceback.format_exc()
        print(f"Detailed error traceback:\n{traceback_str}")

    # ---------- Fallback if AI failed or returned nothing ----------
    if not scored:
        # Priority-based ranking with previous provider match as highest priority
        def _fallback_key(c: Dict[str, Any]):
            # Priority 1: Previous provider match (lower number = higher priority)
            prev_provider_priority = 0 if c.get("Is_Previous_Provider_Match", False) else 1
            
            # Priority 2: Use the primary distance based on active case rules
            primary_distance = c.get("Primary_Distance")
            if primary_distance is None:
                primary_distance = float("inf")
            
            # Priority 3: Secondary distance (always distance to clinician as tiebreaker)
            secondary_distance = c.get("Distance_to_Clinician")
            if secondary_distance is None:
                secondary_distance = float("inf")
                
            # Priority 4: Case ID for final deterministic ordering
            case_id = str(c.get("ID", ""))
            
            return (prev_provider_priority, primary_distance, secondary_distance, case_id)
            
        ranked = sorted(candidates, key=_fallback_key)

        # Convert to pseudo-score (still JSON safe)
        scored = []
        for idx, c in enumerate(ranked, 1):
            cpy = c.copy()
            
            # Calculate score based on priority rules
            base_score = max(0.0, 100.0 - (idx-1) * (100.0/max(1, len(ranked))))
            
            # Boost score significantly for previous provider match
            if c.get("Is_Previous_Provider_Match", False):
                base_score = max(base_score, 85.0)  # Ensure minimum 85 for previous provider
                if base_score < 95.0:
                    base_score += 10.0  # Add bonus points
            
            cpy["Match_Score"] = min(100.0, base_score)
            
            # Build reason string
            reason_bits = []
            if c.get("Is_Previous_Provider_Match", False):
                reason_bits.append("previous provider match")
            
            distance_info = ""
            if c.get("Primary_Distance") is not None:
                distance_type = c.get("Distance_Type_Used", "distance")
                distance_info = f"{distance_type.replace('_', ' ').lower()} ~{c['Primary_Distance']}mi"
                reason_bits.append(distance_info)
            elif c.get("Distance_to_Clinician") is not None:
                reason_bits.append(f"distance to clinician ~{c['Distance_to_Clinician']}mi")
            
            reason_bits.append(f"discipline {c.get('DISCIPLINE')}")
            
            if c.get("Clinician_Has_Active_Cases"):
                reason_bits.append("clinician has active cases")
            
            cpy["Reason"] = ", ".join(reason_bits)[:160]
            scored.append(cpy)

    # ---------- Final sort + trim ----------
    # Sort with priority: Previous provider match > Score > Primary distance > Secondary distance > ID
    def _final_sort_key(x):
        # Priority 1: Previous provider match (inverse boolean for sorting)
        prev_provider_priority = not x.get("Is_Previous_Provider_Match", False)
        
        # Priority 2: Match score (negative for descending)
        match_score = -float(x.get("Match_Score", 0))
        
        # Priority 3: Primary distance (the distance type determined by active case rules)
        primary_dist = x.get("Primary_Distance")
        if primary_dist is None:
            primary_dist = float("inf")
        
        # Priority 4: Distance to clinician as tiebreaker
        secondary_dist = x.get("Distance_to_Clinician")
        if secondary_dist is None:
            secondary_dist = float("inf")
        
        # Priority 5: Case ID for deterministic ordering
        case_id = str(x.get("ID", ""))
        
        return (prev_provider_priority, match_score, primary_dist, secondary_dist, case_id)
    
    scored.sort(key=_final_sort_key)
    
    # ---------- Apply radius filtering ----------
    print(f"Applying radius filter: {radius} miles")
    candidates_before_radius = len(scored)
    
    # Filter candidates by radius - only include patients within specified distance
    filtered_by_radius = []
    for candidate in scored:
        # Get the distance used for primary distance calculation
        distance_to_check = candidate.get("Primary_Distance")
        
        # If no primary distance, fallback to direct distance to clinician
        if distance_to_check is None:
            distance_to_check = candidate.get("Distance_to_Clinician")
        
        # If still no distance, skip this candidate (shouldn't happen but safety check)
        if distance_to_check is None:
            print(f"⚠️ Skipping candidate {candidate.get('ID')} - no distance information")
            continue
            
        # Include candidate if within radius
        if distance_to_check <= radius:
            filtered_by_radius.append(candidate)
        else:
            print(f"Filtered out patient {candidate.get('NAME', 'Unknown')} (ID: {candidate.get('ID')}) - distance {distance_to_check:.1f} miles > {radius} miles")
    
    print(f"Radius filtering: {candidates_before_radius} → {len(filtered_by_radius)} candidates")
    
    # Take the top results from filtered candidates
    out_recs = filtered_by_radius[:max(1, min(top_k, len(filtered_by_radius)))]

    result = {
        "search_params": {
            "provider_id": cid,
            "radius_miles": radius,
            "limit": top_k
        },
        "filtering": {
            "total_before_radius_filter": candidates_before_radius,
            "total_after_radius_filter": len(filtered_by_radius),
            "filtered_out_by_radius": candidates_before_radius - len(filtered_by_radius)
        },
        "clinician": {
            "ID": cid,
            "FOX_PROVIDER_ID": clinician_info.get("FOX_PROVIDER_ID"),
            "Name": clinician_info.get("Name", ""),
            "FirstName": clinician_info.get("FirstName", ""),
            "LastName": clinician_info.get("LastName", ""),
            "Discipline": normalized,
            "DisciplineName": clinician_info.get("DisciplineName", ""),
            "Subspecialty": clinician_info.get("Subspecialty", ""),
            "Specialties": clinician_info.get("specialties", []),
            "Has_Active_Cases": has_active_cases,
            "Active_Cases_Count": active_cases_count,
            "Location": {
                "Latitude": clinician_info.get("Latitude"),
                "Longitude": clinician_info.get("Longitude"),
                "Address": clinician_info.get("LocalAddress"),
                "City": clinician_info.get("CITY"),
                "State": clinician_info.get("STATE")
            },
            "Coordinates": clinician_coords,
        },
        "priority_rules": {
            "distance_calculation": "Distance_to_Clinician" if has_active_cases else "Direct Distance to Clinician",
            "highest_priority": "Previous Provider Match",
            "previous_provider_matches": len([r for r in out_recs if r.get("Is_Previous_Provider_Match", False)]),
            "active_cases_considered": has_active_cases,
        },
        "ai": {
            "model": GEMINI_MODEL,
            "used": ai_used,
            "error": ai_error,
        },
        "processing_time_seconds": round(time.time() - t0, 3),
        "total_candidates": len(candidates),
        "top_k": len(out_recs),
        "requested_limit": top_k,
        "errors": errors,
        "recommendations": clean_nan_values(out_recs),
    }
    return clean_nan_values(result)

# ========== Manual test ==========
if __name__ == "__main__":
    print("=" * 50)
    print("STARTING MANUAL TEST")
    print("=" * 50)
    
    # Test Gemini initialization directly
    print("\nTesting Gemini initialization...")
    gemini_ready = init_gemini()
    print(f"Gemini initialization result: {gemini_ready}")
    
    # Test with a clinician that has active cases
    test_id = "816b9599-4f37-40d3-9886-a75d9c915782"  # Dr. Ayesha Malik who has active case 53416034
    # Specify how many recommendations to return
    test_limit = 10
    
    print(f"\nRunning recommendation for clinician ID: {test_id} with limit: {test_limit}")
    out = recommend_patients(test_id, top_k=test_limit)
    
    # Check if AI was used
    ai_info = out.get("ai", {})
    priority_info = out.get("priority_rules", {})
    clinician_info = out.get("clinician", {})
    
    print(f"\nClinician info:")
    print(f"- Name: {clinician_info.get('Name')}")
    print(f"- Discipline: {clinician_info.get('Discipline')}")
    print(f"- Subspecialty: {clinician_info.get('Subspecialty')}")
    print(f"- Has Active Cases: {clinician_info.get('Has_Active_Cases')}")
    print(f"- Active Cases: {clinician_info.get('Active_Cases_Resolved')}")
    
    print(f"\nPriority rules applied:")
    print(f"- Distance calculation method: {priority_info.get('distance_calculation')}")
    print(f"- Highest priority factor: {priority_info.get('highest_priority')}")
    print(f"- Previous provider matches found: {priority_info.get('previous_provider_matches')}")
    
    print(f"\nAI usage summary:")
    print(f"- Model: {ai_info.get('model')}")
    print(f"- Used: {ai_info.get('used')}")
    print(f"- Error: {ai_info.get('error')}")
    
    print("\nRecommendation summary:")
    print(f"- Total candidates: {out.get('total_candidates')}")
    print(f"- Recommendations returned: {out.get('top_k')}")
    
    # Show top 3 recommendations with key fields
    recommendations = out.get("recommendations", [])
    print(f"\nTop 3 recommendations:")
    for i, rec in enumerate(recommendations[:3], 1):
        print(f"{i}. Patient {rec.get('NAME')} (ID: {rec.get('ID')})")
        print(f"   Score: {rec.get('Match_Score')}")
        print(f"   Previous Provider Match: {rec.get('Is_Previous_Provider_Match')}")
        print(f"   Distance to Clinician: {rec.get('Distance_to_Clinician')} mi")
        print(f"   Primary Distance: {rec.get('Primary_Distance')} mi ({rec.get('Distance_Type_Used')})")
        print(f"   Reason: {rec.get('Reason')}")
        print()
    
    if out.get("errors"):
        print("\nErrors encountered:")
        for error in out.get("errors", []):
            print(f"- {error}")
    
    print("\nFull output:")
    print(json.dumps(out, indent=2))
