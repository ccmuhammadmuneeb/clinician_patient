"""
recommender.py

Muhammad Muneeb 
Employee ID: 19242

Last Edited: 22nd September 2025



"""

import os
import json
import time
import re
import math
import requests
import pandas as pd
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime
import traceback
# import google.auth - moved to function level to avoid module-level hanging

# Custom deduplication logic
from deduplicate import deduplicate_recommendations

# Import for threading and caching
import threading
import concurrent.futures
from functools import lru_cache
import hashlib

# ===== Gemini import, lazy init guard =====
try:
    import google.generativeai as genai  # type: ignore
    GEMINI_IMPORT_OK = True
except Exception:
    genai = None  # type: ignore
    GEMINI_IMPORT_OK = False

# ====================== CONFIG ======================
# Gemini API Key - Replace with your actual API key
GOOGLE_API_KEY = "AIzaSyC09ZGXx4qI5FnSVg5N0Nf0rmXmrtC-LOc"

# Service account file for Google API authentication (fallback)
SERVICE_ACCOUNT_FILE = os.path.abspath(r"C:\Users\MUHAMMADMUNEEB3\v2\aiml-365220-99e7125f22cc.json")
# Gemini model configuration
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash") 
AI_MAX_OUTPUT_TOKENS = 8000  # Increased from 1600 to handle 30+ patients (each ~150 tokens)
AI_TEMPERATURE = 0.1

# AI performance optimization settings
AI_TIMEOUT = 45  # Timeout for individual batches (seconds) - reduced for better responsiveness
AI_GLOBAL_TIMEOUT = 180  # Overall timeout for the entire process 
AI_BATCH_SIZE = 10  # Reduced batch size for faster processing
AI_MAX_WORKERS = 8  # Increased parallel workers
AI_CACHE_SIZE = 200  # Maximum number of cached responses

# AI response cache - stores previously computed scores
ai_response_cache = {}

# API Configuration
CLINICIAN_API_BASE = "https://uat-webservices.mtbc.com/Fox/api/RegionalDirector/GetClinicianDetails_V1"
PROXIMITY_CASES_API_BASE = "https://uat-webservices.mtbc.com/Fox/api/RegionalDirector/GetProximityCasesForProvider"

def fetch_clinician_details_from_api(provider_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch clinician details from the API for a given provider ID.
    
    Args:
        provider_id: The provider ID to fetch details for
        
    Returns:
        Dict containing clinician details, or None if request fails
    """
    try:
        url = f"{CLINICIAN_API_BASE}?ProviderId={provider_id}"
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'PracticeCode': '1012714'
        }
        print(f"Making request to: {url}")
        print(f"Headers: {headers}")
        
        response = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        print(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response data type: {type(data)}")
            
            # Handle the new API response structure with success/message/data
            if isinstance(data, dict) and "success" in data:
                if data.get("success") == True and "data" in data:
                    clinician_data = data["data"]
                    print(f"Found clinician: {clinician_data.get('FirstName', '')} {clinician_data.get('LastName', '')}")
                    return clinician_data
                else:
                    print(f"API returned error: {data.get('message', 'Unknown error')}")
                    return None
            # Handle legacy response format
            elif isinstance(data, list) and len(data) > 0:
                clinician_info = data[0]  # Take first clinician
                print(f"Found clinician: {clinician_info.get('Name', 'Unknown')}")
                return clinician_info
            elif isinstance(data, dict):
                print(f"Found clinician: {data.get('Name', 'Unknown')}")
                return data
            else:
                print("No clinician data in response")
                return None
        else:
            print(f"API request failed with status: {response.status_code}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"⏰ API request timeout after {API_TIMEOUT} seconds for provider {provider_id}")
        return {
            "Name": "Timeout - Provider Not Found",
            "FOX_PROVIDER_ID": provider_id,
            "Discipline": "PT",
            "Latitude": None,
            "Longitude": None,
            "Facility_Lat": None,  
            "Facility_Long": None,
            "timeout_occurred": True,
            "error_message": f"API request timed out after {API_TIMEOUT} seconds"
        }
    except Exception as e:
        print(f"Error fetching clinician details: {str(e)}")
        return None
API_HEADERS = {
    "Accept": "application/json",
    "PracticeCode": "1012714"  # Add PracticeCode header as required
}
API_TIMEOUT = 120  # 2 minutes timeout for API connections

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
                print("API returned empty array - no clinician data found")
                return None
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

def fetch_proximity_cases_from_api(provider_id: str, discipline_id: str, radius_miles: float) -> Optional[Dict[str, Any]]:
    """
    Fetch proximity cases data from the API using Provider ID, Discipline ID, and radius
    
    Args:
        provider_id: The FOX_PROVIDER_ID to fetch cases for
        discipline_id: The discipline ID from clinician data
        radius_miles: The radius in miles to search for cases
        
    Returns:
        Dictionary containing cases data or None if failed
    """
    try:
        print(f"Fetching proximity cases for Provider ID: {provider_id}, Discipline: {discipline_id}, Radius: {radius_miles} miles")
        
        # Build API URL with parameters
        url = f"{PROXIMITY_CASES_API_BASE}?ProviderID={provider_id}&disciplineId={discipline_id}&radiusMiles={radius_miles}"
        print(f"Proximity Cases API URL: {url}")
        print(f"Headers: {API_HEADERS}")
        
        response = requests.get(url, headers=API_HEADERS, timeout=API_TIMEOUT)
        print(f"Proximity Cases API Response Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Proximity Cases API request failed with status {response.status_code}: {response.text}")
            return None
            
        # Parse JSON response
        api_data = response.json()
        print(f"Proximity Cases API Response structure: {list(api_data.keys()) if isinstance(api_data, dict) else type(api_data)}")
        
        # Handle both direct response and wrapped response
        cases_data = None
        if isinstance(api_data, dict):
            # Check for success wrapper
            if "success" in api_data and not api_data.get("success", False):
                print(f"Proximity Cases API returned unsuccessful response: {api_data.get('message', 'Unknown error')}")
                return None
                
            # Try to find cases data in different possible locations
            if "Cases" in api_data:
                cases_data = api_data["Cases"]
            elif "data" in api_data and isinstance(api_data["data"], dict) and "Cases" in api_data["data"]:
                cases_data = api_data["data"]["Cases"]
            elif "data" in api_data:
                cases_data = api_data["data"]
            else:
                cases_data = api_data
        else:
            cases_data = api_data
            
        if not cases_data:
            print("No cases data found in API response")
            return None
            
        print(f"Found {len(cases_data)} case groups in API response")
        return {"Cases": cases_data}
        
    except requests.exceptions.Timeout:
        print(f"⏰ Proximity cases API request timeout after {API_TIMEOUT} seconds")
        return {
            "Cases": [],
            "timeout_occurred": True,
            "error_message": f"Proximity cases API timed out after {API_TIMEOUT} seconds"
        }
    except requests.RequestException as e:
        print(f"Network error fetching proximity cases: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON parsing error in proximity cases: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching proximity cases: {str(e)}")
        traceback.print_exc()
        return None

def transform_proximity_cases_to_patients(cases_data: Dict[str, Any], discipline: str = 'PT') -> List[Dict[str, Any]]:
    """
    Transform the proximity cases API response into patient format compatible with existing AI system
    
    Args:
        cases_data: Raw cases data from proximity API
        discipline: The discipline these patients are for (PT, OT, ST)
        
    Returns:
        List of patient dictionaries in the expected format
    """
    patients = []
    active_case_patients = []  # Separate list to collect active case info
    
    try:
        cases = cases_data.get("Cases", [])
        print(f"Processing {len(cases)} case groups from proximity API...")
        
        for case_group in cases:
            if not isinstance(case_group, dict):
                continue
                
            # Get active case coordinates for distance calculation
            active_case = case_group.get("ClinicianActiveCase")
            active_case_coords = None
            active_case_name = None
            active_case_id = None
            
            if active_case and isinstance(active_case, dict):
                active_lat = active_case.get("Latitude")
                active_lon = active_case.get("Longitude")
                active_case_id = str(active_case.get("CASE_ID", ""))
                active_case_name = f"{active_case.get('FIRST_NAME', '')} {active_case.get('LAST_NAME', '')}".strip()
                if not active_case_name:
                    active_case_name = f"Active Case {active_case_id}"
                    
                if active_lat and active_lon:
                    try:
                        active_case_coords = (float(active_lat), float(active_lon))
                        # Add the active case to our list for later distance calculations
                        active_case_patient = transform_single_case_to_patient(
                            active_case,
                            is_active_case=True,
                            active_case_coords=None,
                            discipline=discipline
                        )
                        if active_case_patient:
                            active_case_patients.append(active_case_patient)
                    except (ValueError, TypeError):
                        active_case_coords = None
                
            # Process nearby cases (available patients needing assignment)
            nearby_cases = case_group.get("NearbyCases", [])
            for nearby_case in nearby_cases:
                if isinstance(nearby_case, dict):
                    # Store the active case coords with each patient for later use
                    patient = transform_single_case_to_patient(
                        nearby_case, 
                        is_active_case=False, 
                        active_case_coords=active_case_coords, 
                        discipline=discipline,
                        active_case_name=active_case_name,
                        active_case_id=active_case_id
                    )
                    if patient:
                        # Store active case coordinates with each patient
                        if active_case_coords:
                            patient["active_case_coords"] = active_case_coords
                            patient["active_case_id"] = active_case_id
                        patients.append(patient)
        
        # Add the active case patients to the full list
        if active_case_patients:
            print(f"Added {len(active_case_patients)} active cases to dataset for distance calculations")
            patients.extend(active_case_patients)
            
        print(f"Transformed {len(patients)} patients from proximity cases API")
        return patients
                        
        print(f"Transformed {len(patients)} patients from proximity cases API")
        return patients
        
    except Exception as e:
        print(f"Error transforming proximity cases: {str(e)}")
        traceback.print_exc()
        return []

def transform_single_case_to_patient(case_data: Dict[str, Any], is_active_case: bool = False, active_case_coords: Optional[Tuple[float, float]] = None, discipline: str = 'PT', active_case_name: Optional[str] = None, active_case_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Transform a single case from proximity API into patient format
    
    Args:
        case_data: Single case data from API
        is_active_case: Whether this is an active case or nearby case
        active_case_coords: Coordinates of the active case for distance calculation
        discipline: The discipline this patient is for (PT, OT, ST)
        active_case_name: Name of the related active case (if any)
        active_case_id: ID of the related active case (if any)
        
    Returns:
        Patient dictionary or None if transformation fails
    """
    try:
        # Extract patient info - handle both active case and nearby case formats
        case_id = case_data.get("CASE_ID")
        
        if not case_id:
            # Try alternative ID fields
            case_id = (case_data.get("ID") or 
                      case_data.get("CaseId") or 
                      case_data.get("Case_ID") or
                      case_data.get("PatientId") or
                      case_data.get("PATIENT_ID"))
            
        if not case_id:
            return None
            
        # For active cases, names are FIRST_NAME/LAST_NAME
        # For nearby cases, names are PatientFirstName/PatientLastName
        first_name = (case_data.get("FIRST_NAME") or 
                     case_data.get("PatientFirstName") or "")
        last_name = (case_data.get("LAST_NAME") or 
                    case_data.get("PatientLastName") or "")
        
        # Build patient record compatible with existing system
        patient = {
            "ID": str(case_id),
            "CASE_ID": case_id,
            "CASE_NO": case_data.get("CASE_NO") or case_data.get("RT_CASE_NO") or "",
            "First_Name": first_name,
            "Last_Name": last_name,
            "NAME": f"{last_name}, {first_name}".strip(" ,"),
            
            # Location data
            "CASE_LAT": case_data.get("Latitude") or case_data.get("CASE_LAT"),
            "CASE_LON": case_data.get("Longitude") or case_data.get("CASE_LON"),
            "Address": case_data.get("Address", ""),
            "City": case_data.get("City", ""),
            "State": case_data.get("State", ""),
            "ZIP": case_data.get("ZIP", ""),
            
            # Medical/Case info
            "DISCIPLINE": case_data.get("Discipline", ""),
            "discipline_id": case_data.get("DISCIPLINE_ID") or case_data.get("discipline_id"),
            "CASE_STATUS": case_data.get("CASE_STATUS", ""),
            "CASE_STATUS_ID": case_data.get("CASE_STATUS_ID"),
            
            # Dates
            "ADMISSION_DATE": case_data.get("ADMISSION_DATE"),
            "DISCHARGE_DATE": case_data.get("DISCHARGE_DATE"),
            "HOLD_DATE": case_data.get("HOLD_DATE"),
            "HOLD_TILL_DATE": case_data.get("HOLD_TILL_DATE"),
            "START_CARE_DATE": case_data.get("START_CARE_DATE"),
            "NON_ADMIT_DATE": case_data.get("NON_ADMIT_DATE"),
            "Hold_Follow_Up_Date": case_data.get("Hold_Follow_Up_Date"),
            
            # Provider info
            "TREATING_PROVIDER_ID": case_data.get("TREATING_PROVIDER_ID"),
            "Previous_Provider": str(case_data.get("TREATING_PROVIDER_ID")) if case_data.get("TREATING_PROVIDER_ID") else None,
            
            # Additional info
            "Gender": case_data.get("Gender", ""),
            "Date_Of_Birth": case_data.get("Date_Of_Birth"),
            "PatientProfile": case_data.get("PatientProfile", "[]"),
            "PATIENT_ACCOUNT": case_data.get("PATIENT_ACCOUNT"),
            "CHART_ID": case_data.get("CHART_ID", ""),
            "SSN": case_data.get("SSN"),
            "FinancialClassCode": case_data.get("FinancialClassCode", ""),
            "region": case_data.get("region", ""),
            "LocCode": case_data.get("LocCode", ""),
            "LocName": case_data.get("LocName", ""),
            "FacilityName": case_data.get("FacilityName", ""),
            "FacilityCode": case_data.get("FacilityCode", ""),
            "HOLD_DURATION": case_data.get("HOLD_DURATION", ""),
            "Parent_Id": case_data.get("Parent_Id"),
            
            # Metadata
            "is_active_case": is_active_case,
            "active_case_name": active_case_name,
            "modified_date": case_data.get("modified_date") or case_data.get("MODIFIED_DATE")
        }
        
        # Set discipline based on what was requested from API
        patient["DISCIPLINE"] = discipline.upper()
        
        # Calculate distance from active case if coordinates available
        if active_case_coords and not is_active_case:
            patient_lat = patient.get("CASE_LAT")
            patient_lon = patient.get("CASE_LON") 
            if patient_lat and patient_lon:
                try:
                    patient_coords = (float(patient_lat), float(patient_lon))
                    distance = calculate_distance(active_case_coords, patient_coords)
                    if distance is not None:
                        patient["DISTANCE_FROM_ACTIVE_CASE"] = round(distance, 2)
                        patient["DISTANCE_FROM_ACTIVE_CASE_UNIT"] = "miles"
                except (ValueError, TypeError):
                    pass
            
        return patient
        
    except Exception as e:
        print(f"Error transforming single case {case_data.get('CASE_ID', 'unknown')}: {str(e)}")
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
    
    print("[INIT] Initializing Gemini AI...")
    
    if _gemini_initialized:
        print("[SUCCESS] Gemini already initialized")
        return True
        
    if not GEMINI_IMPORT_OK:
        print("[ERROR] Gemini import failed - google.generativeai package not installed")
        return False
        
    try:
        # Check if API key is set
        if not GOOGLE_API_KEY or GOOGLE_API_KEY == "AIzaSyDYour-API-Key-Here":
            print("[WARNING] GEMINI API KEY NOT SET!")
            print("[TO FIX]:")
            print("   1. Go to: https://aistudio.google.com/app/apikey")
            print("   2. Get your API key")
            print("   3. Replace the GOOGLE_API_KEY value in recommender.py")
            print("   4. Restart the server")
            return False
            
        print(f"[API_KEY] Using API key: {GOOGLE_API_KEY[:12]}...")
        print(f"[MODEL] Model: {GEMINI_MODEL}")
        
        # Configure with API key
        genai.configure(api_key=GOOGLE_API_KEY)
        
        # Test API connection
        print("[TEST] Testing Gemini API connection...")
        model = genai.GenerativeModel(GEMINI_MODEL)
        test_resp = model.generate_content("Respond with only 'OK' to verify connection.")
        test_text = _extract_text_from_gemini_response(test_resp)
        print(f"[SUCCESS] Gemini API test successful: {test_text.strip()}")
            
        _gemini_initialized = True
        print("[COMPLETE] Gemini initialization completed successfully!")
        return True
        
    except Exception as e:
        print(f"[ERROR] Gemini initialization failed: {str(e)}")
        print("[SOLUTIONS] Common solutions:")
        print("   - Check your API key is correct")
        print("   - Ensure you have internet connection") 
        print("   - Verify your Google AI Studio account is active")
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

def explain_distance_calculation(candidate: Dict[str, Any]) -> str:
    """Create a clear explanation of which distance was used and why"""
    # Get the actual distance source from the candidate
    distance_source = candidate.get("Distance_Source", "")
    primary_distance = candidate.get("Primary_Distance")
    
    if distance_source == "active_case":
        # Distance to active case was used - ensure it's not None
        distance = candidate.get("Distance_to_Nearest_Active_Case")
        if distance is not None:
            return f"distance to nearest active case: {distance:.1f} mi"
        else:
            # This should not happen with our fixed logic, but just in case
            return "distance to active case (unavailable)"
    else:
        # Direct distance to clinician was used
        distance = candidate.get("Distance_to_Clinician")
        if distance is not None:
            return f"distance to clinician: {distance:.1f} mi"
        else:
            return "distance to clinician (unavailable)"
            
    # Fallback in case neither source has valid distance
    if primary_distance is not None:
        return f"distance: {primary_distance:.1f} mi (source unclear)"
    else:
        return "distance information unavailable"

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

def load_data(provider_id: str = "99910678", radius: float = 25.0) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """
    Load clinician data from API and proximity cases as patient data.
    
    Args:
        provider_id: Provider ID for the clinician (default from your example)
        radius: Search radius in miles (default 25.0)
    
    Returns:
        Tuple[Dict[str, Any], pd.DataFrame]: Clinician data and patients DataFrame
    """
    start_time = time.time()
    try:
        print(f"⏱️ [TIMING] Starting data load for Provider ID: {provider_id}")
        
        # Get clinician data from API
        clinician_start = time.time()
        print(f"Loading clinician data from API for Provider ID: {provider_id}")
        clinician_data = fetch_clinician_details_from_api(provider_id)
        clinician_time = time.time() - clinician_start
        print(f"⏱️ [TIMING] Clinician API took: {clinician_time:.2f} seconds")
        
        if not clinician_data:
            print(f"No clinician data found for Provider ID: {provider_id}")
            raise ValueError(f"Provider ID '{provider_id}' not found in the system")
        
        # Check if clinician data indicates a timeout occurred
        if clinician_data.get("timeout_occurred", False):
            print(f"⚠️ Clinician API timed out, proceeding with limited data")
            print(f"   Error: {clinician_data.get('error_message', 'Unknown timeout error')}")
        
        
        # Extract discipline_id from clinician data - check multiple field names
        discipline = (clinician_data.get('DiciplineCode') or     # API has typo "DiciplineCode"  
                     clinician_data.get('Discipline') or 
                     'PT')  # Default to PT
        discipline_id = get_discipline_id(discipline)
        
        print(f"Loading proximity cases for Provider: {provider_id}, Discipline: {discipline} (ID: {discipline_id}), Radius: {radius}")
        
        # Get proximity cases data
        proximity_start = time.time()
        proximity_cases = fetch_proximity_cases_from_api(provider_id, discipline_id, radius)
        proximity_time = time.time() - proximity_start
        print(f"⏱️ [TIMING] Proximity cases API took: {proximity_time:.2f} seconds")
        
        if proximity_cases:
            # Check if proximity cases indicates timeout
            if proximity_cases.get("timeout_occurred", False):
                print(f"⚠️ Proximity cases API timed out, using empty patient list")
                print(f"   Error: {proximity_cases.get('error_message', 'Unknown timeout error')}")
                patient_list = []  # Use empty list for timeout
            else:
                # Transform proximity cases to patient format with discipline info
                transform_start = time.time()
                patient_list = transform_proximity_cases_to_patients(proximity_cases, discipline=discipline)
                transform_time = time.time() - transform_start
                print(f"⏱️ [TIMING] Data transformation took: {transform_time:.2f} seconds")
                print(f"Loaded {len(patient_list)} patients from proximity cases API")
            
        else:
            print("No proximity cases found for specific discipline")
            print(f"🔄 Trying fallback with PT discipline for broader patient pool...")
            
            # Fallback: Try with PT discipline ID if original failed and it wasn't PT
            if discipline.upper() != 'PT':
                fallback_discipline_id = get_discipline_id('PT')
                print(f"   Attempting fallback with PT discipline ID: {fallback_discipline_id}")
                proximity_cases = fetch_proximity_cases_from_api(provider_id, fallback_discipline_id, radius)
                
                if proximity_cases:
                    patient_list = transform_proximity_cases_to_patients(proximity_cases, discipline='PT')
                    print(f"   ✅ Fallback successful! Loaded {len(patient_list)} PT patients for {discipline} clinician")
                else:
                    print(f"   ❌ Fallback also failed")
                    patient_list = []
            else:
                patient_list = []
        
        # Convert to DataFrame for compatibility with existing code
        patients_df = pd.DataFrame(patient_list)
        
        if not patients_df.empty and "CASE_ID" in patients_df.columns:
                # Ensure ID column - Don't set as index since we need it as a column
                patients_df["ID"] = patients_df["CASE_ID"].astype(str)
                
                # Don't set index - keep ID as a regular column
                # patients_df.set_index("ID", inplace=True)
                
                # Patient coordinates (already formatted as string)
                # Discipline (already extracted)
                # Patient profile normalization
                def _normalize_profile(profile):
                    if isinstance(profile, list):
                        return profile
                    if isinstance(profile, str) and profile.strip() == "[]":
                        return []
                    return []
                
                if "PatientProfile" in patients_df.columns:
                    patients_df["PatientProfile_Normalized"] = patients_df["PatientProfile"].apply(_normalize_profile)
                else:
                    patients_df["PatientProfile_Normalized"] = [[] for _ in range(len(patients_df))]
        else:
            print("No proximity cases found, returning empty DataFrame")
            patients_df = pd.DataFrame()
        
        total_time = time.time() - start_time
        print(f"⏱️ [TIMING] Total data load completed in: {total_time:.2f} seconds")
        return clinician_data, patients_df
        
    except Exception as e:
        print(f"Error loading data from API: {e}")
        # Don't create mock data - re-raise the error
        raise e

def get_discipline_id(discipline: str) -> str:
    """
    Map discipline name to discipline ID based on your API specification
    """
    discipline_mapping = {
        'PT': '605108',  # Physical Therapy - Correct ID
        'OT': '605107',  # Occupational Therapy - Correct ID  
        'ST': '605109',  # Speech Therapy - Correct ID

        # Add more mappings as needed
    }
    
    return discipline_mapping.get(discipline.upper(), '605108')  # Default to PT

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
    "Has_Active_Cases": int(clinician_info.get("ACTIVE_CASES", 0) or 0) > 0,
    "Active_Case_Count": int(clinician_info.get("ACTIVE_CASES", 0) or 0),
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

**CRITICAL: THE MATCH_SCORE MUST EQUAL THE SUM OF ALL POINTS FROM CATEGORIES BELOW**

**1. ALREADY TREATED CLINICIAN - 30%**
- If "Previous_Provider" or "PREVIOUS_PROVIDER_ID" equals the clinician's ID: EXACTLY 30 points
- Otherwise: EXACTLY 0 points

**2. PATIENT SPECIFIC CONDITIONS - 25% (5% each condition, max 5 points per condition)**
Check for these conditions in patient data (PatientProfile, Service_Description, etc.):
- Recent surgery: EXACTLY 5 points (no more, no less) if mentioned
- Recently discharged from hospitalization: EXACTLY 5 points if mentioned  
- Recent home health discharge: EXACTLY 5 points if mentioned
- Recent fall: EXACTLY 5 points if mentioned
- Recently discharged from same discipline: EXACTLY 5 points if mentioned

IMPORTANT: Admission date alone is NOT a condition worth points. Only the specific conditions above get points.
Each condition is worth EXACTLY 5 points - not 10, not 55, just 5 points.
Never include "Admission Date" as a scoring reason in explanations - it earns 0 points.

Total possible across ALL conditions: max 25 points

**3. DISTANCE - 20%**
Distance calculation rules:
- If clinician HAS active cases AND patient is within 5 miles of any active case: Use distance between patient and nearest active case
- Otherwise: Use direct distance between clinician and patient

Scoring (STRICTLY FOLLOW THESE EXACT POINTS):
- 0-2 miles: EXACTLY 20 points
- 2-5 miles: EXACTLY 17 points  
- 5-10 miles: EXACTLY 15 points
- 10-20 miles: EXACTLY 10 points
- 20-40 miles: EXACTLY 7 points
- 40+ miles: EXACTLY 3 points

IMPORTANT: For distances over 40 miles (like 60 miles), score MUST be EXACTLY 3 points, and explanation should accurately reflect "Distance > 40 miles (3)". 
Use the value from "Primary_Distance" field for determining distance points.


**4. PATIENT PROFILE AND CLINICIAN PROFILE MATCHING - 15%**
This is the most critical and nuanced scoring category. Your goal is to act as an experienced medical professional to identify connections between the patient's stated needs and the clinician's expertise, even if the terms are not identical.

**Instructions:**
1.  **Analyze Patient Data:** Thoroughly examine all text fields in the patient's record, including `Service_Description`, `PatientProfile`, `Medical_History`, `Diagnosis`, and any other notes. Look for keywords related to diseases, conditions, symptoms, or required treatments.
2.  **Analyze Clinician Specialties:** Review the clinician's `Professional_Specialties` list.
3.  **Check if data exists:** If PatientProfile is empty or null AND no other medical information is available, you MUST score EXACTLY 0 points and note "No profile data (0)" in the explanation.
4.  **Connect Concepts:** Use your medical knowledge to connect the patient's condition to the clinician's specialties. The connection does not need to be a direct keyword match.
    -   **Example:** If the patient has "myeloma" (a type of cancer), and the clinician has "Oncologic Clinical Specialist", this is a **PERFECT match (15 points)**.
    -   **Example:** If a patient has "difficulty walking after a stroke" and the clinician is a "Neurologic Clinical Specialist", this is a **PERFECT match (15 points)**.
    -   **Example:** If a patient has "post-operative weakness" and the clinician is a "Certified Strength and Conditioning Specialist", this is a **GOOD match (10-12 points)**.
    -   **Example:** If a patient is elderly and has "frequent falls", and the clinician is a "Geriatric Clinical Specialist", this is a **PERFECT match (15 points)**.

**Scoring Guidelines:**
-   **Perfect Match (15 points):** The patient's primary condition falls directly within the scope of one of the clinician's core specialties (e.g., cancer -> oncology, stroke -> neurology, heart condition -> cardiovascular).
-   **Good Match (10-12 points):** The patient's condition is strongly related to a clinician's specialty, or a secondary condition matches a specialty (e.g., general deconditioning -> strength and conditioning specialist).
-   **Partial Match (5-8 points):** There is a plausible but not direct link between the patient's needs and the clinician's skills.
-   **No Match (0-3 points):** No discernible connection between the patient's condition and the clinician's listed specialties.
-   **No Data (0 points):** If patient profile is empty/null and no other medical info exists.

You have the expertise to make these connections. Trust your judgment to find the best clinical fit.

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

For patient and clinician profile matching , consider any of the subspaciality mentioned above can match the patient condition(his/her illness) and clinician spaciality. Use your advanced medical knowledge to find the best possible match between the patient's condition and the clinician's specialties, even if the keywords are not an exact match.


  === ADDITIONAL GUIDANCE ===
1. The fact that a clinician has active cases is NOT a scoring factor. It only determines WHICH distance to use (active case vs clinician distance).
2. When explaining the Match_Score, only include the factors that contributed points.
3. For distances over 40 miles (like 60 miles), you should clearly state "Distance > 40 miles (3 points)".
4. If PatientProfile is empty, state "No patient profile data (0 points)".
5. NEVER double count any factor. Previous Provider Match is EXACTLY 30 points TOTAL - never 60 points.
6. Admission Date is NEVER a scoring factor itself - it is worth 0 points. "Admission Date" should NEVER appear as a reason in scoring explanations.
7. Recent surgery is worth EXACTLY 5 points, not 55 points.
8. Discipline match (PT) is NOT a separate scoring factor and gets 0 points. Do not include discipline in the reason.
9. "Has Admission" is NOT a scoring factor and gets 0 points. Do not include in the reason.
10. NEVER include "Clinician Has Active Cases" in the explanation - this is not a scoring factor.

  === MATCH SCORE VERIFICATION ===
CRITICAL: The Match_Score MUST EXACTLY equal the sum of all points listed in the "Reason" field.
Verify your calculations - the sum of all point values in parentheses MUST equal the Match_Score.
For example: If you list "Previous provider (30), Distance 0-2 miles (20), specialty match (15), open case (10)" = Match_Score MUST be 75.
If a patient has Distance > 40 miles, the score MUST include only 3 points for distance.
If a patient has no profile data, the score MUST include 0 points for profile match.

  === SCORING EXAMPLES ===
Example 1: Previous provider match (30) + all conditions (25) + close distance (20) + perfect specialty match (15) + open case (10) = 100 points
Example 2: No previous provider (0) + 2 conditions (10) + medium distance (10) + good specialty match (10) + pending case (10) = 40 points
Example 3: Previous provider (30) + no conditions (0) + distance > 40 miles (3) + no patient profile (0) + pending case (10) = 43 points
Example 4: Previous provider (30, NOT 60) + recent surgery condition (5, NOT 55) + distance (10) + no profile (0) + pending (10) = 55 points
Example 5: Previous provider (30) + distance 0-2 miles (20) + good profile match (10) + open case (10) = 70 points (Admission Date is NEVER counted as points)
Example 6: Previous provider (30) + Distance > 40 miles (3) + No profile data (0) + pending case (10) = 43 points
Example 7: No previous provider (0) + Distance > 40 miles (3) + No profile data (0) + pending case (10) = 13 points

CRITICAL: Do NOT count Previous Provider as 30 points twice. Do NOT assign ANY points for admission date itself.
Admission date merely indicates WHEN the patient was admitted, it does not itself earn points.
If the patient has had recent surgery (admission date may indicate this), that is worth EXACTLY 5 points, not 55.
Never include "Admission Date", "Has Admission", "Clinician Has Active Cases", or "Discipline PT" in the reason text.
 
=== FINAL VERIFICATION ===
1. Check that your Match_Score exactly equals the sum of all points in your explanation
2. For example: "Previous provider (30), Distance > 40 miles (3), pending case (10), no profile (0)" = 43 points total
3. Verify each point value is coming from the correct category (distance > 40 miles = 3 points, not more)
4. Remove any 0-point items from the reason (like "Has Admission (0)" or "Discipline PT (0)")

=== OUTPUT FORMAT (STRICT) ===
You MUST return EXACTLY {len(limited)} patient scores in a JSON array.
Each patient in the input MUST appear in the output. Missing any patient will result in an error.

Expected format:
[
  {{"ID":"53416001","Match_Score":93,"Reason":"Same Previous provider, close proximity(If the Distance between patient and clinician or patient and active case is within range), recent discharge, specialty match, Hold Duration coming to end"}},
  {{"ID":"53416015","Match_Score":65,"Reason":"No prev provider, Recent fall, medium distance, Recent Hospital discharge, pending Assignment"}}
]

CRITICAL: You must score ALL {len(limited)} patients provided above. 
ONLY return the JSON array. No markdown, no backticks, no extra commentary.
Calculate scores precisely using the percentage weights above.
"""
    return prompt

def fast_ai_score(candidates: List[Dict[str,Any]],
                  clinician_info: Dict[str,Any],
                  active_case_ids_resolved: List[str]) -> List[Dict[str,Any]]:
    """Fast AI scoring with actual Gemini AI for top candidates"""
    
    print(f"🚀 Using FAST AI scoring for {len(candidates)} candidates")
    start_time = time.time()
    
    try:
        # Step 1: Pre-filter to top 50 candidates using quick rule-based scoring
        candidates_with_scores = []
        
        for candidate in candidates:
            # Quick rule-based pre-scoring
            base_score = 50
            
            # Previous provider bonus (30 points)
            if candidate.get("Is_Previous_Provider_Match", False):
                base_score += 30
            
            # Distance bonus (25 points max)
            distance = candidate.get("Primary_Distance") or candidate.get("DISTANCE_FROM_ACTIVE_CASE")
            if distance and str(distance).replace('.', '').replace('-', '').isdigit():
                dist_val = float(distance)
                if dist_val <= 2:
                    base_score += 25
                elif dist_val <= 5:
                    base_score += 20
                elif dist_val <= 10:
                    base_score += 15
                else:
                    base_score += 5
            
            # Status bonus (10 points)
            status = str(candidate.get("CASE_STATUS", "")).lower()
            if "pending" in status or "open" in status:
                base_score += 10
            
            # Add initial score directly to candidate (no pre-score)
            candidate["Match_Score"] = base_score
            candidates_with_scores.append(candidate)
        
        # Process ALL candidates with AI scoring (no pre-filtering or limits)
        print(f"Processing ALL {len(candidates_with_scores)} candidates with AI scoring (no pre-score filtering)")
        ai_scored = ai_score(candidates_with_scores, clinician_info, active_case_ids_resolved)
        
        # No remaining candidates since we process all with AI
        remaining_candidates = []
        for candidate in remaining_candidates:
            # Enhanced reasoning similar to AI but rule-based
            reason_bits = []
            
            if candidate.get("Is_Previous_Provider_Match", False):
                reason_bits.append("previous provider match")
            
            # Distance reasoning
            if candidate.get("DISTANCE_FROM_ACTIVE_CASE") is not None:
                active_case_name = candidate.get("active_case_name", "active case") 
                distance_info = f"near {active_case_name} (~{candidate['DISTANCE_FROM_ACTIVE_CASE']}mi)"
                reason_bits.append(distance_info)
            elif candidate.get("Primary_Distance") is not None:
                distance_type = candidate.get("Distance_Type_Used", "distance")
                distance_info = f"{distance_type.replace('_', ' ').lower()} ~{candidate['Primary_Distance']}mi"
                reason_bits.append(distance_info)
            
            reason_bits.append(f"discipline {candidate.get('DISCIPLINE', 'PT')}")
            
            if candidate.get("Clinician_Has_Active_Cases"):
                reason_bits.append("clinician has active cases")
            
            # Match_Score already set above, just add reason
            candidate["Reason"] = ", ".join(reason_bits)[:160]
        
        # Combine AI-scored and rule-based candidates (all candidates processed by AI now)
        all_scored = ai_scored + remaining_candidates
        
        elapsed = time.time() - start_time
        print(f"⏱️ [FAST SCORING] Completed in {elapsed:.2f} seconds")
        
        return all_scored
        
    except Exception as e:
        print(f"❌ Fast AI scoring failed: {str(e)}")
        # Fallback to enhanced reasoning for all candidates
        for candidate in candidates:
            reason_bits = []
            
            if candidate.get("Is_Previous_Provider_Match", False):
                reason_bits.append("previous provider match")
            
            # Use our new helper function for distance explanation
            distance_explanation = explain_distance_calculation(candidate)
            reason_bits.append(distance_explanation)
            
            reason_bits.append(f"discipline {candidate.get('DISCIPLINE', 'PT')}")
            
            candidate["Match_Score"] = 50
            candidate["Reason"] = ", ".join(reason_bits)[:160]
        
        elapsed = time.time() - start_time
        print(f"⏱️ [FAST SCORING] Completed with fallback in {elapsed:.2f} seconds")
        return candidates

def _generate_batch_cache_key(batch: List[Dict[str, Any]], clinician_id: str) -> str:
    """Generate a cache key for a batch of candidates + clinician"""
    # Create a unique hash based on candidate IDs, clinician ID and minimal data
    key_elements = [f"{clinician_id}"]
    
    for candidate in batch:
        patient_id = candidate.get("ID", "")
        primary_distance = candidate.get("Primary_Distance", "")
        is_previous = "1" if candidate.get("Is_Previous_Provider_Match") else "0"
        case_status = candidate.get("CASE_STATUS", "")[:10]  # First 10 chars of status
        
        # Only use the key fields that affect scoring
        key_elements.append(f"{patient_id}|{primary_distance}|{is_previous}|{case_status}")
    
    # Sort to ensure consistent ordering
    key_elements.sort()
    
    # Create a hash of this data for the cache key
    key_string = ":".join(key_elements)
    return hashlib.md5(key_string.encode()).hexdigest()

def process_ai_recommendations(filtered_candidates: List[Dict[str,Any]], 
                            clinician_info: Dict[str,Any],
                            batch_size: int = None,
                            timeout_seconds: int = None) -> List[Dict[str,Any]]:
    """Process recommendations in batches using Gemini AI API with fallback for errors.
    Returns scored candidates sorted by score descending."""
    
    # Use global configuration if not specified
    batch_size = batch_size or AI_BATCH_SIZE
    batch_timeout = timeout_seconds or AI_TIMEOUT
    
    # Calculate an optimal batch size based on number of candidates
    if len(filtered_candidates) > 100:
        # Use smaller batches for large datasets
        adjusted_batch_size = min(batch_size, 8)
        print(f"[OPTIMIZE] Adjusting batch size to {adjusted_batch_size} for {len(filtered_candidates)} candidates")
        batch_size = adjusted_batch_size
    
    print(f"[INFO] Processing {len(filtered_candidates)} candidates with batch size {batch_size}")
    
    # Track overall processing time
    process_start_time = time.time()
    global_timeout = AI_GLOBAL_TIMEOUT
    
    # Track the active case IDs to ensure we don't duplicate candidates
    active_case_ids_resolved = []
    
    # Create batches for AI processing
    ai_batches = []
    current_batch = []
    
    for candidate in filtered_candidates:
        case_id = candidate.get("CASE_ID")
        
        # Skip candidates that are already in the active case ids list
        if case_id in active_case_ids_resolved:
            continue
            
        active_case_ids_resolved.append(case_id)
        current_batch.append(candidate)
        
        if len(current_batch) >= batch_size:
            ai_batches.append(current_batch)
            current_batch = []
    
    # Add any remaining candidates
    if current_batch:
        ai_batches.append(current_batch)
    
    # Process each batch with AI and collect results
    all_ai_results = []
    
    for i, batch in enumerate(ai_batches):
        # Check if we're approaching the global timeout
        elapsed_time = time.time() - process_start_time
        time_remaining = max(5, global_timeout - elapsed_time)  # Ensure at least 5 seconds
        
        # If we're close to the global timeout, use fallback for remaining batches
        if elapsed_time > global_timeout * 0.8:  # 80% of global timeout used
            print(f"[TIMEOUT] Approaching global timeout after {elapsed_time:.1f} seconds, using fallback for remaining batches")
            # Process all remaining batches with fallback
            for remaining_batch in ai_batches[i:]:
                fallback_results = _fallback_rule_based_scoring(remaining_batch, clinician_info)
                all_ai_results.extend(fallback_results)
            break
            
        print(f"[INFO] Processing batch {i+1}/{len(ai_batches)} with {len(batch)} candidates (time remaining: {time_remaining:.1f}s)")
        
        # Use a dynamic timeout that gets shorter as we process more batches
        # This ensures we don't hit the global timeout
        dynamic_timeout = min(batch_timeout, time_remaining)
        
        try:
            # Process the batch with AI and a dynamic timeout
            batch_results = _process_ai_batch_with_timeout(
                batch, 
                clinician_info, 
                active_case_ids_resolved,
                dynamic_timeout
            )
            all_ai_results.extend(batch_results)
        except Exception as e:
            print(f"[ERROR] Batch {i+1} failed: {str(e)}")
            # Fallback to rule-based scoring for this batch
            fallback_results = _fallback_rule_based_scoring(batch, clinician_info)
            all_ai_results.extend(fallback_results)
    
    # Sort by score descending
    all_ai_results.sort(key=lambda x: x.get("Match_Score", 0), reverse=True)
    
    total_time = time.time() - process_start_time
    print(f"[INFO] Completed AI recommendations in {total_time:.2f} seconds")
    
    return all_ai_results

def ai_score(candidates: List[Dict[str,Any]],
             clinician_info: Dict[str,Any],
             active_case_ids_resolved: List[str]) -> List[Dict[str,Any]]:
    """Send everything to Gemini for full scoring with caching and parallel processing"""
    
    ai_start_time = time.time()
    print(f"⏱️ [TIMING] Starting AI scoring for {len(candidates)} candidates")
    
    if not init_gemini():
        raise RuntimeError("Gemini initialization failed")

    # Get the clinician ID for cache keys
    clinician_id = str(clinician_info.get("FOX_PROVIDER_ID") or clinician_info.get("ID", ""))
    
    # Process in optimally sized batches with caching
    batch_size = AI_BATCH_SIZE  # Increased to 10 patients per batch for better efficiency
    all_scored = []
    cache_hits = 0
    
    # For smaller sets, process sequentially to avoid overhead
    if len(candidates) <= batch_size * 3:
        print(f"[STRATEGY] Using sequential processing for {len(candidates)} candidates")
        
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i + batch_size]
            batch_start = time.time()
            print(f"[BATCH] Processing batch {i//batch_size + 1}: patients {i+1}-{min(i+batch_size, len(candidates))}")
            
            # Generate cache key for this batch
            cache_key = _generate_batch_cache_key(batch, clinician_id)
            
            # Check if we have a cached result
            if cache_key in ai_response_cache:
                print(f"[CACHE] ✓ Cache hit for batch {i//batch_size + 1}")
                batch_scored = ai_response_cache[cache_key]
                cache_hits += 1
            else:
                try:
                    # Set timeout for batch processing
                    batch_scored = _process_ai_batch_with_timeout(batch, clinician_info, active_case_ids_resolved, AI_TIMEOUT)
                    # Cache the result
                    ai_response_cache[cache_key] = batch_scored
                    
                    # Limit cache size
                    if len(ai_response_cache) > AI_CACHE_SIZE:
                        # Remove a random key to keep cache size in check
                        ai_response_cache.pop(list(ai_response_cache.keys())[0])
                except Exception as e:
                    batch_time = time.time() - batch_start
                    print(f"⏱️ [TIMING] Failed batch took: {batch_time:.2f} seconds")
                    print(f"[ERROR] Batch {i//batch_size + 1} failed: {str(e)}")
                    # Use rule-based scoring as fallback
                    batch_scored = _fallback_rule_based_scoring(batch, clinician_info)
            
            all_scored.extend(batch_scored)
            batch_time = time.time() - batch_start
            print(f"⏱️ [TIMING] Batch {i//batch_size + 1} took: {batch_time:.2f} seconds")
            print(f"✅ Successfully scored {len(batch_scored)} patients in this batch")
    else:
        # For larger sets, use parallel processing with ThreadPoolExecutor
        print(f"[STRATEGY] Using parallel processing with {AI_MAX_WORKERS} workers for {len(candidates)} candidates")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=AI_MAX_WORKERS) as executor:
            # Prepare batches and submit to executor
            futures = []
            
            for i in range(0, len(candidates), batch_size):
                batch = candidates[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                # Generate cache key for this batch
                cache_key = _generate_batch_cache_key(batch, clinician_id)
                
                # Check if we have a cached result first
                if cache_key in ai_response_cache:
                    print(f"[CACHE] ✓ Cache hit for batch {batch_num}")
                    all_scored.extend(ai_response_cache[cache_key])
                    cache_hits += 1
                else:
                    # Submit to thread pool
                    future = executor.submit(
                        _process_batch_with_caching, 
                        batch, 
                        clinician_info, 
                        active_case_ids_resolved,
                        batch_num,
                        cache_key
                    )
                    futures.append(future)
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    batch_scored = future.result()
                    all_scored.extend(batch_scored)
                except Exception as e:
                    print(f"[ERROR] Batch processing failed: {str(e)}")
    
    total_ai_time = time.time() - ai_start_time
    print(f"⏱️ [TIMING] Total AI scoring completed in: {total_ai_time:.2f} seconds")
    print(f"🎉 Total AI scoring complete: {len(all_scored)} patients processed, with {cache_hits} cache hits")
    return all_scored

def _process_batch_with_caching(batch, clinician_info, active_case_ids_resolved, batch_num, cache_key):
    """Process a batch and store in cache - used by thread pool"""
    batch_start = time.time()
    print(f"[BATCH] Processing batch {batch_num}: {len(batch)} patients in parallel thread")
    
    try:
        # Process with timeout
        batch_scored = _process_ai_batch_with_timeout(batch, clinician_info, active_case_ids_resolved, AI_TIMEOUT)
        
        # Cache the result
        ai_response_cache[cache_key] = batch_scored
        
        # Limit cache size 
        if len(ai_response_cache) > AI_CACHE_SIZE:
            # Remove a random key to keep cache size in check
            ai_response_cache.pop(list(ai_response_cache.keys())[0])
            
        batch_time = time.time() - batch_start
        print(f"⏱️ [TIMING] Parallel batch {batch_num} took: {batch_time:.2f} seconds")
        print(f"✅ Successfully scored {len(batch_scored)} patients in batch {batch_num}")
        
        return batch_scored
        
    except Exception as e:
        batch_time = time.time() - batch_start
        print(f"⏱️ [TIMING] Failed parallel batch {batch_num} took: {batch_time:.2f} seconds")
        print(f"[ERROR] Batch {batch_num} failed: {str(e)}")
        
        # Use rule-based scoring as fallback
        return _fallback_rule_based_scoring(batch, clinician_info)

def _process_ai_batch_with_timeout(candidates: List[Dict[str,Any]],
                           clinician_info: Dict[str,Any],
                           active_case_ids_resolved: List[str],
                           timeout_seconds: int = 60) -> List[Dict[str,Any]]:
    """Process a batch with a timeout to prevent hanging"""
    
    result = None
    exception = None
    
    def process_batch():
        nonlocal result, exception
        try:
            result = _process_ai_batch(candidates, clinician_info, active_case_ids_resolved)
        except Exception as e:
            exception = e
    
    # Create and start thread
    thread = threading.Thread(target=process_batch)
    thread.daemon = True  # Daemon thread will be terminated when main thread exits
    thread.start()
    
    # Wait for thread to complete with timeout
    thread.join(timeout_seconds)
    
    # Check if thread is still alive (timeout occurred)
    if thread.is_alive():
        print(f"[TIMEOUT] AI processing timed out after {timeout_seconds} seconds")
        # We can't forcibly terminate the thread in Python, but we can proceed without waiting
        # for it to complete by returning a fallback result
        return _fallback_rule_based_scoring(candidates, clinician_info)
    
    # If there was an exception, reraise it
    if exception:
        print(f"[ERROR] Exception in AI processing: {str(exception)}")
        raise exception
    
    # Return the result if available
    if result:
        return result
    else:
        print("[ERROR] No result from AI processing thread")
        return _fallback_rule_based_scoring(candidates, clinician_info)

def _fallback_rule_based_scoring(candidates: List[Dict[str,Any]], clinician_info: Dict[str,Any]) -> List[Dict[str,Any]]:
    """Apply rule-based scoring as fallback when AI fails"""
    print("[FALLBACK] Using rule-based scoring instead of AI")
    
    scored_candidates = []
    
    for candidate in candidates:
        enriched = candidate.copy()
        score = 0
        reason_parts = []
        
        # Previous provider match (30 points)
        if candidate.get("Is_Previous_Provider_Match", False):
            score += 30
            reason_parts.append("Previous provider match (30)")
        
        # Distance scoring (20 points max)
        # Use the appropriate distance based on distance type
        distance_type = candidate.get("Distance_Type_Used", "")
        
        if "Active_Case" in distance_type:
            distance_field = "Distance_to_Nearest_Active_Case"
            distance_desc = "Active case distance"
        else:
            distance_field = "Distance_to_Clinician"
            distance_desc = "Clinician distance"
            
        # Get the distance using the appropriate field
        distance = float(candidate.get(distance_field, 9999) or 9999)
        
        # Assign points based on distance
        if distance <= 2:
            score += 20
            reason_parts.append(f"{distance_desc} 0-2 miles (20)")
        elif distance <= 5:
            score += 17
            reason_parts.append(f"{distance_desc} 2-5 miles (17)")
        elif distance <= 10:
            score += 15
            reason_parts.append(f"{distance_desc} 5-10 miles (15)")
        elif distance <= 20:
            score += 10
            reason_parts.append(f"{distance_desc} 10-20 miles (10)")
        elif distance <= 40:
            score += 7
            reason_parts.append(f"{distance_desc} 20-40 miles (7)")
        else:
            score += 3
            reason_parts.append(f"{distance_desc} > 40 miles (3)")
            
        # Case Status (10 points)
        status = str(candidate.get("CASE_STATUS", "")).lower()
        if "open" in status or "pending" in status:
            score += 10
            reason_parts.append("Case Status pending/open (10)")
        else:
            score += 5
            reason_parts.append("Case Status other (5)")
        
        # No patient profile info (0 points)
        profile = candidate.get("PatientProfile", [])
        if not profile:
            reason_parts.append("No patient profile (0)")
        
        # Assemble final result
        enriched["Match_Score"] = score
        
        # Convert detailed reason parts to simplified array format
        simplified_reasons = []
        if any("Previous provider match" in part for part in reason_parts):
            simplified_reasons.append("Previous Provider Match")
        if any("miles" in part for part in reason_parts):
            simplified_reasons.append("Close Proximity")
        if any("pending" in part.lower() or "open" in part.lower() for part in reason_parts):
            simplified_reasons.append("Pending Assignment")
            
            # Use simplified reasons if available, otherwise use the originals
            if simplified_reasons:
                enriched["Reason"] = simplified_reasons
            else:
                # Extract just the factor names without point values
                clean_parts = []
                for part in reason_parts:
                    if "(" in part and ")" in part:
                        clean_parts.append(part.split("(")[0].strip())
                    else:
                        clean_parts.append(part)
                enriched["Reason"] = clean_parts
                
            # Remove ReasonText field if it exists
            if "ReasonText" in enriched:
                del enriched["ReasonText"]        # Add a flag indicating this was scored by fallback method
        enriched["Scoring_Method"] = "rule_based_fallback"
        
        scored_candidates.append(enriched)
    
    return scored_candidates

def _process_ai_batch(candidates: List[Dict[str,Any]],
                     clinician_info: Dict[str,Any],
                     active_case_ids_resolved: List[str]) -> List[Dict[str,Any]]:
    """Process a small batch of candidates through Gemini AI with retry logic"""
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            payload_prompt = _build_ai_payload(clinician_info, candidates, [])
            model = genai.GenerativeModel(GEMINI_MODEL)
            
            resp = model.generate_content(
                payload_prompt,
                generation_config={
                    "temperature": AI_TEMPERATURE, 
                    "max_output_tokens": AI_MAX_OUTPUT_TOKENS,
                    "top_p": 0.8,
                    "top_k": 40
                }
            )
            
            raw = _extract_text_from_gemini_response(resp)
            print(f"[DEBUG] Attempt {retry_count + 1}: Gemini raw response length: {len(raw) if raw else 0} characters")
            
            if not raw:
                raise ValueError("Empty response from Gemini API")
                
            cleaned = _clean_ai_json(raw)
            print(f"🔍 Cleaned JSON length: {len(cleaned) if cleaned else 0} characters")
            
            if not cleaned:
                print(f"Warning: Could not extract valid JSON from Gemini response. Raw response: {raw[:500]}...")
                raise ValueError(f"Failed to extract JSON from Gemini response: {raw[:100]}...")
                
            try:
                data = json.loads(cleaned)
                print(f"🔍 Parsed JSON: Found {len(data) if isinstance(data, list) else 'not a list'} items")
            except json.JSONDecodeError as e:
                print(f"Warning: JSON parsing error. Attempted to parse: {cleaned[:200]}...")
                raise ValueError(f"JSON parsing error: {str(e)}. Attempted to parse: {cleaned[:100]}...")
                
            # Validate the response structure
            if not isinstance(data, list):
                raise ValueError(f"Expected a JSON array from Gemini, got: {type(data).__name__}")
                
            # Check if we got scores for all patients
            scored_patient_ids = {str(item.get("ID", "")) for item in data if isinstance(item, dict) and item.get("ID")}
            expected_patient_ids = {str(c["ID"]) for c in candidates}
            missing_ids = expected_patient_ids - scored_patient_ids
            
            if missing_ids:
                print(f"⚠️ Attempt {retry_count + 1}: AI missed {len(missing_ids)} patients: {list(missing_ids)[:5]}")
                if retry_count < max_retries - 1:
                    retry_count += 1
                    print(f"🔄 Retrying batch processing (attempt {retry_count + 1}/{max_retries})...")
                    continue
            
            # Process the results
            by_id = {str(c["ID"]): c for c in candidates}
            scored_ids = set()
            out: List[Dict[str,Any]] = []
            
            # First, process all candidates that Gemini scored
            for item in data:
                if not isinstance(item, dict):
                    continue
                    
                pid = str(item.get("ID", ""))
                if not pid or pid not in by_id:
                    continue
                    
                enriched = by_id[pid].copy()
                
                # Extract the reason into an array format and validate the score
                reason_str = str(item.get("Reason", ""))
                
                # Calculate the actual score from the reason string
                actual_score = 0
                reason_factors = []
                
                # Extract scoring factors
                for match in re.finditer(r'([^,\(\)]+)\s*\((\d+)\)', reason_str):
                    factor = match.group(1).strip()
                    points = int(match.group(2))
                    
                    # Only include factors with non-zero points
                    if points > 0:
                        reason_factors.append(factor)
                        actual_score += points
                
                # Cap the score at 100
                actual_score = min(actual_score, 100)
                
                # Use our calculated score instead of the AI's score
                enriched["Match_Score"] = actual_score
                
                # Format the reason as a cleaner array of factors (no points)
                clean_factors = []
                
                # ALWAYS include Previous Provider Match if applicable
                if enriched.get("Is_Previous_Provider_Match", False):
                    clean_factors.append("Previous Provider Match")
                    # Ensure score includes 30 points for previous provider if not already counted
                    if not any("previous provider" in f.lower() for f in reason_factors):
                        actual_score += 30
                        enriched["Match_Score"] = min(actual_score, 100)
                
                # Include distance explanation based on distance source
                distance_source = enriched.get("Distance_Source", "")
                primary_distance = enriched.get("Primary_Distance")
                
                if distance_source == "active_case":
                    clean_factors.append("Close to Active Case")
                elif primary_distance is not None and primary_distance <= 20:
                    clean_factors.append("Close Proximity to Clinician")
                
                # Include case status
                case_status = str(enriched.get("Case_Status", "")).lower()
                if "pending" in case_status or "open" in case_status:
                    clean_factors.append("Pending Assignment")
                
                # If we don't have any factors yet, extract from reason_factors
                if not clean_factors:
                    if any("distance" in f.lower() for f in reason_factors):
                        clean_factors.append("Distance Factor")
                    if any("pending" in f.lower() or "open" in f.lower() for f in reason_factors):
                        clean_factors.append("Status Factor")
                
                # Assign our clean factors
                enriched["Reason"] = clean_factors if clean_factors else ["Score factors unavailable"]
                
                # Remove ReasonText field if it exists
                if "ReasonText" in enriched:
                    del enriched["ReasonText"]
                
                out.append(enriched)
                scored_ids.add(pid)
            
            # Add any candidates that still weren't scored (should be minimal now)
            for candidate in candidates:
                candidate_id = str(candidate["ID"])
                if candidate_id not in scored_ids:
                    print(f"⚠️ Final fallback for patient {candidate_id}")
                    enriched = candidate.copy()
                    enriched["Match_Score"] = 0
                    enriched["Reason"] = f"AI scoring incomplete after {retry_count + 1} attempts"
                    out.append(enriched)
                    
            print(f"✅ Batch complete: {len(scored_ids)} AI-scored + {len(out) - len(scored_ids)} fallback = {len(out)} total")
            return out
            
        except Exception as e:
            print(f"[ERROR] Attempt {retry_count + 1} failed: {str(e)}")
            retry_count += 1
            
            if retry_count >= max_retries:
                print(f"💥 All {max_retries} attempts failed. Using fallback scores.")
                # Return all patients with fallback scores
                out = []
                for candidate in candidates:
                    enriched = candidate.copy()
                    enriched["Match_Score"] = 0
                    enriched["Reason"] = f"AI batch processing failed after {max_retries} attempts: {str(e)[:100]}"
                    out.append(enriched)
                return out

# ====================== MAIN ENTRY ======================
def recommend_patients(clinician_id: str, radius: float = 5.0, top_k: int = 5) -> Dict[str, Any]:
    """
    100% AI-driven ranking (Gemini). Fallback to distance-only if AI fails.
    Matches clinicians with patients based on sub-specialty and patient needs.
    Filters results to only include patients within the specified radius.
    Never raises HTTP exceptions here; returns {"error": "..."} on issues.
    
    Args:
        clinician_id: The ID of the clinician for whom to recommend patients
        radius: Maximum distance in miles to include patients (default: 5.0)
        top_k: The number of recommended patients to return (default: 5)
    """
    t0 = time.time()
    errors: List[str] = []
    ai_used = False
    ai_error = None

    # ---------- Load data using new API approach ----------
    cid = str(clinician_id).strip()
    try:
        print("Loading clinician and patient data from APIs...")
        clinician_info, patients = load_data(provider_id=cid, radius=radius)
        print(f"Loaded clinician: {clinician_info.get('Name', 'Unknown')}")
        print(f"Loaded {len(patients)} patients from proximity cases")
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return {"error": f"Failed to load data: {e}"}

    if patients.empty:
        return {"error": "No patients found in proximity"}

    print(f"Found clinician: {clinician_info.get('Name', 'Unknown')} ({clinician_info.get('Discipline', 'Unknown')})")

    # Normalize clinician discipline - check multiple possible field names
    disc_raw = (clinician_info.get("DiciplineCode") or     # API has typo "DiciplineCode"
               clinician_info.get("Discipline") or 
               clinician_info.get("SPECIALTY") or 
               clinician_info.get("DISCIPLINE") or "")
    normalized = normalize_clinician_discipline(disc_raw)
    print(f"Clinician discipline: {disc_raw} -> normalized: {normalized}")
    
    if normalized not in {"PT","OT","ST"}:
        return {"error": f"Unsupported clinician discipline '{disc_raw}' (normalized='{normalized}')"}

    # Get clinician subspecialty
    subspecialty = clinician_info.get("Subspecialty") or ""
    print(f"Clinician subspecialty: {subspecialty}")

    # Clinician coordinates from API data with facility priority
    clinician_coords = None
    
    # PRIORITY 1: Try facility coordinates first (if available)
    facility_lat = clinician_info.get("Facility_Lat") or clinician_info.get("facility_latitude") 
    facility_lon = clinician_info.get("Facility_Long") or clinician_info.get("facility_longitude")
    print(f"Checking facility coordinates first: LAT={facility_lat}, LON={facility_lon}")
    
    if facility_lat and facility_lon:
        clinician_coords = _coords_from_cols(facility_lat, facility_lon)
        if clinician_coords:
            print(f"✅ Using facility coordinates: {clinician_coords}")
    
    # PRIORITY 2: Fall back to individual clinician coordinates if facility not available
    if not clinician_coords:
        lat = clinician_info.get("Latitude")
        lon = clinician_info.get("Longitude") 
        print(f"Falling back to clinician coordinates: LAT={lat}, LON={lon}")
        clinician_coords = _coords_from_cols(lat, lon)
        if clinician_coords:
            print(f"✅ Using clinician coordinates: {clinician_coords}")
        
    print(f"[LOCATION] Final clinician coordinates: {clinician_coords}")
    
    if not clinician_coords:
        print("[WARNING] Clinician has no valid coordinates!")
        print("   This will cause distance calculations to fail")
        print("   Recommendations will be based on discipline matching only")
        print(f"   Using large radius (>=500 miles) will include patients despite missing coordinates")

    # ---------- Active case context ----------
    # Since we're using API data, active cases count is in ACTIVE_CASES field
    active_cases_count = clinician_info.get("ACTIVE_CASES", 0)
    
    # Safely convert active_cases_count to integer, handling empty strings and other non-numeric values
    try:
        # Handle empty strings, None, and convert to int
        active_cases_count_int = int(active_cases_count) if active_cases_count and str(active_cases_count).strip() else 0
        has_active_cases = active_cases_count_int > 0
    except (ValueError, TypeError):
        print(f"Warning: Invalid active cases count value: '{active_cases_count}', defaulting to 0")
        active_cases_count_int = 0
        has_active_cases = False
    
    # Extract active case coordinates from proximity API response
    active_case_coords: List[Tuple[float, float]] = []
    active_case_ids_resolved: List[str] = []
    active_case_names_map: Dict[str, str] = {}  # Map of case_id -> name
    
    # We need to extract active case coordinates from the raw API response
    # This data is available in the proximity API response in ClinicianActiveCase objects
    print(f"Collecting active case coordinates for distance calculations...")
    
    # Process the dataframe to find active cases
    active_cases = patients[patients["is_active_case"] == True].copy() if "is_active_case" in patients.columns else pd.DataFrame()
    
    if not active_cases.empty:
        print(f"Found {len(active_cases)} active cases with coordinates in API response")
        for _, active_case in active_cases.iterrows():
            case_id = str(active_case.get("CASE_ID", ""))
            lat = active_case.get("CASE_LAT")
            lon = active_case.get("CASE_LON")
            
            # Create active case name using Last Name, First Name format
            first_name = active_case.get("First_Name", "")
            last_name = active_case.get("Last_Name", "")
            case_name = f"{last_name}, {first_name}".strip(" ,")
            if not case_name:
                case_name = f"Active Case {case_id}"
                
            # Store the name in the map
            active_case_names_map[case_id] = case_name
            
            if lat is not None and lon is not None:
                try:
                    coords = (float(lat), float(lon))
                    active_case_coords.append(coords)
                    active_case_ids_resolved.append(case_id)
                    print(f"Added active case {case_id} ({case_name}) with coordinates: {coords}")
                except (ValueError, TypeError):
                    pass
    
    # Look for active cases in each row's active case data
    for _, row in patients.iterrows():
        if "active_case_coords" in row and row["active_case_coords"] is not None:
            # This is a direct reference to coordinates stored during transform
            coords = row["active_case_coords"]
            case_id = str(row.get("active_case_id", "unknown"))
            
            # Get active case name if available
            active_case_name = row.get("active_case_name")
            if active_case_name:
                active_case_names_map[case_id] = active_case_name
                print(f"Added active case name mapping: {case_id} -> {active_case_name}")
            
            if coords and isinstance(coords, tuple) and len(coords) == 2:
                active_case_coords.append(coords)
                active_case_ids_resolved.append(case_id)
                print(f"Added active case {case_id} from row data with coords: {coords}")
    
    print(f"Collected {len(active_case_coords)} active case coordinates for distance calculations")

    # ---------- Filter candidates by discipline ----------
    matches = patients[patients["DISCIPLINE"] == normalized].copy()
    if matches.empty:
        return {"error": f"No patients found for discipline '{normalized}'"}
    
    # ---------- Filter out active cases ----------
    # Active cases are already assigned to the clinician, so exclude them from recommendations
    initial_count = len(matches)
    matches = matches[matches["is_active_case"] != True].copy()
    active_cases_filtered = initial_count - len(matches)
    if active_cases_filtered > 0:
        print(f"Filtered out {active_cases_filtered} active cases (already assigned to clinician)")
    
    if matches.empty:
        return {"error": f"No unassigned patients found for discipline '{normalized}' - all cases are already active"}

    # ---------- Build candidate feature rows ----------
    def nearest_active_distance(p_coord: Optional[Tuple[float, float]]) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        if not p_coord or not active_case_coords:
            return None, None, None
        best_d = None
        best_id = None
        best_name = None
        for idx, ac_coord in enumerate(active_case_coords):
            d = calculate_distance(p_coord, ac_coord)
            if d is None:
                continue
            if best_d is None or d < best_d:
                best_d = d
                best_id = active_case_ids_resolved[idx]
                # Look up name in the mapping
                if best_id in active_case_names_map:
                    best_name = active_case_names_map[best_id]
                
        return best_d, best_id, best_name

    # We've already determined if clinician has active cases at the top
    print(f"Clinician has {active_cases_count_int} active cases")
    
    candidates: List[Dict[str, Any]] = []
    for _, row in matches.iterrows():
        r = row.to_dict()
        # Coordinates
        p_coord = _coords_from_cols(r.get("CASE_LAT"), r.get("CASE_LON"))
        d_clin = calculate_distance(clinician_coords, p_coord) if clinician_coords and p_coord else None
        d_ac, near_ac_id, near_ac_name = nearest_active_distance(p_coord)

        # Implement priority distance calculation rules
        primary_distance = None
        distance_type = ""
        distance_source = ""
        
        # First, check if we have a valid active case distance AND the clinician has active cases
        if has_active_cases and d_ac is not None:
            # If we have valid active case distance, use that as primary
            primary_distance = d_ac
            distance_type = "Active_Case_Distance"
            distance_source = "active_case"
            print(f"Using active case distance: {d_ac:.1f} miles for patient {r.get('ID', '')}")
        else:
            # In all other cases (no active cases OR no valid active case distance), 
            # fall back to direct clinician distance
            primary_distance = d_clin
            distance_type = "Distance_to_Clinician"
            distance_source = "clinician"
            
            # Debug info for distance issues
            if has_active_cases and d_ac is None:
                print(f"WARNING: Patient {r.get('ID', '')} has no valid distance to active cases despite clinician having {active_cases_count_int} active cases")
                if len(active_case_coords) == 0:
                    print(f"  - No active case coordinates collected - missing active cases in API response?")
                elif p_coord is None:
                    print(f"  - Patient has no valid coordinates")
                else:
                    print(f"  - Patient coords: {p_coord}")
                    print(f"  - Active case coords: {active_case_coords[:3]} (showing up to 3)")
                    
            if primary_distance and primary_distance > 20:
                print(f"WARNING: Large distance {primary_distance:.1f} miles for patient {r.get('ID', '')}")

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
            "NAME": f"{str(r.get('Last_Name',''))}, {str(r.get('First_Name',''))}".strip(" ,"),
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
            "DISTANCE_FROM_ACTIVE_CASE": d_ac,  # Use calculated distance to active case
            "active_case_name": near_ac_name or r.get("active_case_name"),  # Use nearest active case name or row data
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

    # ---------- Filter out candidates with empty IDs (causes AI scoring issues) ----------
    initial_candidate_count = len(candidates)
    
    candidates = [c for c in candidates if c.get("ID") and str(c.get("ID")).strip()]
    filtered_count = initial_candidate_count - len(candidates)
    if filtered_count > 0:
        print(f"⚠️ Filtered out {filtered_count} candidates with missing IDs")

    # ---------- AI scoring ----------
    scored: List[Dict[str, Any]] = []
    ai_type = "Unknown"  # Track which AI method was used
    try:
        print("Attempting to use Gemini AI for scoring...")
        
        # Add timeout to prevent hanging
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("AI scoring timed out after 30 seconds")
        
        # Set up timeout for Windows (alternative approach)
        try:
            print(f"Starting AI scoring for {len(candidates)} candidates...")
            
            # Add timeout protection for AI scoring
            import threading
            import time as time_module
            
            ai_result = [None]  # Use list to allow modification from inner function
            ai_exception = [None]
            
            def ai_scoring_worker():
                try:
                    # Use full AI scoring for detailed reasoning when candidate count is manageable
                    if len(candidates) <= 100:  # Increased threshold to get better reasoning for more cases
                        # Use full AI scoring for detailed reasoning when candidate count is manageable
                        ai_result[0] = ai_score(candidates, clinician_info, active_case_ids_resolved)
                        print(f"Successfully scored {len(ai_result[0])} candidates using FULL Gemini AI")
                    else:
                        # Use fast AI scoring for large candidate sets
                        ai_result[0] = fast_ai_score(candidates, clinician_info, active_case_ids_resolved)
                        print(f"Successfully scored {len(ai_result[0])} candidates using FAST Gemini AI")
                except Exception as e:
                    ai_exception[0] = e
            
            # Start AI scoring in a separate thread with timeout
            ai_thread = threading.Thread(target=ai_scoring_worker)
            ai_thread.daemon = True
            ai_thread.start()
            
            # Wait for up to 180 seconds for AI scoring to complete (increased for more candidates)
            ai_thread.join(timeout=180)
            
            if ai_thread.is_alive():
                print("⚠️ AI scoring timed out after 180 seconds, using fallback...")
                raise TimeoutError("AI scoring timed out after 180 seconds")
            
            if ai_exception[0]:
                raise ai_exception[0]
                
            if ai_result[0]:
                scored = ai_result[0]
                ai_used = True
                # Determine AI type based on candidate count
                ai_type = "Full Gemini AI" if len(candidates) <= 100 else "Fast Gemini AI"
            else:
                raise RuntimeError("AI scoring returned no results")
        except Exception as ai_exception:
            print(f"AI scoring failed with exception: {str(ai_exception)}")
            raise ai_exception
            
    except Exception as e:
        ai_error = str(e)
        errors.append(f"AI scoring failed: {ai_error}")
        print(f"AI scoring failed: {ai_error}")
        print("🔄 Using enhanced fallback reasoning instead of basic scoring...")
        # Log detailed error for debugging
        import traceback
        traceback_str = traceback.format_exc()
        print(f"Detailed error traceback:\n{traceback_str}")
        
        # Create enhanced fallback reasoning using the same logic as fast_ai_score
        scored = []
        for candidate in candidates:
            enriched = candidate.copy()
            
            # Calculate a reasonable score
            base_score = 50
            if candidate.get("Is_Previous_Provider_Match", False):
                base_score += 30
            
            distance = candidate.get("DISTANCE_FROM_ACTIVE_CASE")
            if distance and str(distance).replace('.', '').isdigit():
                dist_val = float(distance)
                if dist_val <= 2:
                    base_score += 25
                elif dist_val <= 5:
                    base_score += 20
                elif dist_val <= 10:
                    base_score += 15
                else:
                    base_score += 5
            
            enriched["Match_Score"] = min(base_score, 100)
            
            # Build enhanced reasoning
            reason_bits = []
            if candidate.get("Is_Previous_Provider_Match", False):
                reason_bits.append("previous provider match")
            
            # Use our new helper function for distance explanation
            distance_explanation = explain_distance_calculation(candidate)
            reason_bits.append(distance_explanation)
            
            reason_bits.append(f"discipline {candidate.get('DISCIPLINE')}")
            reason_bits.append("AI fallback scoring")
            
            enriched["Reason"] = ", ".join(reason_bits)[:160]
            scored.append(enriched)

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
            # Use our new helper function for distance explanation
            distance_explanation = explain_distance_calculation(c)
            reason_bits.append(distance_explanation)
            
            reason_bits.append(f"discipline {c.get('DISCIPLINE')}")
            
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
    
    # ---------- No radius filtering - using distance for sorting only ----------
    print(f"📍 Proximity API already filtered patients by radius: {radius} miles")
    print(f"📊 Using distance information for sorting and priority only (no radius filtering)")
    
    # Process all candidates with distance information for logging/stats
    candidates_count = len(scored)
    with_active_case_distance = 0
    with_clinician_distance = 0
    no_distance_info = 0
    
    for candidate in scored:
        # Get distance measurements for logging
        distance_to_active_case = candidate.get("DISTANCE_FROM_ACTIVE_CASE")
        distance_to_clinician = candidate.get("Distance_to_Clinician")
        primary_distance = candidate.get("Primary_Distance")
        
        # Count distance types for logging
        if distance_to_active_case is not None:
            with_active_case_distance += 1
        if distance_to_clinician is not None:
            with_clinician_distance += 1
        if distance_to_active_case is None and distance_to_clinician is None and primary_distance is None:
            no_distance_info += 1
    
    # Log distance statistics
    print(f"📊 Distance information statistics:")
    print(f"   - Total candidates: {candidates_count}")
    print(f"   - With active case distance: {with_active_case_distance}")
    print(f"   - With clinician distance: {with_clinician_distance}")
    print(f"   - No distance information: {no_distance_info}")
    
    # Keep all candidates from the proximity API (already radius-filtered)
    filtered_by_radius = scored
    
    # Remove duplicate patients from recommendations
    deduplicated_candidates = deduplicate_recommendations(filtered_by_radius)
    filtered_by_radius = deduplicated_candidates
    
    # Take the top results from filtered candidates (already filtered by proximity API)
    out_recs = filtered_by_radius[:max(1, min(top_k, len(filtered_by_radius)))]

    # Ensure all recommendations have Reason as an array format
    for rec in out_recs:
        if "Reason" in rec:
            if isinstance(rec["Reason"], list):
                # Already in correct format, remove ReasonText if it exists
                if "ReasonText" in rec:
                    del rec["ReasonText"]
            else:
                # If Reason is still a string, convert it to array format
                reason_text = rec["Reason"]
                
                # Create simplified array version
                simplified = []
                if "Previous Provider" in reason_text or "previous provider" in reason_text:
                    simplified.append("Previous Provider Match")
                if "mile" in reason_text or "distance" in reason_text:
                    simplified.append("Close Proximity")
                if "pending" in reason_text.lower() or "open" in reason_text.lower():
                    simplified.append("Pending Assignment")
                    
                rec["Reason"] = simplified if simplified else ["Score factors unavailable"]
                
                # Remove ReasonText field
                if "ReasonText" in rec:
                    del rec["ReasonText"]
    
    result = {
        "search_params": {
            "provider_id": cid,
            "radius_miles": radius,
            "max_requested": top_k
        },
        "filtering": {
            "total_candidates": len(scored),
            "proximity_api_pre_filtered": True,
            "additional_filtering": False,
            "note": "All patients from proximity API are within requested radius"
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
            "Active_Cases_Count": active_cases_count_int,
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
            "type": ai_type,
            "error": ai_error,
        },
        "processing_time_seconds": round(time.time() - t0, 3),
        "total_candidates": len(candidates),
        "recommendations_count": len(out_recs),
        "max_requested": top_k,
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
    test_id = "6053290"  
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