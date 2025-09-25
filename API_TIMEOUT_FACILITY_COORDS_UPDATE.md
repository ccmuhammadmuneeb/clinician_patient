# API Timeout and Facility Coordinates Update

## ✅ **Changes Implemented**

### 🕐 **2-Minute API Timeout**
- **Updated API_TIMEOUT**: Changed from 5 seconds to 120 seconds (2 minutes)
- **Added timeout handling**: Both clinician and proximity cases APIs now handle timeouts gracefully
- **Timeout Response**: Returns structured data with null values instead of failing completely

### 📍 **Facility Coordinates Priority**
- **New Priority Logic**: 
  1. **FIRST**: Try facility coordinates (`Facility_Lat`, `Facility_Long`)
  2. **FALLBACK**: Use clinician coordinates (`Latitude`, `Longitude`) if facility not available
- **Enhanced Logging**: Clear indication of which coordinate source is being used

### 🔧 **Technical Details**

#### API Timeout Handling:
```python
API_TIMEOUT = 120  # 2 minutes timeout for API connections

# Clinician API timeout handling
except requests.exceptions.Timeout:
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

# Proximity Cases API timeout handling  
except requests.exceptions.Timeout:
    return {
        "Cases": [],
        "timeout_occurred": True,
        "error_message": f"Proximity cases API timed out after {API_TIMEOUT} seconds"
    }
```

#### Facility Coordinates Priority Logic:
```python
# PRIORITY 1: Try facility coordinates first (if available)
facility_lat = clinician_info.get("Facility_Lat") or clinician_info.get("facility_latitude") 
facility_lon = clinician_info.get("Facility_Long") or clinician_info.get("facility_longitude")

if facility_lat and facility_lon:
    clinician_coords = _coords_from_cols(facility_lat, facility_lon)
    if clinician_coords:
        print(f"✅ Using facility coordinates: {clinician_coords}")

# PRIORITY 2: Fall back to individual clinician coordinates if facility not available
if not clinician_coords:
    lat = clinician_info.get("Latitude")
    lon = clinician_info.get("Longitude")
    clinician_coords = _coords_from_cols(lat, lon)
    if clinician_coords:
        print(f"✅ Using clinician coordinates: {clinician_coords}")
```

### 🎯 **Benefits**

1. **Robust Timeout Handling**:
   - APIs won't hang indefinitely  
   - Postman connections stay alive properly
   - Graceful degradation with null values instead of complete failure

2. **Better Distance Calculations**:
   - More accurate facility-based distances when available
   - Fallback ensures no loss of functionality
   - Clear logging shows which coordinate source was used

3. **Production Ready**:
   - 2-minute timeout handles slow network conditions
   - Structured error responses for better debugging
   - Maintains backward compatibility

### 📊 **Expected Behavior**

**Facility Coordinates Available**:
```
✅ Using facility coordinates: (40.7128, -74.0060)
[LOCATION] Final clinician coordinates: (40.7128, -74.0060)
```

**Facility Coordinates Missing**:
```
Falling back to clinician coordinates: LAT=40.7589, LON=-73.9851
✅ Using clinician coordinates: (40.7589, -73.9851)
[LOCATION] Final clinician coordinates: (40.7589, -73.9851)
```

**API Timeout**:
```
⏰ API request timeout after 120 seconds for provider 6053290
⚠️ Clinician API timed out, proceeding with limited data
   Error: API request timed out after 120 seconds
```

The system now handles slow API responses better and prioritizes facility-based distance calculations for more accurate patient-provider matching! 🚀