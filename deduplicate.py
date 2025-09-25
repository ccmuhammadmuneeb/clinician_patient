def deduplicate_recommendations(candidates):
    """
    Remove duplicate patients from recommendations list,
    keeping only the highest scored instance of each patient ID.
    
    Args:
        candidates: List of patient recommendation candidates
        
    Returns:
        Deduplicated list of patient recommendations, sorted by same final sort key
    """
    unique_patients = {}
    duplicate_count = 0
    
    # Keep only the highest scored instance of each patient
    for candidate in candidates:
        patient_id = str(candidate.get("ID", ""))
        current_score = candidate.get("Match_Score", 0)
        
        if patient_id in unique_patients:
            duplicate_count += 1
            # Keep the one with the higher score
            if current_score > unique_patients[patient_id].get("Match_Score", 0):
                unique_patients[patient_id] = candidate
        else:
            unique_patients[patient_id] = candidate
    
    if duplicate_count > 0:
        print(f"⚠️ Found and removed {duplicate_count} duplicate patient IDs")
    
    # Convert back to a sorted list
    from operator import itemgetter
    deduplicated_candidates = list(unique_patients.values())
    # Sort by Match_Score descending
    deduplicated_candidates.sort(key=lambda x: (-1 * float(x.get("Match_Score", 0) or 0)))
    
    return deduplicated_candidates