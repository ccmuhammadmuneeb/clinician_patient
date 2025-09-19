def fast_ai_score(candidates: List[Dict[str,Any]],
                  clinician_info: Dict[str,Any],
                  active_case_ids_resolved: List[str]) -> List[Dict[str,Any]]:
    """Fast AI scoring with simplified logic for performance"""
    
    print(f"🚀 Using FAST AI scoring for {len(candidates)} candidates")
    start_time = time.time()
    
    try:
        if not init_gemini():
            raise RuntimeError("Gemini initialization failed")
        
        # Process only top candidates for speed
        limited_candidates = candidates[:10]  # Limit to 10 best candidates
        
        # Simple scoring logic for quick processing
        scored_candidates = []
        
        for candidate in limited_candidates:
            try:
                # Quick rule-based pre-scoring
                base_score = 50  # Start with base score
                
                # Previous provider bonus
                if candidate.get("PREVIOUS_PROVIDER_ID") == clinician_info.get("ID"):
                    base_score += 30
                
                # Distance bonus
                distance = candidate.get("DISTANCE_FROM_ACTIVE_CASE", candidate.get("DISTANCE"))
                if distance and str(distance).replace('.', '').isdigit():
                    dist_val = float(distance)
                    if dist_val <= 5:
                        base_score += 25
                    elif dist_val <= 10:
                        base_score += 15
                    else:
                        base_score += 5
                
                # Case type bonus
                case_type = candidate.get("CASE_TYPE", "").lower()
                if any(keyword in case_type for keyword in ["post-surgical", "fall", "discharge"]):
                    base_score += 15
                
                # Cap at 100
                final_score = min(base_score, 100)
                
                enriched = candidate.copy()
                enriched["Match_Score"] = final_score
                enriched["Reason"] = f"Fast scoring - distance: {distance}mi, prev provider: {'Yes' if candidate.get('PREVIOUS_PROVIDER_ID') == clinician_info.get('ID') else 'No'}"
                
                scored_candidates.append(enriched)
                
            except Exception as e:
                # Fallback scoring
                enriched = candidate.copy()
                enriched["Match_Score"] = 25
                enriched["Reason"] = f"Fallback scoring due to error: {str(e)[:50]}"
                scored_candidates.append(enriched)
        
        # Add remaining candidates with lower scores
        for candidate in candidates[10:]:
            enriched = candidate.copy()
            enriched["Match_Score"] = 20  # Lower score for non-processed candidates
            enriched["Reason"] = "Quick processing - basic scoring applied"
            scored_candidates.append(enriched)
        
        elapsed = time.time() - start_time
        print(f"⏱️ [FAST AI] Completed in {elapsed:.2f} seconds")
        
        return scored_candidates
        
    except Exception as e:
        print(f"❌ Fast AI scoring failed: {e}")
        # Return candidates with basic scores
        return [
            {**candidate, "Match_Score": 30, "Reason": f"AI unavailable: {str(e)[:50]}"}
            for candidate in candidates
        ]