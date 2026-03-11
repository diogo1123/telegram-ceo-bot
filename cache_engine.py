import os
import json
import time
from datetime import datetime

CACHE_DIR = os.path.join(os.getcwd(), 'cache')

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_filename(rest_id, report_type, start_date, end_date):
    return os.path.join(CACHE_DIR, f"{rest_id}_{report_type}_{start_date}_{end_date}.json")

def get_cached(rest_id, report_type, start_date, end_date, fetch_function):
    """
    Wrap an API call with caching logic.
    - If end_date < today -> Immutable cache (cache forever).
    - If end_date >= today -> Ephemeral cache (cache for 15 minutes to avoid duplicate inner calls).
    """
    today_str = datetime.now().strftime('%Y-%m-%d')
    is_live = end_date >= today_str
    
    filename = get_cache_filename(rest_id, report_type, start_date, end_date)
    
    # Check cache existence
    if os.path.exists(filename):
        # Se for um dado do passado, carregamos instantaneamente sempre
        if not is_live:
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except: pass
            
        # Se for livedata (hoje), só carrega se o cache tiver menos de 15 minutos
        else:
            file_age = time.time() - os.path.getmtime(filename)
            if file_age < 900: # 15 minutos
                try:
                    with open(filename, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except: pass
                
    # Se não tem cache (ou o livedata tá velho), bate na API oficial!
    data = fetch_function(rest_id, start_date, end_date)
    
    # Salva o arquivo pra próxima vez!
    if data:
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except: pass
        
    return data
