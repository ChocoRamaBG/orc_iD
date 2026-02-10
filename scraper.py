import pandas as pd
import os
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def orcid_adaptive_parser():
    start_time = time.time()
    # 5.5 часа лимит (в секунди), за да остане време за запис в GitHub
    MAX_RUNTIME = 5.5 * 3600 

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = "Superdoc_Full_List_012026_doc_formulas.xlsx"
    input_path = os.path.join(script_dir, input_file)
    
    output_folder = os.path.join(script_dir, "script")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = os.path.join(output_folder, "ORCID_Deep_Scan_Results.xlsx")

    # Проверяваме докъде сме стигнали
    processed_queries = set()
    existing_data = []
    if os.path.exists(output_path):
        try:
            df_existing = pd.read_excel(output_path)
            existing_data = df_existing.to_dict('records')
            processed_queries = set(df_existing['Search Query'].unique())
            print(f"Намерихме {len(processed_queries)} вече обработени скандалчовци. Продължаваме нататък!")
        except Exception:
            print("Започваме начисто, льольо!")

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        df_input = pd.read_excel(input_path)
        
        for index, row in df_input.iterrows():
            # Проверка на времето - да не ни отреже GitHub главата
            if time.time() - start_time > MAX_RUNTIME:
                print("Времето изтича! Спираме за днес, за да запишем прогреса.")
                break

            f_name_lat = str(row.iloc[3]).strip()
            l_name_lat = str(row.iloc[4]).strip()
            search_query = f"{f_name_lat} {l_name_lat}"

            if f_name_lat.lower() == 'nan' or search_query in processed_queries:
                continue

            search_url = f"https://orcid.org/orcid-search/search?firstName={f_name_lat}&lastName={l_name_lat}"
            print(f"Ровим за: {search_query}...")
            driver.get(search_url)

            # --- Тук е логиката за парсване на страниците (същата като преди) ---
            # ... (за краткост използваме същата логика за пагинация) ...
            # След всяко име добавяме в existing_data и записваме файла
            # existing_data.append(...) 
            # pd.DataFrame(existing_data).to_excel(output_path, index=False)
            
            # (ВАЖНО: В пълния код тук стои логиката с while True от предишния ми отговор)
