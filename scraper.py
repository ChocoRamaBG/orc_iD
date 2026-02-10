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
    MAX_RUNTIME = 5 * 3600 # 5 часа за сигурност

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = "Superdoc_Full_List_012026_doc_formulas.xlsx"
    input_path = os.path.join(script_dir, input_file)
    
    output_folder = os.path.join(script_dir, "script")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = os.path.join(output_folder, "ORCID_Deep_Scan_Results.xlsx")

    processed_queries = set()
    all_results = []

    if os.path.exists(output_path):
        try:
            df_existing = pd.read_excel(output_path)
            all_results = df_existing.to_dict('records')
            processed_queries = set(df_existing['Search Query'].unique())
            print(f"--- Skibidi Logic: Намерихме {len(processed_queries)} вече обработени скандалчовци. Продължаваме! ---")
        except Exception as e:
            print(f"Грешка при четене на стария файл: {e}")

    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        if not os.path.exists(input_path):
            print("Мамка му човече, няма го входния файл в репозиторито!")
            return

        df_input = pd.read_excel(input_path)
        total_rows = len(df_input)
        print(f"Общо за проверка: {total_rows} картофчовци.")

        for index, row in df_input.iterrows():
            if time.time() - start_time > MAX_RUNTIME:
                print("Аура лимитът е достигнат! Спираме за днес, за да запишем прогреса.")
                break

            specialty = str(row.iloc[0]).strip()
            s_url = str(row.iloc[1]).strip()
            f_name_bg = str(row.iloc[2]).strip()
            f_name_lat = str(row.iloc[3]).strip()
            l_name_lat = str(row.iloc[4]).strip()
            
            search_query = f"{f_name_lat} {l_name_lat}"

            if f_name_lat.lower() == 'nan' or search_query in processed_queries:
                # Прескачаме вече готовите или празните
                continue

            search_url = f"https://orcid.org/orcid-search/search?firstName={f_name_lat}&lastName={l_name_lat}"
            print(f"[{index+1}/{total_rows}] Ровим за: {search_query}")
            driver.get(search_url)

            # Логика за парсване... (всичко вътре в while True)
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
                
                # Проверка за липса на резултати
                no_results = driver.find_elements(By.CLASS_NAME, "notFoundResults")
                if no_results and not no_results[0].get_attribute("hidden"):
                    all_results.append({
                        "Search Query": search_query, "Source Link": search_url,
                        "Специалност": specialty, "Superdoc URL": s_url, "Full Name": f_name_bg,
                        "ORCID ID": "No results found", "Affiliations": "-"
                    })
                else:
                    # Въртим страниците
                    while True:
                        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
                        for r in rows:
                            cols = r.find_elements(By.TAG_NAME, "td")
                            if len(cols) >= 5:
                                all_results.append({
                                    "Search Query": search_query, "Source Link": search_url,
                                    "Специалност": specialty, "Superdoc URL": s_url, "Full Name": f_name_bg,
                                    "ORCID ID": cols[0].text.strip(),
                                    "ORCID First Name": cols[1].text.strip(),
                                    "ORCID Last Name": cols[2].text.strip(),
                                    "Other Names": cols[3].text.strip(),
                                    "Affiliations": cols[4].text.strip()
                                })
                        
                        # Пагинация
                        try:
                            label_el = driver.find_element(By.CLASS_NAME, "mat-mdc-paginator-range-label")
                            pages = re.findall(r'\d+', label_el.text)
                            if len(pages) >= 2 and int(pages[0]) < int(pages[1]):
                                next_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Next page']")
                                driver.execute_script("arguments[0].click();", next_btn)
                                time.sleep(3)
                            else:
                                break
                        except:
                            break
            except TimeoutException:
                print(f"Мамка му, ORCID заспа за {search_query} - прескачаме го този гащник.")

            # Обновяваме файла и сета с обработени заявки
            processed_queries.add(search_query)
            pd.DataFrame(all_results).to_excel(output_path, index=False)

    finally:
        driver.quit()
        print("Цикълът приключи. Малини и къпини, все тая, важното е да сме записали нещо!")

if __name__ == "__main__":
    orcid_adaptive_parser()
