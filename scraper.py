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
    # 5.2 часа лимит, за да остане време за финалния запис и push към GitHub
    MAX_RUNTIME = 5.2 * 3600 

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = "Superdoc_Full_List_012026_doc_formulas.xlsx"
    input_path = os.path.join(script_dir, input_file)
    
    # Резултатите отиват в папка script
    output_folder = os.path.join(script_dir, "script")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = os.path.join(output_folder, "ORCID_Deep_Scan_Results.xlsx")

    all_results = []
    processed_queries = set()

    # Проверяваме за вече съществуващи картофчовци, за да не ги повтаряме
    if os.path.exists(output_path):
        try:
            df_existing = pd.read_excel(output_path)
            all_results = df_existing.to_dict('records')
            # Помни кои Search Queries вече са в кюпа
            processed_queries = set(df_existing['Search Query'].unique())
            print(f"Намерихме {len(processed_queries)} вече обработени скандалчовци. Продължаваме нататък!")
        except Exception as e:
            print(f"Грешка при четене на стария файл (малини и къпини...): {e}")

    chrome_options = Options()
    chrome_options.add_argument("--headless=new") # Задължително за облака
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        if not os.path.exists(input_path):
            print("Мамка му човече, няма го входния файл!")
            return

        df_input = pd.read_excel(input_path)

        for index, row in df_input.iterrows():
            # Проверка на таймера
            if time.time() - start_time > MAX_RUNTIME:
                print("Времето изтича, гащници! Спираме, за да съхраним прогреса.")
                break

            # Данни от оригиналните колони
            specialty = str(row.iloc[0]).strip()   # A
            superdoc_url = str(row.iloc[1]).strip() # B
            full_name_bg = str(row.iloc[2]).strip() # C
            f_name_lat = str(row.iloc[3]).strip()  # D
            l_name_lat = str(row.iloc[4]).strip()  # E
            
            search_query = f"{f_name_lat} {l_name_lat}"

            if f_name_lat.lower() == 'nan' or search_query in processed_queries:
                continue

            search_url = f"https://orcid.org/orcid-search/search?firstName={f_name_lat}&lastName={l_name_lat}"
            print(f"[{index}] Ровим за: {search_query}")
            driver.get(search_url)

            # Пагинация - въртим, докато има страници
            while True:
                try:
                    # Чакаме да се появи таблицата
                    wait = WebDriverWait(driver, 15)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
                    
                    # Гледаме дали изобщо има резултати
                    no_results = driver.find_elements(By.CLASS_NAME, "notFoundResults")
                    if no_results and not no_results[0].get_attribute("hidden"):
                        all_results.append({
                            "Search Query": search_query, "Source Link": search_url,
                            "Специалност": specialty, "Superdoc URL": superdoc_url, "Full Name": full_name_bg,
                            "ORCID ID": "No results found", "Affiliations": "-"
                        })
                        break

                    # Извличаме редовете от текущата страница
                    rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
                    for r in rows:
                        cols = r.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 5:
                            all_results.append({
                                "Search Query": search_query, "Source Link": search_url,
                                "Специалност": specialty, "Superdoc URL": superdoc_url, "Full Name": full_name_bg,
                                "ORCID ID": cols[0].text.strip(),
                                "ORCID First Name": cols[1].text.strip(),
                                "ORCID Last Name": cols[2].text.strip(),
                                "Other Names": cols[3].text.strip(),
                                "Affiliations": cols[4].text.strip()
                            })

                    # Логика за пагинация: "Page 1 of 12"
                    try:
                        label_el = driver.find_element(By.CLASS_NAME, "mat-mdc-paginator-range-label")
                        pages = re.findall(r'\d+', label_el.text)
                        
                        if len(pages) >= 2:
                            current = int(pages[0])
                            total = int(pages[1])
                            
                            if current < total:
                                print(f"  --> Отиваме на страница {current + 1} от {total}")
                                next_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Next page']")
                                driver.execute_script("arguments[0].click();", next_btn)
                                time.sleep(3) # Angular-ът иска време да се съвземе
                                continue
                        break # Край на страниците за този докторчо
                    except Exception:
                        break # Няма пагинация

                except TimeoutException:
                    print(f"Мамка му, ORCID заспа за {search_query}!")
                    break

            # Записваме прогреса след всеки успешно завършен доктор
            pd.DataFrame(all_results).to_excel(output_path, index=False)

    finally:
        driver.quit()
        print("Всички налични картофчовци бяха обработени за този цикъл!")

if __name__ == "__main__":
    orcid_adaptive_parser()
