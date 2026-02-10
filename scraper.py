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

def orcid_paginator_parser():
    # Вече не ползваме твърди пътища, че е скандално!
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Входният файл трябва да е в същата папка на GitHub
    input_file_name = "Superdoc_Full_List_012026_doc_formulas.xlsx"
    input_path = os.path.join(script_dir, input_file_name)
    
    # Изходът си остава в папка 'script' според завета
    output_folder = os.path.join(script_dir, "script")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    output_path = os.path.join(output_folder, "ORCID_Deep_Scan_Results.xlsx")

    chrome_options = Options()
    # Задължително headless за GitHub Actions/Cloud
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        if not os.path.exists(input_path):
            print(f"Мамка му човече, няма го файла {input_file_name} в папката!")
            return

        df_input = pd.read_excel(input_path)
        all_results = []

        for index, row in df_input.iterrows():
            # Напасваме колоните: А(0), B(1), C(2), D(3), E(4)
            specialty = str(row.iloc[0]).strip()   
            superdoc_url = str(row.iloc[1]).strip() 
            full_name_bg = str(row.iloc[2]).strip() 
            first_name_lat = str(row.iloc[3]).strip() 
            last_name_lat = str(row.iloc[4]).strip()  
            
            if first_name_lat.lower() == 'nan' or last_name_lat.lower() == 'nan':
                continue

            search_query = f"{first_name_lat} {last_name_lat}"
            search_url = f"https://orcid.org/orcid-search/search?firstName={first_name_lat}&lastName={last_name_lat}"
            
            print(f"--- Сканираме за тези картофчовци: {search_query} ---")
            driver.get(search_url)

            while True:
                try:
                    # Увеличаваме малко времето за чакане, че облакът понякога лагва
                    wait = WebDriverWait(driver, 15)
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
                    
                    no_res = driver.find_elements(By.CLASS_NAME, "notFoundResults")
                    if no_res and not no_res[0].get_attribute("hidden"):
                        all_results.append({
                            "Search Query": search_query, "Source Link": search_url,
                            "Специалност": specialty, "Superdoc URL": superdoc_url, "Full Name": full_name_bg,
                            "ORCID ID": "No results", "Affiliations": "-"
                        })
                        break

                    rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
                    for r in rows:
                        cols = r.find_elements(By.TAG_NAME, "td")
                        if len(cols) >= 5:
                            all_results.append({
                                "Search Query": search_query,
                                "Source Link": search_url,
                                "Специалност": specialty,
                                "Superdoc URL": superdoc_url,
                                "Full Name": full_name_bg,
                                "ORCID ID": cols[0].text.strip(),
                                "ORCID First Name": cols[1].text.strip(),
                                "ORCID Last Name": cols[2].text.strip(),
                                "Other Names": cols[3].text.strip(),
                                "Affiliations": cols[4].text.strip()
                            })

                    # Пагинация
                    try:
                        range_label_el = driver.find_elements(By.CLASS_NAME, "mat-mdc-paginator-range-label")
                        if not range_label_el:
                            break
                            
                        range_label = range_label_el[0].text
                        pages = re.findall(r'\d+', range_label)
                        
                        if len(pages) >= 2:
                            current_page = int(pages[0])
                            total_pages = int(pages[1])
                            
                            if current_page < total_pages:
                                next_btn = driver.find_element(By.CSS_SELECTOR, "button[aria-label='Next page']")
                                driver.execute_script("arguments[0].click();", next_btn)
                                time.sleep(3) 
                                continue
                            else:
                                break
                        else:
                            break
                    except Exception:
                        break

                except TimeoutException:
                    print(f"Мамка му, тия скандалчовци от ORCID не отговарят за {search_query}.")
                    break

            # Записваме прогреса
            pd.DataFrame(all_results).to_excel(output_path, index=False)

        print(f"Готово! Всички картофчовци са в кюпа: {output_path}")

    finally:
        driver.quit()

if __name__ == "__main__":
    orcid_paginator_parser()
