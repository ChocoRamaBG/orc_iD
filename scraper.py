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
    print("Йо шефе, палим гумите...")
    start_time = time.time()
    # Намаляваме малко времето, за да има време да запише преди Github да го убие брутално
    MAX_RUNTIME = 5.5 * 3600 

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Увери се, че името на файла е 1:1, иначе ще ядем хурката
    input_file = "Superdoc_Full_List_012026_doc_formulas - Remaining.xlsx"
    input_path = os.path.join(script_dir, input_file)
    
    output_folder = os.path.join(script_dir, "script")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = os.path.join(output_folder, "ORCID_Deep_Scan_Results.xlsx")

    processed_queries = set()
    all_results = []

    # --- THE FIX: Зареждане с "Rizz" ---
    if os.path.exists(output_path):
        try:
            # Четем всичко като string, за да няма 'nan' float мизерии
            df_existing = pd.read_excel(output_path, dtype=str)
            
            # Чистим всички празни пространства и правим списъка
            if 'Search Query' in df_existing.columns:
                # Превръщаме в string, махаме whitespace и пълним сета
                processed_queries = set(df_existing['Search Query'].astype(str).str.strip().unique())
                
                # Възстановяваме старите резултатчовци, за да не ги загубим при презапис
                all_results = df_existing.to_dict('records')
                
            print(f"--- Skibidi Logic: Намерихме {len(processed_queries)} вече обработени скандалчовци. Продължаваме напред! ---")
        except Exception as e:
            print(f"Грешка при четене на стария файл (Hell no): {e}")
            # Ако файлът е счупен, правим бекъп и почваме на чисто, малини и къпини, все тая
            if os.path.exists(output_path):
                os.rename(output_path, output_path + f".backup_{int(time.time())}.xlsx")

    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # User-Agent, за да не ни мислят за ботове (въпреки че сме)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    try:
        if not os.path.exists(input_path):
            print("Мамка му човече, няма го входния файл в репозиторито!")
            return

        df_input = pd.read_excel(input_path)
        total_rows = len(df_input)
        print(f"Общо за проверка: {total_rows} картофчовци.")

        save_counter = 0 # Брояч за периодичен запис

        for index, row in df_input.iterrows():
            # Проверка за време - спираме малко преди лимита
            if time.time() - start_time > MAX_RUNTIME:
                print("⚠️ Аура лимитът е достигнат! Спираме за днес, за да запишем прогреса.")
                break

            # Взимаме данните и ги чистим от боклуци
            specialty = str(row.iloc[0]).strip()
            s_url = str(row.iloc[1]).strip()
            f_name_bg = str(row.iloc[2]).strip()
            f_name_lat = str(row.iloc[3]).strip()
            l_name_lat = str(row.iloc[4]).strip()
            
            # Формираме ключа за търсене
            search_query = f"{f_name_lat} {l_name_lat}".strip()

            # --- LOGIC CHECK ---
            # Ако името е 'nan' или ВЕЧЕ Е В СПИСЪКА -> skip
            # Използваме string конверсия за всеки случай
            if f_name_lat.lower() == 'nan' or str(search_query) in processed_queries:
                # print(f"Skipping {search_query} - already has rizz.") # Spam filter
                continue

            search_url = f"https://orcid.org/orcid-search/search?firstName={f_name_lat}&lastName={l_name_lat}"
            print(f"[{index+1}/{total_rows}] Ровим за: {search_query}")
            
            try:
                driver.get(search_url)
                
                # Чакаме малко повече, ORCID са бавни като държавна администрация
                try:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
                except TimeoutException:
                    # Може да няма резултати и да не зареди tbody, проверяваме за notFound
                    pass

                found_something = False
                
                # Проверка за липса на резултати
                no_results = driver.find_elements(By.CLASS_NAME, "notFoundResults")
                if no_results and not no_results[0].get_attribute("hidden"):
                    all_results.append({
                        "Search Query": search_query, "Source Link": search_url,
                        "Специалност": specialty, "Superdoc URL": s_url, "Full Name": f_name_bg,
                        "ORCID ID": "No results found", "Affiliations": "-"
                    })
                else:
                    # Въртим страниците (Pagination Logic)
                    page_count = 0
                    while True:
                        rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")
                        if not rows:
                            break # Safety break
                            
                        for r in rows:
                            cols = r.find_elements(By.TAG_NAME, "td")
                            if len(cols) >= 4: # ORCID понякога сменят колоните
                                orcid_id = cols[0].text.strip() if len(cols) > 0 else "-"
                                first_n = cols[1].text.strip() if len(cols) > 1 else "-"
                                last_n = cols[2].text.strip() if len(cols) > 2 else "-"
                                other_n = cols[3].text.strip() if len(cols) > 3 else "-"
                                affil = cols[4].text.strip() if len(cols) > 4 else "-"

                                all_results.append({
                                    "Search Query": search_query, "Source Link": search_url,
                                    "Специалност": specialty, "Superdoc URL": s_url, "Full Name": f_name_bg,
                                    "ORCID ID": orcid_id,
                                    "ORCID First Name": first_n,
                                    "ORCID Last Name": last_n,
                                    "Other Names": other_n,
                                    "Affiliations": affil
                                })
                                found_something = True
                        
                        # Пагинация - само първите 2-3 страници, да не прекаляваме
                        page_count += 1
                        if page_count > 2: break 

                        try:
                            next_btn = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Next page']")
                            if next_btn and next_btn[0].is_enabled():
                                driver.execute_script("arguments[0].click();", next_btn[0])
                                time.sleep(2) # Brainrot delay
                            else:
                                break
                        except:
                            break
                
                # Ако нищо не е намерено и не сме влезли в no_results (странен случай)
                if not found_something and not (no_results and not no_results[0].get_attribute("hidden")):
                     all_results.append({
                        "Search Query": search_query, "Source Link": search_url,
                        "Специалност": specialty, "Superdoc URL": s_url, "Full Name": f_name_bg,
                        "ORCID ID": "No results found (Timeout/Error)", "Affiliations": "-"
                    })

            except Exception as e:
                print(f"Мамка му, ORCID гръмна за {search_query}: {e}")
            
            # Маркираме като обработен
            processed_queries.add(search_query)
            save_counter += 1

            # --- BATCH SAVING ---
            # Записваме само на всеки 10 човека или ако е минало много време
            # Това предпазва файла от корупция (IO error) и е по-бързо
            if save_counter >= 10:
                print("💾 Записваме междинни резултатчовци...")
                try:
                    df_out = pd.DataFrame(all_results)
                    # Пренареждаме колоните за красота (optional)
                    cols_order = ["Search Query", "Full Name", "ORCID ID", "Affiliations", "ORCID First Name", "ORCID Last Name", "Source Link"]
                    existing_cols = [c for c in cols_order if c in df_out.columns]
                    remainder = [c for c in df_out.columns if c not in cols_order]
                    df_out = df_out[existing_cols + remainder]
                    
                    df_out.to_excel(output_path, index=False)
                    save_counter = 0 # Нулираме брояча
                except Exception as save_err:
                    print(f"Не можах да запиша файла! {save_err}")

    finally:
        # Финален запис при излизане (дори при грешка или timeout)
        print("Финализиране... записваме последните данни.")
        try:
            if all_results:
                pd.DataFrame(all_results).to_excel(output_path, index=False)
                print("Успешен запис. Довиждане, льольо.")
        except:
            print("Баси, дори финалният запис не стана.")
            
        driver.quit()
        print("Цикълът приключи. Малини и къпини, все тая.")

if __name__ == "__main__":
    orcid_adaptive_parser()
