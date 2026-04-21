import pandas as pd
import os
import sys
import time
import subprocess
import json
import numpy as np
from playwright.sync_api import sync_playwright

# Import analysis module
try:
    import keywords_analysis
except ImportError:
    print("WARNING: 'keywords_analysis.py' not found. Report generation will be skipped after scraping.")

import tempfile

# ================= Configuration =================
TARGET_URL = "https://superset.sds.advertising.amazon.dev/superset/dashboard/2/"
# Portable path relative to current script
_BASE = os.path.dirname(os.path.abspath(__file__))
CHROME_USER_DATA = os.path.normpath(os.path.join(_BASE, "..", "browser_data"))
if not os.path.exists(CHROME_USER_DATA):
    try: os.makedirs(CHROME_USER_DATA, exist_ok=True)
    except: CHROME_USER_DATA = os.path.join(tempfile.gettempdir(), "sel_chrome_ast")
MASTER_FILENAME = os.path.normpath(os.path.join(_BASE, "..", "Excel Template", "ASIN_Input_Template For Keywords.xlsx"))

# [New] Global Reporting Directory
REPORTING_DIR = os.path.normpath(os.path.join(_BASE, "..", "Reporting-keyword"))
if not os.path.exists(REPORTING_DIR):
    os.makedirs(REPORTING_DIR, exist_ok=True)

# 1. Marketplace Mapping
MARKETPLACE_MAP = {
    'US': 'Amazon.com', 'CA': 'Amazon.ca', 'MX': 'Amazon.com.mx', 'BR': 'Amazon.com.br',
    'DE': 'Amazon.de', 'UK': 'Amazon.co.uk', 'GB': 'Amazon.co.uk', 
    'FR': 'Amazon.fr', 'IT': 'Amazon.it', 'ES': 'Amazon.es', 
    'NL': 'Amazon.nl', 'TR': 'Amazon.com.tr', 'SE': 'Amazon.se',
    'PL': 'Amazon.pl', 'BE': 'Amazon.com.be', 'EG': 'Amazon.eg',
    'AE': 'Amazon.ae', 'SA': 'Amazon.sa',
    'JP': 'Amazon.co.jp', 'AU': 'Amazon.com.au', 'SG': 'Amazon.sg', 'IN': 'Amazon.in'
}

# 2. Region Mapping
REGION_MAP = {
    'US': 'NA', 'CA': 'NA', 'MX': 'NA', 'BR': 'NA',
    'Amazon.com': 'NA', 'Amazon.ca': 'NA', 'Amazon.com.mx': 'NA', 'Amazon.com.br': 'NA',
    'DE': 'EU', 'UK': 'EU', 'GB': 'EU', 'FR': 'EU', 'IT': 'EU', 'ES': 'EU',
    'NL': 'EU', 'TR': 'EU', 'SE': 'EU', 'PL': 'EU', 'BE': 'EU', 'EG': 'EU',
    'AE': 'EU', 'SA': 'EU',
    'Amazon.de': 'EU', 'Amazon.co.uk': 'EU', 'Amazon.fr': 'EU', 'Amazon.it': 'EU', 'Amazon.es': 'EU',
    'JP': 'FE', 'AU': 'FE', 'SG': 'FE', 'IN': 'FE',
    'Amazon.co.jp': 'FE', 'Amazon.com.au': 'FE', 'Amazon.sg': 'FE', 'Amazon.in': 'FE'
}

DEFAULT_REGION = 'NA'

# ================= Launch Chrome =================
def launch_chrome_with_debug():
    """启动有头 Chrome 调试模式，返回进程对象"""
    possible_paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe") 
    ]
    chrome_path = next((p for p in possible_paths if os.path.exists(p)), None)
    if not chrome_path:
        print("ERROR: Chrome executable not found.")
        return None

    print(f"[系统] 启动 Chrome (有头模式): {chrome_path}")
    cmd = [
        chrome_path,
        "--remote-debugging-port=9223",
        f"--user-data-dir={CHROME_USER_DATA}",
        "--no-first-run"
    ]
    try:
        proc = subprocess.Popen(cmd)
        time.sleep(0.5)
        return proc
    except Exception as e:
        print(f"启动失败: {e}")
        return None

# ================= Scraper Core =================
def run_downloader(df_master, download_dir, target_weeks):
    # --- 1. Task Pre-processing ---
    country_col = None
    for col in df_master.columns:
        if "Country" in col or "Marketplace" in col:
            country_col = col
            break
    if not country_col: country_col = df_master.columns[3] 

    tasks = {} 
    # Determine columns more robustly
    col_child = next((c for c in df_master.columns if 'Child ASIN' in str(c)), 'Child ASIN')
    col_comp = next((c for c in df_master.columns if 'Competitor ASIN' in str(c)), 'Competitor ASIN')

    for country in df_master[country_col].unique():
        if pd.isna(country) or str(country).strip() == '': continue
        country_code = str(country).strip().upper()
        country_data = df_master[df_master[country_col] == country]
        
# ================= [强化版] 提取并彻底去重当前国家的所有 ASIN =================
        child_list = country_data[col_child].dropna().tolist() if col_child in country_data.columns else []
        comp_list = country_data[col_comp].dropna().tolist() if col_comp in country_data.columns else []
        
        # 1. 合并本品和竞品
        merged_list = child_list + comp_list
        
        # 2. 过滤掉长度不够的无效值、"NAN" 等，确保留下的都是干净的字符串
        all_valid_asins = [str(a).strip() for a in merged_list if len(str(a).strip()) >= 8 and str(a).upper() != 'NAN']
        
        # 3. 使用 Python 原生 set() 进行终极去重！
        # 无论 Excel 里重复了多少次，进入 unique_asins 后永远只剩唯一的一个
        unique_asins = list(set(all_valid_asins))
        
        if unique_asins:
            tasks[country_code] = unique_asins
        # =========================================================================

    if not tasks:
        print("ERROR: No valid tasks extracted.")
        return

    # --- 2. Playwright Execution ---
    print("Connecting to Chrome (auto-detecting port)...")
    
    with sync_playwright() as p:
        browser = None
        try:
            from chrome_port_finder import get_cdp_url
            cdp_url = get_cdp_url()
            browser = p.chromium.connect_over_cdp(cdp_url)
            if not browser.contexts: 
                return
            context = browser.contexts[0]
            context.grant_permissions(["clipboard-read", "clipboard-write"])

            print("Opening operational tab...")
            page = context.new_page()
            page.set_viewport_size({"width": 1920, "height": 1080})
        
            print(f"Navigating to: {TARGET_URL}")
            page.goto(TARGET_URL, timeout=90000, wait_until='domcontentloaded')
            page.bring_to_front()

            print("Checking login status...")
            try:
                page.wait_for_selector(".ant-select-selector", state="visible", timeout=8000)
                print("Login verified.")
            except:
                print("Please log in manually in the browser...")
                page.wait_for_selector(".ant-select-selector", state="visible", timeout=0)
                print("Login successful.")

            is_global_settings_set = False

            for country_code, all_asins in tasks.items():
                print(f"Processing: {country_code} | ASIN Count: {len(all_asins)}")
                
                # ================= 401 Session Detection =================
                if "login" in page.url or "signin" in page.url:
                    print("Session expired. Attempting to reload...")
                    try:
                        page.close()
                        page = context.new_page()
                        page.set_viewport_size({"width": 1920, "height": 1080})
                        page.goto(TARGET_URL, timeout=60000, wait_until='domcontentloaded')
                        print("Waiting for page reload/auto-login...")
                        page.wait_for_selector(".ant-select-selector", state="visible", timeout=30000)
                        print("Recovery successful.")
                    except Exception as e:
                        print(f"Recovery failed: {e}")
                        return 

                # ================= Business Logic =================
                try:
                    marketplace_domain = MARKETPLACE_MAP.get(country_code, country_code)
                    region = REGION_MAP.get(country_code, REGION_MAP.get(marketplace_domain, DEFAULT_REGION))
                    
                    page.wait_for_selector(".ant-select-selector", timeout=15000)

                    # [1] Region
                    print(f" Setting Region -> {region}")
                    try:
                        page.locator(".ant-select-selector").nth(0).click()
                        page.keyboard.insert_text(region) 
                        page.keyboard.press("Enter")
                        page.locator("body").click()
                        time.sleep(0.5) 
                    except Exception as e:
                        print(f" Region setting exception: {e}")

                    # [2] Marketplace
                    print(f" Setting Marketplace -> {marketplace_domain}")
                    market_container = page.locator(".ant-select-selector").nth(1)
                    market_real_input = market_container.locator("input.ant-select-selection-search-input").first
                    
                    market_container.click(force=True)
                    time.sleep(0.3)
                    
                    try:
                        for _ in range(20): page.keyboard.press("Backspace")
                    except: pass
                    
                    try:
                        market_real_input.click(force=True) 
                        page.keyboard.press("Control+A")
                        page.keyboard.press("Delete")
                    except: 
                        market_container.click(force=True)

                    print(f" Typing value: {marketplace_domain}")
                    market_real_input.click(force=True) 
                    time.sleep(0.2)
                    page.keyboard.type(marketplace_domain, delay=150)
                    time.sleep(1)
                    page.keyboard.press("Enter")
                    page.locator("body").click()
                    time.sleep(1)

                    # [3] ASIN
                    print(f" Setting ASINs...")
                    asin_container = page.locator(".ant-select-selector").nth(2)
                    real_input = page.locator("input.ant-select-selection-search-input").nth(2)

                    try:
                        real_input.click(force=True)
                        time.sleep(0.2)
                        page.keyboard.press("Enter")
                        time.sleep(0.1)
                        page.keyboard.press("Enter")
                        print(" -> Cleared ASINs using 'Double Enter' method.")
                    except:
                        try: real_input.click(force=True)
                        except: asin_container.click(force=True)
                        time.sleep(0.2)
                        for _ in range(200): page.keyboard.press("Backspace")
                        page.keyboard.press("Control+A")
                        page.keyboard.press("Delete")
                    
                    if all_asins:
                        clipboard_content = "\n".join(all_asins)
                        json_content = json.dumps(clipboard_content)
                        page.evaluate(f"navigator.clipboard.writeText({json_content})")
                        
                        try: real_input.click(force=True)
                        except: asin_container.click(force=True)
                        time.sleep(0.1)
                        page.keyboard.press("Control+V")
                        
                        try: page.locator(".ant-select-selection-item").first.wait_for(state="visible", timeout=5000)
                        except: time.sleep(0.5)
                        page.locator("body").click()

                    # [Global Setup] - Run Once
                    if not is_global_settings_set:
                        print(" Global setup: Tab / Time / Language")
                        
                        # Tab
                        try:
                            target_tab_text = "Top 100 Keyword Trends by ASINs (Data Table Only)"
                            tab_locator = page.get_by_text(target_tab_text, exact=False).first
                            if tab_locator.is_visible():
                                tab_locator.click(force=True)
                                page.locator(".ant-select-selector").nth(1).wait_for(state="visible", timeout=5000)
                            time.sleep(0.5)
                        except: print(" Tab switch exception, continuing...")

                        # Time
                        try:
                            date_label = page.locator(".date-label-content").first
                            if date_label.is_visible():
                                date_label.click(force=True)
                                time.sleep(1)
                            
                            time_input = page.locator("input[placeholder='Relative quantity']")
                            if time_input.is_visible():
                                time_input.click(force=True); page.keyboard.press("Control+A"); page.keyboard.press("Backspace")
                                page.keyboard.type(str(target_weeks)); page.keyboard.press("Enter"); time.sleep(0.5)
                                
                                apply_btn_xpath = "//input[@placeholder='Relative quantity']/ancestor::div[contains(@class, 'ant-popover')]//button[contains(@class, 'superset-button-primary')]"
                                apply_btn = page.locator(apply_btn_xpath).filter(has_text="APPLY").first
                                if apply_btn.is_visible(): apply_btn.click(force=True); time.sleep(0.5)
                        except Exception as e: print(f" Time setting exception: {e}")

                        # Language
                        try:
                            lang_container = page.locator(".ant-select-selector").nth(3)
                            lang_container.hover()
                            clear_btn = lang_container.locator(".ant-select-clear")
                            if clear_btn.is_visible():
                                clear_btn.click(force=True)
                            else:
                                lang_container.click(force=True)
                                time.sleep(0.2)
                                for _ in range(10): page.keyboard.press("Backspace")
                                page.keyboard.press("Control+A")
                                page.keyboard.press("Backspace")
                                page.locator("body").click()
                            time.sleep(0.5)
                        except Exception as e: print(f" Language cleanup exception: {e}")

                        is_global_settings_set = True

                    # [4] Apply
                    print(" Applying filters...")
                    apply_btn = page.get_by_text("Apply filters").first
                    if apply_btn.is_visible(): apply_btn.click()
                    else: apply_btn.click(force=True)
                    
                    try:
                        page.wait_for_load_state("networkidle", timeout=10000)
                    except: 
                        pass 
                    
                    print(" ⏳ 等待数据加载 (2s)...")
                    time.sleep(3)

                    # [5] Export
                    print(" Exporting...")
                    try:
                        page.mouse.click(100, 100)
                        page.keyboard.press("End")
                    except: pass

                    with page.expect_download(timeout=60000) as download_info:
                        try:
                            three_dots = page.locator(".css-nxtj6w").last
                            if not three_dots.is_visible(): three_dots = page.locator("button[aria-label='More options']").last
                            three_dots.dispatch_event('click')
                            
                            export_locator = page.get_by_text("Export to .CSV", exact=False).first
                            export_locator.wait_for(state="attached", timeout=3000)
                            export_locator.dispatch_event('click')
                        except Exception as export_e:
                            print(f" Export menu click failed: {export_e}")
                            three_dots.click(force=True)
                            time.sleep(0.5)
                            page.keyboard.press("ArrowDown")
                            page.keyboard.press("Enter")
                    
                    download = download_info.value
                    target_file = os.path.join(REPORTING_DIR, f"{country_code}.csv")
                    try:
                        download.save_as(target_file)
                        print(f" Downloaded successfully: {target_file}")
                    except Exception as e: print(f" Save failed: {e}")

                except Exception as e:
                    print(f" CRITICAL ERROR: Failed to process {country_code}: {e}")
                    import traceback
                    traceback.print_exc()
                    print("Execution halted to prevent further errors.")
                    return 

        except Exception as e:
            print(f"Outer process error: {e}")
        finally:
            if browser:
                try: browser.close()
                except: pass

    print("\nAll scraping tasks completed.")

# ================= Main Entry =================
if __name__ == "__main__":
    if getattr(sys, 'frozen', False):
        app_path = os.path.dirname(sys.executable)
    else:
        app_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(app_path)
    
    print(f"Working Directory: {app_path}")

# 1. Extract Time Range from Excel
    print("-" * 30)
    TARGET_WEEKS = 4 # 默认值为4周
    if os.path.exists(MASTER_FILENAME):
        try:
            # 自动读取 Excel 表头，寻找带有 generate data for week 的列
            df_temp = pd.read_excel(MASTER_FILENAME)
            for col in df_temp.columns:
                if "generate data for week" in str(col).lower():
                    # 提取该列第一行的内容
                    val = str(df_temp[col].iloc[0]).replace('nan', '').replace('None', '').strip()
                    
                    # 【核心修复点】用 .split('.')[0] 把 "8.0" 提前切成 "8"，防止小数点丢失变成 80
                    val = val.split('.')[0]
                    
                    # 强行过滤，只保留阿拉伯数字
                    val_num = ''.join(filter(str.isdigit, val))
                    if val_num:
                        TARGET_WEEKS = int(val_num)
                    break
        except Exception:
            pass
    print(f"Data time dimension (weeks): {TARGET_WEEKS}")
    print("-" * 30)

    # 2. Check Master File
    if not os.path.exists(MASTER_FILENAME):
        print(f"ERROR: Missing master file: {MASTER_FILENAME}")
        sys.exit()

    # 3. Launch Chrome
    chrome_process = launch_chrome_with_debug()
    if not chrome_process:
        print("WARNING: Unable to start Chrome. Please check installation path.")
        sys.exit()

# 4. Load Data
    try:
        df = pd.read_excel(MASTER_FILENAME)
        target_col = "Product Line"
        if target_col not in df.columns:
            for c in df.columns:
                if "Parent" in str(c) and "Product" not in str(c):
                    df.rename(columns={c: target_col}, inplace=True)
                    break
            if target_col not in df.columns:
                df.rename(columns={df.columns[0]: target_col}, inplace=True)

        # 强制把整个表格先转为字符串，防止纯空列被当成 float64 导致正则报错
        df = df.astype(str).replace(r'^(nan|None|none|NaN|\s*)$', np.nan, regex=True)

        # ===== Smart autofill for key columns =====
        _autofill_cols = []
        for _af in ["Parent ASIN or Product line", "Product Line", "Country", "Brand name(optional)"]:
            _match = [c for c in df.columns if c.strip().lower() == _af.lower()]
            if _match:
                _autofill_cols.append(_match[0])
        if _autofill_cols:
            _asin_cols = [c for c in df.columns if 'Child ASIN' in str(c) or 'Competitor ASIN' in str(c)]

            _last_data_idx = -1
            if _asin_cols:
                for ac in _asin_cols:
                    _non_empty = df[df[ac].notna() & (df[ac].astype(str).str.strip().str.lower().replace(['nan','none',''], np.nan).notna())].index
                    if not _non_empty.empty:
                        _last_data_idx = max(_last_data_idx, _non_empty.max())

            if _last_data_idx != -1:
                for col in _autofill_cols:
                    df.loc[:_last_data_idx, col] = df.loc[:_last_data_idx, col].ffill()
        # ===== End smart autofill =====

        # ================= [修复版] 深度清洗 ASIN 并解决类型冲突 =================
        for col in df.columns:
            if 'Child ASIN' in str(col) or 'Competitor ASIN' in str(col):
                df[col] = df[col].astype(str).replace(['nan', 'None', 'none', 'NaN'], '')
                df[col] = df[col].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper()
        # ======================================================================

    except Exception as e:
        print(f"Read Excel failed: {e}")
        if chrome_process:
            chrome_process.terminate()
        sys.exit()

    # 5. Run Downloader
    try:
        run_downloader(df, REPORTING_DIR, TARGET_WEEKS)
    except KeyboardInterrupt:
        print("\nInterrupt: Stopped by user")
    finally:
        # 6. Close Browser
        if chrome_process:
            print("Closing browser process...")
            try:
                chrome_process.terminate()
                chrome_process.wait(timeout=2)
            except:
                try: chrome_process.kill()
                except: pass

    # 7. Auto Analysis
    print("\n" + "="*30)
    print("Starting data analysis module...")
    print("="*30)
    try:
        if 'keywords_analysis' in sys.modules:
            keywords_analysis.main()
        else:
            subprocess.run(["python", "keywords_analysis.py"], check=True)
    except Exception as e:
        print(f"ERROR: Analysis module failed: {e}")

    print("\nExiting in 3 seconds...")
    time.sleep(1)