import os
import sys
import time
import re
import sqlite3
import subprocess
import asyncio
import pandas as pd
import random
from typing import Tuple, List, Optional, Dict
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ================= Configuration (Global) =================
CHROME_USER_DATA = r"C:\sel_chrome"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_PLAN_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "Media Plan For Campaign Builder.xlsx"))
ASIN_INFO_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "ASIN_Input_Template For Campaign Builder.xlsx"))
FORMAT_PATH = os.path.join(BASE_DIR, "campaign format.xlsx")

# ================= Module 1: Data Processing =================
class DataProcessor:
    def __init__(self, media_plan_path, asin_info_path, format_path):
        self.media_plan_path = media_plan_path
        self.asin_info_path = asin_info_path
        self.format_path = format_path
        self.conn = sqlite3.connect(':memory:')
        
    def clean_text(self, text, is_asin=False, is_url=False):
        """核心数据清洗模块：保留中文数字英文，ASIN强制大写"""
        if pd.isna(text): return ""
        text = str(text).strip()
        
        if is_url:
            return re.sub(r'\s+', '', text)
            
        cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', text)
        if is_asin: return cleaned.upper() 
        return cleaned 
    
    def process(self):
        print("\n[数据模块] 开始解析 Excel 并构建多品牌命名数据库...")
        for p in [self.media_plan_path, self.asin_info_path, self.format_path]:
            if not os.path.exists(p):
                print(f"❌ 找不到文件: {p}")
                sys.exit(1)
            
        df_asin = pd.read_excel(self.asin_info_path, sheet_name='ASIN information')
        df_asin.columns = [str(c).strip().replace('\n', '').replace('\r', '') for c in df_asin.columns]
        
        column_map = {col.replace(" ", "").lower(): col for col in df_asin.columns}
        brand_col = next((column_map[k] for k in column_map if 'brandname' in k), 'Brand name') 
        url_col = next((column_map[k] for k in column_map if 'urlsite' in k), 'url site')
        pl_col = next((column_map[k] for k in column_map if 'productline' in k or 'parentasin' in k), 'Parent ASIN or Product line')
        country_col = next((column_map[k] for k in column_map if 'country' in k), 'Country')

        df_asin = df_asin.rename(columns={
            brand_col: 'Brand name',
            url_col: 'url site',
            pl_col: 'Parent ASIN or Product line',
            country_col: 'Country'
        })

        cols_to_fill = ['Parent ASIN or Product line', 'Country', 'Brand name', 'url site']
        for col in cols_to_fill:
            if col in df_asin.columns:
                df_asin[col] = df_asin[col].replace(r'^\s*$', pd.NA, regex=True).ffill()
                
        df_asin['Parent ASIN or Product line'] = df_asin['Parent ASIN or Product line'].apply(lambda x: str(x).strip() if pd.notna(x) else "")
        if 'Child ASIN' in df_asin.columns:
            df_asin['Child ASIN'] = df_asin['Child ASIN'].apply(lambda x: self.clean_text(x, is_asin=True))
        df_asin['Country'] = df_asin['Country'].apply(self.clean_text)
        df_asin['Brand name'] = df_asin['Brand name'].apply(self.clean_text)
        df_asin['url site'] = df_asin['url site'].apply(lambda x: self.clean_text(x, is_url=True))

        df_asin.to_sql('ASIN_information', self.conn, index=False, if_exists='replace')
        
        df_format = pd.read_excel(self.format_path)
        df_format.columns = [str(c).strip() for c in df_format.columns]
        df_format.to_sql('campaign_format', self.conn, index=False, if_exists='replace')
        
        media_plan_data = []
        unique_brands = df_asin['Brand name'].replace('', pd.NA).dropna().unique()
        
        if len(unique_brands) == 0:
            raise Exception("❌ ASIN information 表中未发现有效的 Brand name！")

        all_sheets = pd.ExcelFile(self.media_plan_path).sheet_names
        
        for brand in unique_brands:
            actual_sheet = next((s for s in all_sheets if self.clean_text(s) == brand), None)
            if not actual_sheet: continue
                
            df_brand_plan = pd.read_excel(self.media_plan_path, sheet_name=actual_sheet)
            product_line_cols = df_brand_plan.columns[7:] 
            
            for pl_col in product_line_cols:
                raw_pl_col = str(pl_col).strip()
                
                for _, row in df_brand_plan.iterrows():
                    base_name = str(row.iloc[0]).strip()
                    if base_name.lower() == 'nan' or not base_name: continue
                    
                    if str(row[pl_col]).strip().upper() == 'Y':
                        media_plan_data.append({
                            'Brand': brand,
                            'Product_line': raw_pl_col,
                            'Base_campaign_name': base_name
                        })
        df_media_plan = pd.DataFrame(media_plan_data)
        
        pl_to_country = df_asin.dropna(subset=['Country']).set_index('Parent ASIN or Product line')['Country'].to_dict()
        df_media_plan['Country'] = df_media_plan['Product_line'].map(pl_to_country)
        df_media_plan = df_media_plan.dropna(subset=['Country'])
        
        return df_media_plan, df_asin, df_format

# ================= Module 2: Async Namer with Task Queue =================
class AsyncAmazonAutoNamerBot:
    def __init__(self, df_media_plan, df_asin, df_format, concurrency=3):
        self.concurrency = concurrency
        self.playwright = None
        self.browser = None
        self.headless_browser = None
        self.chrome_path = None
        self.context = None
        self.login_cookies = None
        
        self.df_media_plan = df_media_plan
        self.df_asin = df_asin
        self.df_format = df_format
        
        self.tasks_queue = asyncio.Queue()  # ✨ 任务队列：工人从这里取任务
        self.naming_tracker = {}
        self.failed_asins = {}
        self.completed_tasks = 0
        self.total_tasks = 0
        
        # 生成所有任务并加入队列
        self._generate_task_packages()

    def _generate_task_packages(self):
        """🚀 生成任务包裹：按 URL + Country + AdType 维度
        
        关键：同一个URL+Country下的多个Brand共用一个卡片，所以应该合并为一个包裹
        """
        print("\n[任务生成] 按 URL + Country + AdType 维度打包任务...")
        
        unique_urls = self.df_asin.dropna(subset=['url site']).drop_duplicates(subset=['url site'])['url site'].tolist()
        
        for url in unique_urls:
            # 获取该URL下的所有国家
            countries_for_url = self.df_asin[self.df_asin['url site'] == url]['Country'].dropna().unique().tolist()
            
            for country in countries_for_url:
                # 获取该URL+Country下的所有Brand
                brands_for_url_country = self.df_asin[
                    (self.df_asin['url site'] == url) & 
                    (self.df_asin['Country'] == country)
                ]['Brand name'].unique().tolist()
                
                # 该URL+Country下的所有Brand的媒体计划合并
                combined_plans = []
                for brand in brands_for_url_country:
                    brand_plan = self.df_media_plan[
                        (self.df_media_plan['Brand'] == brand) & 
                        (self.df_media_plan['Country'] == country)
                    ]
                    if not brand_plan.empty:
                        combined_plans.append(brand_plan)
                
                # 如果该URL+Country组合没有任何有效的媒体计划，跳过
                if not combined_plans:
                    continue
                
                merged_campaigns = pd.concat(combined_plans, ignore_index=True)
                
                # 为每个 AdType (SB, SD, SP) 生成独立的任务包裹
                for ad_type in ['SB', 'SD', 'SP']:
                    task = {
                        'url': url,
                        'brands': brands_for_url_country,  # 以列表形式存储所有Brand
                        'country': country,
                        'ad_type': ad_type,
                        'campaigns': merged_campaigns  # 合并后的媒体计划
                    }
                    self.total_tasks += 1
        
        print(f"✅ 任务生成完成！共生成 {self.total_tasks} 个包裹")
        if self.total_tasks > 0:
            unique_urls_count = len(unique_urls)
            print(f"   • URL数量: {unique_urls_count}")
            print(f"   • 广告类型: 3个 (SB/SD/SP)")
            print(f"   • 计算公式: {unique_urls_count} URL × (各URL下的Country数) × 3 AdType = {self.total_tasks} 包裹")
            print(f"   • 每个URL有3个广告类型 (SB/SD/SP)")
            print(f"   • 总包裹数 = {self.total_tasks}")

    def _clean_chrome_cache(self):
        """🧹 清理Chrome缓存，但保留Cookies和登录信息"""
        print("\n[清理] 清理Chrome缓存（保留Cookies）...")
        
        cache_dirs = [
            os.path.join(CHROME_USER_DATA, 'Default', 'Cache'),
            os.path.join(CHROME_USER_DATA, 'Default', 'Code Cache'),
            os.path.join(CHROME_USER_DATA, 'Default', 'Service Worker'),
            os.path.join(CHROME_USER_DATA, 'Default', 'Cache.lock'),
        ]
        
        for cache_path in cache_dirs:
            try:
                if os.path.isdir(cache_path):
                    import shutil
                    shutil.rmtree(cache_path, ignore_errors=True)
                    print(f"  ✅ 已清理: {cache_path}")
                elif os.path.isfile(cache_path):
                    os.remove(cache_path)
                    print(f"  ✅ 已删除: {cache_path}")
            except Exception as e:
                print(f"  ⚠️ 清理失败 {cache_path}: {e}")

    def start_local_chrome(self):
        """启动Chrome浏览器，启动前自动清理缓存"""
        # 🔑 关键：启动前清理缓存，但保留Cookies
        self._clean_chrome_cache()
        
        possible_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe") 
        ]
        self.chrome_path = next((p for p in possible_paths if os.path.exists(p)), None)
        if not self.chrome_path: raise FileNotFoundError("ERROR: Chrome not found.")
        
        print(f"\n[系统] 启动本地 Chrome...")
        cmd = [self.chrome_path, "--remote-debugging-port=9225", f"--user-data-dir={CHROME_USER_DATA}"]
        subprocess.Popen(cmd)
        time.sleep(3)

    async def _clean_page_storage(self, page):
        """🧹 清理Page的本地存储（不影响Cookies）"""
        try:
            # 清理localStorage和sessionStorage
            await page.evaluate("""() => {
                try { localStorage.clear(); console.log('✅ localStorage已清理'); } catch(e) {}
                try { sessionStorage.clear(); console.log('✅ sessionStorage已清理'); } catch(e) {}
            }""")
            print("  ✅ 已清理Page存储（localStorage/sessionStorage保留Cookies）")
        except Exception as e:
            print(f"  ⚠️ 清理Page存储失败: {e}")

    async def vanguard_login(self):
        """🤖 1号先锋：验证環境"""
        self.playwright = await async_playwright().start()
        print("[1号先锋] 正在探测 Chrome 调试端口...")
        from chrome_port_finder import get_cdp_url
        cdp_url = get_cdp_url()
        self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url)
        self.context = self.browser.contexts[0]
        
        vanguard_page = await self.context.new_page()
        await vanguard_page.set_viewport_size({"width": 1920, "height": 10000})
        
        sample_url = self.df_asin.dropna(subset=['url site'])['url site'].iloc[0] if len(self.df_asin) > 0 else "https://advertising.amazon.com/"
        print(f"[1号先锋] 正在校验环境: {sample_url}")
        await vanguard_page.goto(sample_url, timeout=60000)
        
        try:
            await vanguard_page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=25000)
        except PlaywrightTimeoutError:
            print("  -> ⏸️ 环境未就绪！如果需要验证码，请在浏览器中手动完成...")
            await vanguard_page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=0)
            
        self.login_cookies = await self.context.cookies()
        print(f"✅ 环境验证通过！已提取 {len(self.login_cookies)} 条登录凭证，分配给 {self.concurrency} 个并发工人...")
        await vanguard_page.close()

    def _filter_authorized_names(self, potential_names, product_line, df_plan, targeting_direction=""):
        """
        ✅ 原汁原味的同步版本逻辑：在Media Plan中查找并验证授权名字
        """
        authorized_names = []
        for name in potential_names:
            plan_match = df_plan[(df_plan['Product_line'] == product_line) & (df_plan['Base_campaign_name'] == name)]
            if not plan_match.empty:
                authorized_names.append(name)
        
        return authorized_names
    def dehydrate(self, s):
        """脱水处理：用于字符串匹配"""
        if pd.isna(s) or s is None: return ""
        s_str = str(s).strip()
        if s_str.lower() in ['nan', 'none', '']: return ""
        return re.sub(r'[^a-zA-Z0-9]', '', s_str).lower()

    def find_product_line_by_asin(self, target_asin: str, current_country: str) -> Optional[str]:
        """通过ASIN和国家查找产品线"""
        target_asin = str(target_asin).strip().upper() 
        match = self.df_asin[
            (self.df_asin['Child ASIN'].astype(str).str.strip().str.upper() == target_asin) &
            (self.df_asin['Country'].apply(self.dehydrate) == self.dehydrate(current_country))
        ]
        if not match.empty:
            unique_pls = match['Parent ASIN or Product line'].dropna().unique().tolist()
            if unique_pls: return str(unique_pls[0])
        return None

    async def navigate_and_switch_country(self, page, task, worker_id):
        """🚀 导航并切换国家"""
        target_url = task['url']
        target_country = task['country']
        
        print(f"[{worker_id}] 🌐 准备前往目标站点 [{target_country}]...")
        if not target_url or str(target_url).strip() == "" or str(target_url).lower() == "nan":
            raise Exception("❌ URL为空！请检查 Excel。")

        # 按工人编号错峰进入网址，避免并发同时打到同一页面
        worker_match = re.search(r"\d+", str(worker_id))
        worker_seq = int(worker_match.group()) if worker_match else 1
        stagger_seconds = min((worker_seq - 1) * 2.0, 12.0) + random.uniform(0.4, 1.2)
        print(f"[{worker_id}] ⏱️ 进入网址前错峰等待 {stagger_seconds:.1f}s...")
        await asyncio.sleep(stagger_seconds)

        # 分层导航：先 load，超时后回退到 domcontentloaded，避免单次 60s 超时直接崩溃
        max_goto_retries = 3
        goto_ok = False
        for retry in range(max_goto_retries):
            try:
                await page.goto(target_url, timeout=60000, wait_until="load")
                goto_ok = True
                break
            except PlaywrightTimeoutError:
                print(f"[{worker_id}] ⚠️ 第 {retry + 1}/{max_goto_retries} 次 load 超时，尝试 domcontentloaded 回退...")
                try:
                    await page.goto(target_url, timeout=45000, wait_until="domcontentloaded")
                    goto_ok = True
                    break
                except Exception:
                    if retry < max_goto_retries - 1:
                        print(f"[{worker_id}] 🔄 导航回退仍失败，准备重试（先跳转空白页释放挂起请求）...")
                        try:
                            await page.goto("about:blank", timeout=15000, wait_until="domcontentloaded")
                        except Exception:
                            pass
                        await page.wait_for_timeout(1200)
                    else:
                        raise
            except Exception:
                if retry < max_goto_retries - 1:
                    print(f"[{worker_id}] ⚠️ 链接加载失败，1.2秒后重试...")
                    await page.wait_for_timeout(1200)
                else:
                    raise

        if not goto_ok:
            raise Exception(f"导航失败: {target_url}")
        
        # 核心改进：等待国家选择器出现（表示基础页面已加载）
        try:
            await page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=30000)
        except PlaywrightTimeoutError:
            print(f"[{worker_id}] ⚠️ 国家切换器未出现，尝试刷新恢复页面...")
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=30000)
        
        country_mapping = {
            "ES": "Spain", "SPAIN": "Spain", "UK": "United Kingdom", "GB": "United Kingdom",
            "US": "United States", "DE": "Germany", "FR": "France", "IT": "Italy",
            "JP": "Japan", "CA": "Canada", "MX": "Mexico", "AU": "Australia"
        }
        ui_country_name = country_mapping.get(target_country.strip().upper(), target_country)
        country_selector = page.locator('[data-takt-id="header_marketplace_switcher"]')
        current_displayed = await country_selector.inner_text()
        
        if ui_country_name.lower() not in current_displayed.lower():
            print(f"[{worker_id}] 🔄 正在切换国家至 [{ui_country_name}]...")
            await country_selector.click(force=True)
            await page.wait_for_timeout(500) 
            
            country_option = page.locator(f'div:text-is("{ui_country_name}"), span:text-is("{ui_country_name}")').first
            if await country_option.is_visible():
                await country_option.click(force=True)
                await page.wait_for_timeout(300)
                confirm_btn = page.locator('#aac-chrome-change-country-button, button[data-takt-id="storm-ui-country-selector-footer-apply-button"]')
                if await confirm_btn.is_visible(): 
                    await confirm_btn.click(force=True)
                    print(f"[{worker_id}] -> 已点击确认按钮，等待页面重新加载...")
                    
                    # === 核心改进：像创建代码一样，等待确认页面完全加载的信号 ===
                    # 这里不能直接去找 Ready to publish，必须先确保主页面已经加载完成
                    print(f"[{worker_id}] -> 等待页面完全加载，监控'创建'按钮...")
                    try:
                        # 等待"创建"按钮出现，表示页面已完全加载（和创建代码的逻辑一致）
                        await page.wait_for_selector(
                            'button:has-text("Create customized new campaigns"), button:has-text("Create new campaign")',
                            state="visible",
                            timeout=30000
                        )
                        print(f"[{worker_id}] ✅ 页面完全加载！")
                    except:
                        print(f"[{worker_id}] ⚠️ 未找到'创建'按钮，但继续前进（可能页面结构不同）")
                        await page.wait_for_timeout(3000)
                    
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    print(f"[{worker_id}] ✅ 跨国切换成功！")
            else: 
                raise Exception(f"找不到国家: '{ui_country_name}'")
        else:
            print(f"[{worker_id}] ✅ 已在目标国家，无需切换。")
            # 即使不需要切换，也要等待页面完全加载
            try:
                await page.wait_for_selector(
                    'button:has-text("Create customized new campaigns"), button:has-text("Create new campaign")',
                    state="visible",
                    timeout=30000
                )
            except:
                print(f"[{worker_id}] ⚠️ 页面加载信号不明确，继续前进")
                await page.wait_for_timeout(2000)
            print(f"[{worker_id}] ✅ 页面已就绪")
        
        # 🧹 关键：页面加载完成后立即清理存储，保证系统流畅
        await self._clean_page_storage(page)

    # ================= 完全复用原文件的命名逻辑 =================
    async def analyze_sp_popup(self, page, row_locator, targeting_type: str):
        """提取SP的ASIN和targets"""
        try:
            await row_locator.locator("[col-id='numberOfAsinGroups'] button").click(force=True)
            await page.wait_for_timeout(1500)  # 增加等待时间，给popup充分时间渲染
            
            if targeting_type == "Auto":
                product_tab = page.get_by_text("Product", exact=True).first
                try:
                    await product_tab.wait_for(state="visible", timeout=3000)
                    await product_tab.click()
                    await page.wait_for_timeout(1200)  # 增加等待时间
                except: pass
            
            # 增加ASIN等待时间到10秒，并添加重试机制
            asins = []
            max_asin_retries = 3
            for asin_attempt in range(max_asin_retries):
                try:
                    await page.wait_for_selector("p:has-text('ASIN:')", state="visible", timeout=10000)
                    asins_text = await page.locator("p:has-text('ASIN:')").all_inner_texts()
                    asins = [t.replace("ASIN:", "").strip() for t in asins_text if "ASIN:" in t]
                    if asins:  # 如果成功获取到ASIN就停止重试
                        break
                except:
                    if asin_attempt < max_asin_retries - 1:
                        await page.wait_for_timeout(500)  # 稍微等待后重试
                    continue
            
            targets = "Unknown"
            if targeting_type == "Auto": targets = "All Match Type"
            else:
                kw_tab = page.get_by_role("tab", name=re.compile(r"^Keyword$", re.IGNORECASE)).first
                if await kw_tab.is_visible():
                    await kw_tab.click()
                    await page.wait_for_timeout(500)
                    match_el = page.locator('[data-e2e-id="tactical-recommendations-table:tactical-recommendations-table:cell-matchType:edit"]').first
                    if await match_el.is_visible():
                        raw_m = await match_el.get_attribute("title") or ""
                        if "Broad" in raw_m: targets = "Broad Match Type"
                        elif "Phrase" in raw_m: targets = "Phrase Match Type"
                        elif "Exact" in raw_m: targets = "Exact Match Type"
            return targets, asins
        except Exception: return "Unknown", []
        finally:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

    async def analyze_sb_popup(self, page, row_locator, targeting_type: str):
        """提取SB的ASIN, landing page和targets"""
        try:
            await row_locator.locator("[col-id='numberOfAsinGroups'] button").first.click(force=True)
            await page.wait_for_timeout(1500)  # 增加等待时间
            
            lp_tab = page.locator('button#LANDING_PAGE').first
            await lp_tab.wait_for(state="visible", timeout=5000)
            await lp_tab.click()
            await page.wait_for_timeout(1000)  # 增加等待时间
            
            raw_lp_type = await page.locator("h5:has-text('Landing page type:')").locator("..").locator("~ div > p").first.inner_text()
            lp_type = "Home page" if "store" in raw_lp_type.lower() or "home page" in raw_lp_type.lower() else raw_lp_type
            
            asins = []
            if lp_type == "Home page":
                await page.locator('button#CREATIVE').first.click()
                await page.wait_for_timeout(1000)  # 增加等待时间
            
            # 增加ASIN等待时间到10秒，并添加重试机制
            max_asin_retries = 3
            for asin_attempt in range(max_asin_retries):
                try:
                    await page.wait_for_selector("p:has-text('ASIN:')", state="visible", timeout=10000)
                    asins_text = await page.locator("p:has-text('ASIN:')").all_inner_texts()
                    asins = [t.replace("ASIN:", "").strip() for t in asins_text if "ASIN:" in t]
                    if asins:  # 如果成功获取到ASIN就停止重试
                        break
                except:
                    if asin_attempt < max_asin_retries - 1:
                        await page.wait_for_timeout(500)  # 稍微等待后重试
                    continue

            targets = "Unknown"
            if targeting_type == "Keyword":
                kw_tab = page.get_by_role("tab", name=re.compile(r"^Keyword$", re.IGNORECASE)).first
                if await kw_tab.is_visible():
                    await kw_tab.click()
                    await page.wait_for_timeout(500) 
                    match_el = page.locator('[data-e2e-id="tactical-recommendations-table:tactical-recommendations-table:cell-matchType:edit"]').first
                    if await match_el.is_visible():
                        raw_m = await match_el.get_attribute("title") or ""
                        if "Broad" in raw_m: targets = "Broad Match Type"
                        elif "Phrase" in raw_m: targets = "Phrase Match Type"
                        elif "Exact" in raw_m: targets = "Exact Match Type"
            elif targeting_type == "Product":
                cat_tab = page.locator('p:has-text("Target categories")').first
                if await cat_tab.is_visible():
                    await cat_tab.click()
                    await page.wait_for_timeout(600)
                    cell_count = await page.locator('[data-e2e-id="tactical-recommendations-table:tactical-recommendations-table:cell-name:edit"]').count()
                    targets = "Target Categories" if cell_count > 0 else "Target product"
                else: targets = "Target product"
            return targets, asins, lp_type
        except Exception: return "Unknown", [], "Unknown"
        finally:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

    async def analyze_sd_popup(self, page, row_locator, targeting_type: str):
        """提取SD的ASIN、targets和定向类型（竞品vs本品）"""
        try:
            await row_locator.locator("[col-id='numberOfAsinGroups'] button").click(force=True)
            await page.wait_for_timeout(1500)
            
            # 增加ASIN等待时间到10秒，并添加重试机制
            asins = []
            max_asin_retries = 3
            for asin_attempt in range(max_asin_retries):
                try:
                    await page.wait_for_selector("p:has-text('ASIN:')", state="visible", timeout=10000)
                    asins_text = await page.locator("p:has-text('ASIN:')").all_inner_texts()
                    asins = [t.replace("ASIN:", "").strip() for t in asins_text if "ASIN:" in t]
                    if asins:
                        break
                except:
                    if asin_attempt < max_asin_retries - 1:
                        await page.wait_for_timeout(500)
                    continue
            
            # 🔑 关键：检查是否有"Target products"列 → 有=竞品定向，无=本品定向
            has_product_column = False
            try:
                target_products_tab = page.get_by_text(re.compile(r"^Target products?$", re.IGNORECASE))
                if await target_products_tab.is_visible():
                    has_product_column = True
                    targeting_direction = "竞品ASIN定向"
                else:
                    targeting_direction = "本品ASIN定向"
            except:
                targeting_direction = "本品ASIN定向"
            
            targets = "Unknown"
            if "Audience" in targeting_type: 
                targets = "Target Categories"
            else:
                target_cat_tab = page.get_by_text(re.compile(r"^Target categories$", re.IGNORECASE))
                if await target_cat_tab.is_visible():
                    await target_cat_tab.click()
                    await page.wait_for_timeout(500)
                    cell_count = await page.locator('[data-e2e-id="tactical-recommendations-table:tactical-recommendations-table:cell-name:edit"]').count()
                    targets = "Target Categories" if cell_count > 0 else "Target product"
                else: 
                    targets = "Target product"
            
            return targets, asins, targeting_direction  # ✨ 现在返回3个值，包括定向类型
        except Exception: 
            return "Unknown", [], "本品ASIN定向"  # 默认本品
        finally:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)

    async def _run_naming_engine(self, page, task, worker_id, ad_type):
        """🚀 核心命名引擎：完全复用原文件逻辑"""
        idx, retry_count = 0, 0
        scroll_recovery_mode = False
        current_country = task['country']
        df_plan = task['campaigns']
        current_page = 1
        total_results = 0
        enable_pagination = False
        status_selector = 'p#tactical-recommendations-table\\:pagination-page-status'
        next_btn_selector = 'button[id="tactical-recommendations-table:pagination-next"]'

        async def _read_pagination_status_text() -> str:
            """读取分页状态文本，如: 1-100 of 184 results"""
            try:
                status_el = page.locator(status_selector).first
                if await status_el.count() == 0:
                    return ""
                text = (await status_el.inner_text() or "").strip()
                return text
            except Exception:
                return ""

        async def _read_next_button_state() -> Dict[str, str]:
            """读取 next 按钮关键属性，便于翻页前后对比"""
            state = {
                "disabled": "",
                "aria_disabled": "",
                "class": "",
            }
            try:
                btn = page.locator(next_btn_selector).first
                if await btn.count() == 0:
                    return state
                disabled_attr = await btn.get_attribute("disabled")
                aria_disabled_attr = await btn.get_attribute("aria-disabled")
                class_attr = await btn.get_attribute("class")
                state["disabled"] = "present" if disabled_attr is not None else "absent"
                state["aria_disabled"] = (aria_disabled_attr or "").strip()
                state["class"] = (class_attr or "").strip()
            except Exception:
                pass
            return state

        async def _wait_for_page_status_transition(old_status: str, timeout_ms: int = 12000) -> str:
            """等待分页状态从旧值变更，返回新状态文本"""
            deadline = time.time() + (timeout_ms / 1000)
            while time.time() < deadline:
                new_status = await _read_pagination_status_text()
                if new_status and new_status != old_status:
                    return new_status
                await page.wait_for_timeout(250)
            return await _read_pagination_status_text()

        def _parse_range(status_text: str) -> Optional[Tuple[int, int, int]]:
            """解析分页文本中的 start/end/total"""
            if not status_text:
                return None
            m = re.search(r'(\d+)\s*-\s*(\d+)\s+of\s+(\d+)\s+results', status_text, flags=re.IGNORECASE)
            if not m:
                return None
            return int(m.group(1)), int(m.group(2)), int(m.group(3))

        async def _wait_for_row_visible_by_index(row_index: int, timeout_ms: int = 8000) -> bool:
            """等待指定 row-index 可见，用于翻页后校验是否回到新页首行"""
            try:
                row_locator = page.locator(f".ag-center-cols-container > .ag-row[row-index='{row_index}']").first
                await row_locator.wait_for(state="visible", timeout=timeout_ms)
                return True
            except Exception:
                return False

        async def _scroll_grid_to_top() -> None:
            """将表格滚动到顶部，避免翻页后停在中间导致首行不可见"""
            try:
                await page.evaluate("""() => {
                    const viewport = document.querySelector('.ag-body-viewport');
                    if (viewport) viewport.scrollTop = 0;
                }""")
            except Exception:
                pass
        
        # 🧹 在开始处理此AD Type前清理一次存储，确保系统资源充足
        print(f"[{worker_id}] 🧹 准备处理 {ad_type}，先进行系统清理...")
        await self._clean_page_storage(page)

        # 读取分页状态，例如："1-100 of 184 results"
        try:
            status_text = await _read_pagination_status_text()
            parsed = _parse_range(status_text)
            if parsed:
                _start, _end, total_results = parsed
                enable_pagination = total_results > 100
                print(f"[{worker_id}] 📊 分页状态初始值: {status_text}")
                print(f"[{worker_id}] 📊 检测到结果总数: {total_results}")
                if enable_pagination:
                    print(f"[{worker_id}] 🔄 结果数大于100，触底时将自动翻页继续处理")
            elif status_text:
                print(f"[{worker_id}] ⚠️ 分页状态存在但无法解析: {status_text}")
        except Exception as _pg_err:
            print(f"[{worker_id}] ⚠️ 读取分页状态失败，按单页模式继续: {_pg_err}")
        
        while True:
            row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{idx}']").first
            if not await row.is_visible():
                visible_rows = page.locator(".ag-center-cols-container > .ag-row")
                if await visible_rows.count() > 0:
                    await visible_rows.last.scroll_into_view_if_needed()
                    await page.mouse.wheel(0, 800) 
                    await page.wait_for_timeout(800 if scroll_recovery_mode else 1500)
                
                if not await row.is_visible():
                    retry_count += 1
                    max_retries = 30 if scroll_recovery_mode else 2
                    if retry_count >= max_retries:
                        # 触底后，如结果总数 > 100，则尝试翻页继续处理
                        if enable_pagination:
                            try:
                                next_btn = page.locator(next_btn_selector).first
                                if await next_btn.count() > 0 and not await next_btn.is_disabled():
                                    before_status = await _read_pagination_status_text()
                                    before_btn_state = await _read_next_button_state()
                                    print(f"[{worker_id}] 📄 第 {current_page} 页触底，尝试翻页")
                                    print(
                                        f"[{worker_id}] ↪ 翻页前 status={before_status or 'N/A'} | "
                                        f"next.disabled={before_btn_state['disabled']} | "
                                        f"next.aria-disabled={before_btn_state['aria_disabled'] or 'N/A'} | "
                                        f"next.class={before_btn_state['class'] or 'N/A'}"
                                    )

                                    await next_btn.scroll_into_view_if_needed()
                                    await next_btn.click(force=True)

                                    # 先等分页状态变化，再等表格首行可见
                                    after_status = await _wait_for_page_status_transition(before_status, timeout_ms=12000)
                                    after_range = _parse_range(after_status)
                                    expected_start_row_index = (after_range[0] - 1) if after_range else None

                                    await _scroll_grid_to_top()
                                    if expected_start_row_index is not None:
                                        first_row_ready = await _wait_for_row_visible_by_index(expected_start_row_index, timeout_ms=8000)
                                        if not first_row_ready:
                                            raise Exception(
                                                f"翻页后未定位到新页首行 row-index={expected_start_row_index} "
                                                f"(status={after_status or 'N/A'})"
                                            )
                                        idx = expected_start_row_index
                                    else:
                                        # 状态文本无法解析时退化为等待任意行，并保守从0继续
                                        await page.wait_for_selector(
                                            ".ag-center-cols-container > .ag-row",
                                            state="visible",
                                            timeout=8000,
                                        )
                                        idx = 0

                                    after_btn_state = await _read_next_button_state()

                                    print(
                                        f"[{worker_id}] ↪ 翻页后 status={after_status or 'N/A'} | "
                                        f"next.disabled={after_btn_state['disabled']} | "
                                        f"next.aria-disabled={after_btn_state['aria_disabled'] or 'N/A'} | "
                                        f"next.class={after_btn_state['class'] or 'N/A'}"
                                    )

                                    # 强校验：确认从 1-100 跳到 101-200（或至少起始值增长）
                                    before_range = _parse_range(before_status)
                                    if before_range and after_range:
                                        b_start, b_end, b_total = before_range
                                        a_start, a_end, a_total = after_range
                                        if a_total == b_total and a_start > b_start:
                                            print(
                                                f"[{worker_id}] ✅ 分页推进成功: "
                                                f"{b_start}-{b_end} of {b_total} -> {a_start}-{a_end} of {a_total}"
                                            )
                                        else:
                                            print(
                                                f"[{worker_id}] ⚠️ 分页状态未按预期推进: "
                                                f"{before_status or 'N/A'} -> {after_status or 'N/A'}"
                                            )
                                    else:
                                        print(
                                            f"[{worker_id}] ⚠️ 无法解析翻页前后状态: "
                                            f"{before_status or 'N/A'} -> {after_status or 'N/A'}"
                                        )

                                    current_page += 1
                                    retry_count = 0
                                    scroll_recovery_mode = False
                                    continue
                            except Exception as _next_err:
                                print(f"[{worker_id}] ⚠️ 翻页失败，结束当前类型处理: {_next_err}")

                        print(f"[{worker_id}] ✅ {ad_type} 列表触底，最终停在第 {current_page} 页，共处理 {idx} 行！")
                        break
                    continue
                else: 
                    retry_count = 0
                    scroll_recovery_mode = False
            else: 
                retry_count = 0
                scroll_recovery_mode = False

            print(f"[{worker_id}] --- 处理 {ad_type} 行 {idx + 1} ---")
            try:
                await row.scroll_into_view_if_needed()
                
                # ============= 完全复用原文件的命名逻辑 =============
                if ad_type == 'SP':
                    targeting_raw = await row.locator("[col-id='campaignTargetingType'] .cell-renderer-content-text").inner_text(timeout=2000)
                    targeting_type = "Auto" if "Automatic" in targeting_raw else "Keyword"
                    targets, asins = await self.analyze_sp_popup(page, row, targeting_type)
                    if not asins: raise ValueError("未抓取到 ASIN")

                    product_line = self.find_product_line_by_asin(asins[0], current_country)
                    if not product_line: raise ValueError(f"未找到 ASIN {asins[0]} 的产品线")

                    c_targeting = self.dehydrate(targeting_type)
                    c_targets = self.dehydrate(targets)
                    
                    df_clean = self.df_format.copy()
                    df_clean['c_ad_prod'] = df_clean['Ad product'].apply(self.dehydrate)
                    df_clean['c_tgt_type'] = df_clean['Targeting'].apply(self.dehydrate)
                    df_clean['c_tgts'] = df_clean['Targets'].apply(self.dehydrate)

                    matched_formats = self.df_format[
                        (df_clean['c_ad_prod'].str.contains('sponsoredproduct', na=False)) &
                        (df_clean['c_tgt_type'].str.contains(c_targeting, na=False) | df_clean['c_tgt_type'].eq(c_targeting)) &
                        (df_clean['c_tgts'].str.contains(c_targets, na=False) | df_clean['c_tgts'].eq(c_targets))
                    ]
                    if matched_formats.empty: raise ValueError("Format 表无匹配规则")
                    
                    name_cols = ['Campaign Name', 'Campaign Name 2', 'Campaign Name 3', 'Campaign Name 4', 'Campaign Name 5', 'Campaign Name 6']
                    format_row = matched_formats.iloc[0]
                    potential_names = []
                    for col in name_cols:
                        val = format_row.get(col)
                        if pd.notna(val) and str(val).strip():
                            potential_names.append(str(val).strip())
                    
                    if not potential_names: raise ValueError("Format 表中该规则未配置任何命名")

                    # 使用统一的过滤函数从Media Plan验证授权命名
                    authorized_names = self._filter_authorized_names(potential_names, product_line, df_plan, targeting_direction="")
                    
                    if not authorized_names: raise ValueError("该规则的1~6列命名均未在 Excel 中打 Y")
                    
                    # 🔑 关键：用potential_names[0]作为tracker键（稳定标识），循环authorized_names中的值
                    base_name = potential_names[0]
                    if product_line not in self.naming_tracker: self.naming_tracker[product_line] = {}
                    if base_name not in self.naming_tracker[product_line]: self.naming_tracker[product_line][base_name] = 0
                    usage_count = self.naming_tracker[product_line][base_name]

                    if usage_count >= len(authorized_names):
                        raise ValueError(f"命名名额已用完（可用 {len(authorized_names)} 个），跳过以避免重复命名")
                    target_template = authorized_names[usage_count]
                    final_name = str(target_template).replace("Productline", str(product_line))
                    self.naming_tracker[product_line][base_name] += 1

                elif ad_type == 'SB':
                    targeting_raw = await row.locator("[col-id='campaignTargetingType'] .cell-renderer-sub-content p").inner_text(timeout=2000)
                    targeting_type = "Keyword" if "Keyword" in targeting_raw else "Product"
                    ad_format = await row.locator('[data-e2e-id="tactical-recommendations-table:cell-campaignFormat:edit"]').first.get_attribute("title") or "Unknown"
                    
                    targets, asins, lp_type = await self.analyze_sb_popup(page, row, targeting_type)
                    if not asins: raise ValueError("未抓取到 ASIN")

                    product_line = self.find_product_line_by_asin(asins[0], current_country)
                    if not product_line: raise ValueError(f"未找到 ASIN {asins[0]} 的产品线")

                    # 🔑 翻译官：将页面的值映射到Format表的对应值
                    format_mapping = {self.dehydrate("Product collection"): self.dehydrate("Product collection"), self.dehydrate("Video"): self.dehydrate("Video")}
                    lp_mapping = {self.dehydrate("Product list"): self.dehydrate("Product list"), self.dehydrate("Home page"): self.dehydrate("Home page"), self.dehydrate("Detail page"): self.dehydrate("Product detailed page")}
                    target_mapping = {self.dehydrate("Target Categories"): "category", self.dehydrate("Target product"): "product", self.dehydrate("Broad Match Type"): "broad", self.dehydrate("Phrase Match Type"): "phrase", self.dehydrate("Exact Match Type"): "exact"}
                    
                    c_format = format_mapping.get(self.dehydrate(ad_format), self.dehydrate(ad_format))
                    c_lp = lp_mapping.get(self.dehydrate(lp_type), self.dehydrate(lp_type))
                    c_targeting = self.dehydrate(targeting_type)
                    c_targets = target_mapping.get(self.dehydrate(targets), self.dehydrate(targets))

                    df_clean = self.df_format.copy()
                    df_clean['c_ad_prod'] = df_clean['Ad product'].apply(self.dehydrate)
                    df_clean['c_format'] = df_clean['Ad Format'].apply(self.dehydrate)
                    df_clean['c_lp'] = df_clean['Landing Page'].apply(self.dehydrate)
                    df_clean['c_tgt_type'] = df_clean['Targeting'].apply(self.dehydrate)
                    df_clean['c_tgts'] = df_clean['Targets'].apply(self.dehydrate)

                    matched_formats = self.df_format[
                        (df_clean['c_ad_prod'].str.contains('sponsoredbrand', na=False)) &
                        (df_clean['c_format'].str.contains(c_format, na=False) | df_clean['c_format'].eq(c_format)) &
                        (df_clean['c_lp'].str.contains(c_lp, na=False) | df_clean['c_lp'].eq(c_lp)) &
                        (df_clean['c_tgt_type'].str.contains(c_targeting, na=False) | df_clean['c_tgt_type'].eq(c_targeting)) &
                        (df_clean['c_tgts'].str.contains(c_targets, na=False) | df_clean['c_tgts'].eq(c_targets))
                    ]
                    if matched_formats.empty: raise ValueError("Format 表无匹配规则")
                    
                    name_cols = ['Campaign Name', 'Campaign Name 2', 'Campaign Name 3', 'Campaign Name 4', 'Campaign Name 5', 'Campaign Name 6']
                    format_row = matched_formats.iloc[0]
                    potential_names = []
                    for col in name_cols:
                        val = format_row.get(col)
                        if pd.notna(val) and str(val).strip():
                            potential_names.append(str(val).strip())
                    
                    if not potential_names: raise ValueError("Format 表中该规则未配置任何命名")

                    # 使用统一的过滤函数从Media Plan验证授权命名
                    authorized_names = self._filter_authorized_names(potential_names, product_line, df_plan, targeting_direction="")
                    
                    if not authorized_names: raise ValueError("该规则的1~6列命名均未在 Excel 中打 Y")
                    
                    # 🔑 关键：用potential_names[0]作为tracker键（稳定标识），循环authorized_names中的值
                    base_name = potential_names[0]
                    if product_line not in self.naming_tracker: self.naming_tracker[product_line] = {}
                    if base_name not in self.naming_tracker[product_line]: self.naming_tracker[product_line][base_name] = 0
                    usage_count = self.naming_tracker[product_line][base_name]

                    if usage_count >= len(authorized_names):
                        raise ValueError(f"命名名额已用完（可用 {len(authorized_names)} 个），跳过以避免重复命名")
                    target_template = authorized_names[usage_count]
                    final_name = str(target_template).replace("Productline", str(product_line))
                    self.naming_tracker[product_line][base_name] += 1

                elif ad_type == 'SD':
                    targeting_raw = await row.locator("[col-id='campaignTargetingType'] .cell-renderer-sub-content p").inner_text(timeout=2000)
                    cost_type = await row.locator("[col-id='costType'] .cell-renderer-content-text").inner_text(timeout=2000)
                    strategy = "Maximize conversions" if "CPC" in cost_type else "Maximize impressions"
                    
                    # 调用修改后的analyze_sd_popup，返回3个值
                    targets, asins, targeting_direction = await self.analyze_sd_popup(page, row, targeting_raw)
                    if not asins: raise ValueError("未抓取到 ASIN")

                    product_line = self.find_product_line_by_asin(asins[0], current_country)
                    if not product_line: raise ValueError(f"未找到 ASIN {asins[0]} 的产品线")

                    # 🔑 关键：完全借鉴auton named campaign的逻辑
                    # 从Format表读取模板、条件匹配、Media Plan授权、naming_tracker循环
                    c_targeting = self.dehydrate(targeting_raw)
                    c_strategy = self.dehydrate(strategy)
                    c_targets = self.dehydrate(targets)
                    
                    df_clean = self.df_format.copy()
                    df_clean['c_ad_prod'] = df_clean['Ad product'].apply(self.dehydrate)
                    df_clean['c_tgt_type'] = df_clean['Targeting'].apply(self.dehydrate)
                    df_clean['c_strat'] = df_clean['Strategy'].apply(self.dehydrate)
                    df_clean['c_tgts'] = df_clean['Targets'].apply(self.dehydrate)

                    matched_formats = self.df_format[
                        (df_clean['c_ad_prod'].str.contains('sponsoreddisplay', na=False)) &
                        (df_clean['c_tgt_type'].str.contains(c_targeting, na=False) | df_clean['c_tgt_type'].eq(c_targeting)) &
                        (df_clean['c_strat'].str.contains(c_strategy, na=False) | df_clean['c_strat'].eq(c_strategy)) &
                        (df_clean['c_tgts'].str.contains(c_targets, na=False) | df_clean['c_tgts'].eq(c_targets))
                    ]
                    if matched_formats.empty: raise ValueError("Format 表无匹配规则")
                    
                    # ================= 完整的命名流程：1. 读模板 2. 过滤授权 3. 追踪使用 =================
                    name_cols = ['Campaign Name', 'Campaign Name 2', 'Campaign Name 3', 'Campaign Name 4', 'Campaign Name 5', 'Campaign Name 6']
                    format_row = matched_formats.iloc[0]
                    
                    # 1️⃣ 从Format表读取1~6个可能的命名
                    potential_names = []
                    for col in name_cols:
                        val = format_row.get(col)
                        if pd.notna(val) and str(val).strip():
                            potential_names.append(str(val).strip())
                    
                    if not potential_names: raise ValueError("Format 表中该规则未配置任何命名")

                    # 使用统一的过滤函数，传入targeting_direction来区分竞品vs本品
                    authorized_names = self._filter_authorized_names(potential_names, product_line, df_plan, targeting_direction)
                    
                    if not authorized_names: raise ValueError("该规则的1~6列命名均未在 Excel 中打 Y")
                    
                    # 🔑 关键：用potential_names[0]作为tracker键（稳定标识），循环authorized_names中的值
                    base_name = potential_names[0]
                    if product_line not in self.naming_tracker: self.naming_tracker[product_line] = {}
                    if base_name not in self.naming_tracker[product_line]: self.naming_tracker[product_line][base_name] = 0
                    usage_count = self.naming_tracker[product_line][base_name]

                    if usage_count >= len(authorized_names):
                        raise ValueError(f"命名名额已用完（可用 {len(authorized_names)} 个），跳过以避免重复命名")
                    target_template = authorized_names[usage_count]
                    final_name = str(target_template).replace("Productline", str(product_line))
                    self.naming_tracker[product_line][base_name] += 1
                    

                name_trigger = row.locator('[data-e2e-id="tactical-recommendations-table:cell-campaignName:edit"]')
                await name_trigger.wait_for(state="visible", timeout=3000)
                await name_trigger.click(position={'x': 12, 'y': 10}, force=True)
                await page.wait_for_timeout(300)
                
                if await page.locator("p:has-text('ASIN:')").is_visible():
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(400)
                    await name_trigger.click(position={'x': 15, 'y': 10}, force=True)

                input_box = page.locator('[data-e2e-id="tactical-recommendations-table:cell-campaignName:input"]')
                await input_box.wait_for(state="visible", timeout=3000)
                await input_box.click()
                await page.keyboard.press("Control+A")
                await page.keyboard.press("Backspace")
                await input_box.type(final_name)
                await page.wait_for_timeout(200)
                await page.keyboard.press("Enter")
                
                print(f"[{worker_id}]   ✅ 命名保存成功。")
                
            except Exception as e:
                print(f"[{worker_id}]   ❌ 跳过该行: {e}")
                try: await row.click() 
                except: pass
            idx += 1

    async def process_single_worker(self, worker_id: str):
        """🤖 工人处理函数：不断从任务队列取任务直到完成"""
        task_context = None
        page = None
        
        try:
            while True:
                try:
                    # 从队列获取任务（无阻塞）
                    task = self.tasks_queue.get_nowait()
                except asyncio.QueueEmpty:
                    print(f"[{worker_id}] 🛑 任务队列已空，工人退出。")
                    break
                
                url = task['url']
                brands = task['brands']
                country = task['country']
                ad_type = task['ad_type']
                
                # 多品牌情况下的友好显示
                brands_str = '+'.join(brands) if isinstance(brands, list) else brands
                print(f"\n[{worker_id}] 📦 领到新包裹: [{country}] {ad_type} 类型 (品牌: {brands_str})")
                
                if task_context is None:
                    task_context = await self.headless_browser.new_context(viewport={"width": 1920, "height": 10000})
                    if self.login_cookies:
                        await task_context.add_cookies(self.login_cookies)
                
                page = await task_context.new_page()
                
                # 🛡️ 核心优化：拦截图片、字体和媒体流，大幅减轻网络压力并加快渲染
                async def route_interceptor(route):
                    if route.request.resource_type in ["image", "media", "font"]:
                        await route.abort()
                    else:
                        await route.continue_()
                await page.route("**/*", route_interceptor)
                
                try:
                    await self.navigate_and_switch_country(page, task, worker_id)
                    
                    # === 新改进：在进入 Ready to publish 前，一定要确认导航成功 ===
                    print(f"[{worker_id}] ✅ 国家切换已确认，现在准备进入 Ready to publish...")
                    await page.wait_for_timeout(2000)  # 给页面额外的稳定时间
                    
                    print(f"[{worker_id}] -> 正在定位 Ready to publish 标签...")
                    rtp_btn = page.locator('[data-takt-id="gac_top_nav_ready-to-publish"]')
                    
                    # 先检查按钮是否存在
                    rtp_count = await rtp_btn.count()
                    if rtp_count == 0:
                        # 尝试刷新
                        print(f"[{worker_id}] ⚠️ 未找到 Ready to publish，尝试刷新...")
                        await page.reload(wait_until="networkidle")
                        await page.wait_for_timeout(2000)
                        rtp_btn = page.locator('[data-takt-id="gac_top_nav_ready-to-publish"]')
                        rtp_count = await rtp_btn.count()
                        if rtp_count == 0:
                            raise Exception("刷新后仍找不到 Ready to publish 按钮")
                    
                    print(f"[{worker_id}] -> 找到 Ready to publish，准备点击...")
                    await rtp_btn.wait_for(state="visible", timeout=10000)
                    await rtp_btn.click(force=True)
                    
                    # === 核心改进：点击后不能急，要给页面时间重新渲染 ===
                    print(f"[{worker_id}] -> 已点击 Ready to publish，等待内容加载...")
                    await page.wait_for_timeout(3000)  # 给 JavaScript 重新渲染的时间
                    try:
                        await page.wait_for_load_state('networkidle', timeout=15000)
                    except Exception:
                        print(f"[{worker_id}] ⚠️ networkidle 等待超时（后台可能在持续轮询，不阻塞执行）...")
                    await page.wait_for_timeout(2000)  # 额外缓冲
                    
                    print(f"[{worker_id}] ✅ Ready to publish 已加载！现在寻找卡片...")
                    
                    # === 关键修复：用正确的卡片容器层级 ===
                    # 每个卡片的结构：
                    # <div class="sc-storm-ui-30101660__sc-1ofhy6d-1 fjxdzg">           ← 最外层
                    #   <div class="sc-feoqov jxbakB ...">                          ← 中层
                    #     <div id="card-header" data-testid="card-header">...</div>
                    #     <div id="card-body" data-testid="card-body">...内容...</div>  ← 包含AdProduct
                    #     <div id="card-footer" data-testid="card-footer">          ← 按钮在这里！
                    #       <button>View recommendation details</button>
                    #     </div>
                    #   </div>
                    # </div>
                    
                    print(f"[{worker_id}] -> 正在等待卡片出现 (最大延时增至 60 秒)...")
                    try:
                        # 增加基础超时时间，应对大量广告批量创建时的极端慢速加载
                        await page.wait_for_selector('div[data-testid="card-footer"]', state="visible", timeout=60000)
                        print(f"[{worker_id}] ✅ 卡片已加载！")
                    except:
                        print(f"[{worker_id}] ⚠️ 用footer选择器失败，尝试card-body...")
                        # 给最后一次机会
                        await page.wait_for_selector('div[data-testid="card-body"]', state="visible", timeout=60000)
                        print(f"[{worker_id}] ✅ 通过card-body找到卡片！")
                    
                    # 现在卡片肯定存在了，开始查找
                    print(f"[{worker_id}] -> 正在寻找 {ad_type} 类型的卡片...")
                    
                    # 用更大的容器定义"卡片"：包含card-body和card-footer的父容器
                    # 方式：找所有包含 card-body 和 card-footer 的最小容器
                    all_cards = page.locator('div:has(> div[data-testid="card-body"]) >> div[data-testid="card-footer"]').locator('..')
                    card_count = await all_cards.count()
                    
                    if card_count == 0:
                        # 备用方案：直接用footer作为卡片容器（因为footer中有button）
                        all_cards = page.locator('div[data-testid="card-footer"]')
                        card_count = await all_cards.count()
                    
                    print(f"[{worker_id}]    -> 找到 {card_count} 个卡片")
                    
                    if card_count == 0:
                        raise Exception("找不到任何卡片")
                    
                    # 逐个检查卡片
                    found_button = False
                    for i in range(card_count):
                        try:
                            # 获取单个卡片（可能是footer或中间层div）
                            card = all_cards.nth(i)
                            
                            # 在卡片内查找 card-body（包含内容和AdType信息）
                            card_body = card.locator('div[data-testid="card-body"]')
                            
                            # 如果这个卡片没有card-body，说明all_cards本身就是footer，需要向上找
                            body_exists = await card_body.count()
                            if body_exists == 0:
                                # 说明card本身就是footer或其他容器，尝试找到父元素的card-body
                                card_body = card.locator('.. >> div[data-testid="card-body"]')
                                body_exists = await card_body.count()
                            
                            if body_exists == 0:
                                print(f"[{worker_id}]    ⚠️ 卡片 {i+1} 没有找到card-body，跳过")
                                continue
                            
                            card_text = await card_body.first.inner_text()
                            
                            # 检查是否包含目标AdType
                            if ad_type in card_text:
                                print(f"[{worker_id}]    ✓ 卡片 {i+1} 包含 {ad_type}")
                                
                                # === 关键修复：button在card-footer内，需要从卡片的footer中查找 ===
                                card_footer = card.locator('div[data-testid="card-footer"]')
                                footer_count = await card_footer.count()
                                
                                if footer_count == 0:
                                    # 备用：button可能直接在卡片内或其他位置
                                    card_footer = card
                                
                                btn = card_footer.locator('button:has-text("View recommendation details")').first
                                btn_visible = await btn.count()
                                
                                if btn_visible > 0:
                                    await btn.scroll_into_view_if_needed()
                                    await page.wait_for_timeout(500)
                                    await btn.click(force=True)
                                    print(f"[{worker_id}]    ✓ 已点击按钮")
                                found_button = True
                                break
                        except:
                            continue
                    
                    if not found_button:
                        raise Exception(f"找不到 {ad_type} 卡片的按钮")
                    
                    # 等待表格
                    print(f"[{worker_id}]    -> 等待详情表格...")
                    await page.wait_for_selector(".ag-center-cols-container .ag-row", state="visible", timeout=60000)
                    await page.wait_for_timeout(1500)
                    print(f"[{worker_id}] ✅ 成功加载 {ad_type} 详情表格！")
                    
                    # 执行命名引擎
                    await self._run_naming_engine(page, task, worker_id, ad_type)
                    
                    self.completed_tasks += 1
                    print(f"[{worker_id}] ✅ [{country}] {ad_type} ({brands_str}) 完成！进度: {self.completed_tasks}/{self.total_tasks}")
                    
                except Exception as e:
                    print(f"[{worker_id}] ❌ 包裹崩溃: {e}")
                    # 记录失败的包裹（品牌+国家+ad_type）
                    self.failed_asins[(brands_str, country, ad_type)] = str(e)
                finally:
                    try:
                        if page: await page.close()
                    except: 
                        pass
        finally:
            if task_context:
                try:
                    await task_context.close()
                except: 
                    pass

    async def run_all(self):
        """🧠 中控台：启动5个工人从任务队列处理任务"""
        await self.vanguard_login()
        
        print("\n[系统] 🌐 正在初始化并发浏览器 (无头模式)...")
        self.headless_browser = await self.playwright.chromium.launch(
            executable_path=self.chrome_path,
            headless=True, 
            args=["--headless=new", "--disable-blink-features=AutomationControlled"] 
        )
        
        # 将所有任务加入队列（与 _generate_task_packages 的逻辑保持一致）
        unique_urls = self.df_asin.dropna(subset=['url site']).drop_duplicates(subset=['url site'])['url site'].tolist()
        
        for url in unique_urls:
            # 获取该URL下的所有国家
            countries_for_url = self.df_asin[self.df_asin['url site'] == url]['Country'].dropna().unique().tolist()
            
            for country in countries_for_url:
                # 获取该URL+Country下的所有Brand
                brands_for_url_country = self.df_asin[
                    (self.df_asin['url site'] == url) & 
                    (self.df_asin['Country'] == country)
                ]['Brand name'].unique().tolist()
                
                # 该URL+Country下的所有Brand的媒体计划合并
                combined_plans = []
                for brand in brands_for_url_country:
                    brand_plan = self.df_media_plan[
                        (self.df_media_plan['Brand'] == brand) & 
                        (self.df_media_plan['Country'] == country)
                    ]
                    if not brand_plan.empty:
                        combined_plans.append(brand_plan)
                
                # 如果该URL+Country组合没有任何有效的媒体计划，跳过
                if not combined_plans:
                    continue
                
                merged_campaigns = pd.concat(combined_plans, ignore_index=True)
                
                # 为每个 AdType (SB, SD, SP) 生成独立的任务包裹并加入队列
                for ad_type in ['SB', 'SD', 'SP']:
                    task = {
                        'url': url,
                        'brands': brands_for_url_country,
                        'country': country,
                        'ad_type': ad_type,
                        'campaigns': merged_campaigns
                    }
                    await self.tasks_queue.put(task)
        
        print("\n" + "🔥"*25)
        print(f"🔥 任务队列引擎启动！共 {self.total_tasks} 个任务")
        print(f"🔥 启动 {self.concurrency} 个并发工人...（工人完成一个任务立即取下一个）")
        print("🔥"*25 + "\n")
        
        # 启动N个工人并发执行
        workers = [self.process_single_worker(f"W-{i+1}") for i in range(self.concurrency)]
        await asyncio.gather(*workers, return_exceptions=True)
        
        # 输出汇总报告
        print("\n" + "="*60)
        print("📊 [命名任务汇总报告]")
        print("="*60)
        print(f"\n✅ 已完成任务: {self.completed_tasks}/{self.total_tasks}")
        
        if self.failed_asins:
            print(f"\n❌ 以下 {len(self.failed_asins)} 个广告无法命名：\n")
            for key, reason in self.failed_asins.items():
                print(f"  • {key}")
                print(f"    原因: {reason}\n")
        else:
            print("\n✅ 全部广告均已成功命名！")
        
        print("="*60)
        print("\n✅ 所有工人已完成工作，系统即将关闭...")

    async def close(self):
        if self.playwright: 
            await self.playwright.stop()

# ================= Main Execution =================
def main():
    print("="*50)
    print(" 🚀 Amazon Auto-Namer V2.2 (Task Queue Mode) 🚀")
    print("="*50)
    
    bot = None
    try:
        processor = DataProcessor(MEDIA_PLAN_PATH, ASIN_INFO_PATH, FORMAT_PATH)
        df_plan, df_asin, df_format = processor.process()
        
        bot = AsyncAmazonAutoNamerBot(df_plan, df_asin, df_format, concurrency=3)
        bot.start_local_chrome()
        
        asyncio.run(bot.run_all())
        
    except Exception as e:
        print(f"\n❌ 运行中发生严重错误: {e}")
    finally:
        if bot: asyncio.run(bot.close())
        print("\n🏁 任务结束。")

if __name__ == "__main__":
    main()
