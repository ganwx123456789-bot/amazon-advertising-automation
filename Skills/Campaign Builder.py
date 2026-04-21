import os
import sys
import time
import re
import sqlite3
import subprocess
import pandas as pd # type: ignore
import asyncio
import random
import urllib.parse
from typing import Optional, List, Dict, Any, Union, Tuple
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Browser, BrowserContext, Page, Playwright # type: ignore

# ================= Configuration (Global) =================
CHROME_USER_DATA = r"C:\sel_chrome"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_PLAN_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "Media Plan For Campaign Builder.xlsx"))
ASIN_INFO_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "ASIN_Input_Template For Campaign Builder.xlsx"))
FORMAT_PATH = os.path.join(BASE_DIR, "campaign format.xlsx")

# ================= Module 1: Data Processing =================
class DataProcessor:
    def __init__(self, media_plan_path: str, asin_info_path: str, format_path: str) -> None:
        self.media_plan_path = media_plan_path
        self.asin_info_path = asin_info_path
        self.format_path = format_path
        self.conn = sqlite3.connect(':memory:')
        
    def clean_text(self, text: Any, is_asin: bool = False, is_url: bool = False) -> str:
        if pd.isna(text): return ""
        text = str(text).strip()
        if is_url: return re.sub(r'\s+', '', text)
        cleaned = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', text)
        if is_asin: return cleaned.upper()
        return cleaned 
    
    def process(self):
        print("\n[数据模块] 开始解析 Excel 并构建本地数据库...")
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

        df_asin = df_asin.rename(columns={brand_col: 'Brand name', url_col: 'url site', pl_col: 'Parent ASIN or Product line', country_col: 'Country'})
        cols_to_fill = ['Parent ASIN or Product line', 'Country', 'Brand name', 'url site']
        for col in cols_to_fill:
            if col in df_asin.columns:
                df_asin[col] = df_asin[col].replace(r'^\s*$', pd.NA, regex=True).ffill()
                
        df_asin['Parent ASIN or Product line'] = df_asin['Parent ASIN or Product line'].apply(lambda x: self.clean_text(x))
        if 'Child ASIN' in df_asin.columns:
            df_asin['Child ASIN'] = df_asin['Child ASIN'].apply(lambda x: self.clean_text(x, is_asin=True))
        df_asin['Country'] = df_asin['Country'].apply(lambda x: self.clean_text(x))
        df_asin['Brand name'] = df_asin['Brand name'].apply(lambda x: self.clean_text(x))

        df_format = pd.read_excel(self.format_path)
        df_format.columns = [str(c).strip() for c in df_format.columns]
        
        media_plan_data = []
        unique_brands = df_asin['Brand name'].replace('', pd.NA).dropna().unique()
        all_sheets = pd.ExcelFile(self.media_plan_path).sheet_names
        
        for brand in unique_brands:
            try:
                actual_sheet = next((s for s in all_sheets if self.clean_text(s) == brand), None) # type: ignore
                if not actual_sheet: continue
                    
                df_brand_plan = pd.read_excel(self.media_plan_path, sheet_name=actual_sheet) # type: ignore
                product_line_cols = df_brand_plan.columns[7:] 
                
                for pl_col in product_line_cols:
                    clean_pl_col = self.clean_text(pl_col)
                    for _, row in df_brand_plan.iterrows():
                        base_campaign_name = str(row.iloc[0]).strip()
                        if base_campaign_name.lower() == 'nan' or not base_campaign_name: continue
                            
                        if str(row[pl_col]).strip().upper() == 'Y':
                            ad_type = 'SD' if 'SD' in base_campaign_name.upper() else ('SBV' if 'SBV' in base_campaign_name.upper() else ('SB' if 'SB' in base_campaign_name.upper() else 'SP'))
                            media_plan_data.append({
                                'Brand': brand,
                                'Product_line': clean_pl_col,
                                'Campaign_name': re.sub(r'campaign', clean_pl_col, base_campaign_name, flags=re.IGNORECASE),
                                'Base_campaign_name': base_campaign_name,
                                'Ad_product': ad_type
                            })
            except Exception as e:
                print(f"⚠️ 解析品牌 '{brand}' 时出错: {e}")
                    
        df_media_plan = pd.DataFrame(media_plan_data)
        if df_media_plan.empty: raise Exception("❌ 抓取失败，请检查数据！")
        return df_media_plan, df_asin, df_format

# ================= Module 2: ASYNC Web Automation =================
class AsyncAmazonAdBot:
    def __init__(self, df_media_plan: Any, df_asin: Any, df_format: Any, concurrency: int =5 ) -> None: 
        self.concurrency = concurrency
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None  # 先锋用的可视浏览器
        self.headless_browser: Optional[Browser] = None # 并发静默浏览器
        self.chrome_path: Optional[str] = None
        self.context: Optional[BrowserContext] = None
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        
        self.tasks_list: List[Dict[str, Any]] = []
        self.login_cookies: List[Any] = []
        self.resolved_urls: Dict[Tuple[str, str], str] = {}
        self.df_asin = df_asin
        self.df_format = df_format
        

        self.global_staging_area: List[Tuple[BrowserContext, Page, str]] = []  # 存放当前批次已就绪的包裹 [(context, page, name), ...]
        self.failed_tasks: List[Dict[str, str]] = []  # 追踪失败的任务信息 [{'product_line': str, 'campaign': str, 'reason': str}]
        self.failed_task_packages: List[Dict[str, Any]] = []  # 追踪整包失败（用于全量校验）
        self.expected_campaign_total: int = 0
        self.configured_campaign_total: int = 0
        self._reusable_pages: Dict[Tuple[str, str], Tuple[Any, Any]] = {}  # (brand, country) → (context, page) 页面复用缓存
        # =========================================================

        # 用 (brand, product_line) 组合键映射国家，解决不同国家同名产品线的问题
        _pl_country_df = df_asin.dropna(subset=['Country', 'Brand name']).drop_duplicates(
            subset=['Brand name', 'Parent ASIN or Product line'], keep='first'
        )
        brand_pl_to_country: Dict[Tuple[str, str], str] = {}
        for _, r in _pl_country_df.iterrows():
            key = (str(r['Brand name']).strip(), str(r['Parent ASIN or Product line']).strip())
            brand_pl_to_country[key] = str(r['Country']).strip()
        
        # 给 media plan 加 Country 列：用 (Brand, Product_line) 组合查找
        df_media_plan['Country'] = df_media_plan.apply(
            lambda row: brand_pl_to_country.get((row['Brand'], row['Product_line']), None), axis=1
        )
        df_media_plan = df_media_plan.dropna(subset=['Country'])
        
        # 调试：打印国家分布
        country_counts = df_media_plan['Country'].value_counts().to_dict()
        print(f"\n📊 产品线→国家映射: {country_counts} (共 {len(brand_pl_to_country)} 个组合键)")

        # 启动前总览：本次预计创建的 campaign 数量分布（SB 含 SBV）
        _ad_series = df_media_plan['Ad_product'].astype(str).str.upper()
        _sp_count = int((_ad_series == 'SP').sum())
        _sd_count = int((_ad_series == 'SD').sum())
        _sb_count = int(_ad_series.isin(['SB', 'SBV']).sum())
        _total_campaigns = int(len(df_media_plan))
        self.expected_campaign_total = _total_campaigns
        print("\n📌 [启动总览] 本次创建任务统计：")
        print(f"   总 Campaign: {_total_campaigns}")
        print(f"   SP: {_sp_count}")
        print(f"   SB: {_sb_count}")
        print(f"   SD: {_sd_count}")
        
        # (brand, country) → url 精准映射；同时保留 brand → url fallback
        brand_country_to_url: Dict[Tuple[str, str], str] = {}
        brand_to_url: Dict[str, str] = {}
        for _, row in df_asin.iterrows():
            b = str(row.get('Brand name', '')).strip()
            u = str(row.get('url site', '')).strip()
            c = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', str(row.get('Country', '')).strip())
            if b and b.lower() != 'nan' and u and u.lower() != 'nan':
                if not u.startswith('http'): u = 'https://' + u
                # 精准 (brand, country) 映射：第一次出现的优先
                if c and c.lower() != 'nan':
                    if (b, c) not in brand_country_to_url:
                        brand_country_to_url[(b, c)] = u
                # fallback brand-only 映射
                if b not in brand_to_url:
                    brand_to_url[b] = u

        print("\n🔗 当前内存中的品牌链接映射：")
        for (b, c), u in brand_country_to_url.items():
            print(f"   -> 品牌 [{b}] 国家 [{c}] : {u}")
        if not brand_country_to_url and not brand_to_url:
            print("   -> ⚠️ 致命警告：系统没有抓取到任何有效的 URL！")

        self.vanguard_url = list(brand_to_url.values())[0] if brand_to_url else "https://advertising.amazon.com/"

        # 调试：打印 Media Plan 的国家分布
        if not df_media_plan.empty and 'Country' in df_media_plan.columns:
            country_dist = df_media_plan['Country'].value_counts().to_dict()
            print(f"\n📊 Media Plan 国家分布: {country_dist}")
        
        # 恢复成独立的小包裹，统计总数
        # 🚀 改进 1：拆分任务粒度并增加单批最大包裹限额，提升并发防崩溃
        for (brand, country, product_line), plan_group in df_media_plan.groupby(['Brand', 'Country', 'Product_line'], sort=False):
            asins = df_asin[df_asin['Parent ASIN or Product line'] == product_line]['Child ASIN'].dropna().tolist()
            if not asins: continue
            # 优先使用 (brand, country) 精准 URL，fallback 到 brand 通用 URL
            target_url = brand_country_to_url.get((brand, country)) or brand_to_url.get(brand)
            if not target_url:
                print(f"  ⚠️ 找不到 URL: brand={brand}, country={country}")
                continue
            # 调试：如果用了 fallback，打印警告
            if (brand, country) not in brand_country_to_url and brand in brand_to_url:
                print(f"  ⚠️ [{country}] 无精准 URL，使用品牌通用 URL: {target_url}")
                
            plan_group = plan_group.copy()
            plan_group['Ad_Family'] = plan_group['Ad_product'].apply(lambda x: 'SB_FAMILY' if str(x).upper() in ['SB', 'SBV'] else str(x).upper())
            sb_count = len(plan_group[plan_group['Ad_Family'] == 'SB_FAMILY'])
            
            needs_split = (len(plan_group) > 30) or (sb_count > 16)
            
            if needs_split:
                print(f"[{product_line}] 触发拆分条件 (总条数: {len(plan_group)}, SB条数: {sb_count})！按类型拆分为独立机器人处理。")
                for fam, fam_chunk in plan_group.groupby('Ad_Family', sort=False):
                    if fam == 'SB_FAMILY':
                        total_sb = len(fam_chunk)
                        if total_sb > 16:
                            num_bots = (total_sb + 15) // 16
                            print(f"    -> {fam} 族群共 {total_sb} 条(超过16条)，将平均分配给 {num_bots} 个机器人！")
                            base_size = total_sb // num_bots
                            remainder = total_sb % num_bots
                            start_idx = 0
                            for bot_i in range(num_bots):
                                current_chunk_size = base_size + (1 if bot_i < remainder else 0)
                                end_idx = start_idx + current_chunk_size
                                sub_chunk = fam_chunk.iloc[start_idx:end_idx] # type: ignore
                                task = {
                                    'brand': brand,
                                    'country': country,
                                    'product_line': product_line,
                                    'ad_family': fam,
                                    'chunk_idx': bot_i + 1,
                                    'url': target_url,
                                    'asins': asins,
                                    'campaigns': sub_chunk
                                }
                                self.tasks_list.append(task)
                                start_idx = end_idx
                        else:
                            task = {
                                'brand': brand,
                                'country': country,
                                'product_line': product_line,
                                'ad_family': fam,
                                'chunk_idx': 1,
                                'url': target_url,
                                'asins': asins,
                                'campaigns': fam_chunk
                            }
                            self.tasks_list.append(task)
                    else:
                        MAX_PER_BOT = 32
                        if len(fam_chunk) > MAX_PER_BOT:
                            print(f"    -> {fam} 族群达到 {len(fam_chunk)} 条，将按最大 {MAX_PER_BOT} 条硬切分成多个机器人！")
                        for i in range(0, len(fam_chunk), MAX_PER_BOT):
                            sub_chunk = fam_chunk.iloc[i:i+MAX_PER_BOT] # type: ignore
                            task = {
                                'brand': brand,
                                'country': country,
                                'product_line': product_line,
                                'ad_family': fam,
                                'chunk_idx': i // MAX_PER_BOT + 1,
                                'url': target_url,
                                'asins': asins,
                                'campaigns': sub_chunk
                            }
                            self.tasks_list.append(task)
            else:
                task = {
                    'brand': brand,
                    'country': country,
                    'product_line': product_line,
                    'ad_family': 'ALL_IN_ONE',
                    'chunk_idx': 1,
                    'url': target_url,
                    'asins': asins,
                    'campaigns': plan_group
                }
                self.tasks_list.append(task)
                
        print(f"\n📊 [中控中心] 全局批次齐射初始化完成！共打包 {len(self.tasks_list)} 个独立任务包裹。")

    def start_local_chrome(self) -> None:
        possible_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe") 
        ]
        self.chrome_path = next((p for p in possible_paths if os.path.exists(p)), None)
        if not self.chrome_path: raise FileNotFoundError("ERROR: Chrome not found.")
        
        print(f"\n[系统] 启动本地 Chrome (可见模式，供先锋验证)...")
        cmd: List[str] = [str(self.chrome_path), "--remote-debugging-port=9224", f"--user-data-dir={CHROME_USER_DATA}"]
        subprocess.Popen(cmd)
        time.sleep(3)

    async def vanguard_login(self) -> None:
        """🤖 1号机器人：探路与环境接管"""
        self.playwright = await async_playwright().start()
        print("[1号先锋] 正在探测 Chrome 调试端口...")
        from chrome_port_finder import get_cdp_url
        cdp_url = get_cdp_url()
        assert self.playwright is not None
        self.browser = await self.playwright.chromium.connect_over_cdp(cdp_url) # type: ignore
        assert self.browser is not None
        self.context = self.browser.contexts[0] # type: ignore
        assert self.context is not None
        
        vanguard_page = await self.context.new_page() # type: ignore
        await vanguard_page.set_viewport_size({"width": 1920, "height": 10000})
        
        print(f"[1号先锋] 正在使用真实链接校验环境: {self.vanguard_url}")
        await vanguard_page.goto(self.vanguard_url, timeout=60000)
        
        try:
            await vanguard_page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=25000)
        except PlaywrightTimeoutError:
            print("  -> ⏸️ 环境未就绪！如果需要手机验证码，请在浏览器中手动完成，直到左上角出现国家选择器...")
            await vanguard_page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=0)
            
        self.login_cookies = await self.context.cookies() or [] # type: ignore
        cookies_len = len(self.login_cookies)
        print(f"✅ 大门已开！已成功提取 {cookies_len} 条登录状态凭证，准备分发给并发工人...")
        await vanguard_page.close()

    async def _click_dropdown_option(self, page, dropdown_locator, option_text=None, index=0, worker_id=""):
        """🛡️ 强化版点击：增加 3 次展开校验与 1s 间隔重试"""
        if await dropdown_locator.is_visible():
            options = page.locator('[role="option"]')
            
# 🚀 第一层修改：尝试 3 次“原地点击并确认展开”
            success_expanded = False
            for attempt in range(3):
                # 1. 执行点击展开 (或再次点击以唤醒)
                await dropdown_locator.scroll_into_view_if_needed()
                await dropdown_locator.click(force=True)
                
                # 2. 增加动态侦听：给 3 秒时间每隔 0.5 秒查一次
                start_wait = time.time()
                while time.time() - start_wait < 3: 
                    if await options.count() > 0:
                        # 只要刷出了名字，哪怕是一个，也说明列表活了
                        success_expanded = True
                        break
                    await page.wait_for_timeout(500)
                
                if success_expanded:
                    break
                else:
                    print(f"[{worker_id}] ⚠️ 第 {attempt + 1} 次尝试后未发现选项，1秒后原位重试点击...")
                    # 💡 这里不再按 Escape，而是发呆 1 秒后直接进入下一次 for 循环的 click
                    await page.wait_for_timeout(1000)

            # 如果 3 次都点不开
            if not success_expanded:
                raise Exception(f"❌ 连续 3 次尝试点击下拉框均无法加载选项，页面渲染超时。")
            # --- 第二层：原有的匹配与点击逻辑 ---
            if option_text:
                target_norm = str(option_text).replace(" ", "").lower()
                
                # 新增重试遍历：第一次找不到的话，等待1秒后重试点击并再次获取节点
                for match_attempt in range(2):
                    count = await options.count()
                    if count > 0:
                        # 仅精确匹配（全等），不接受任何模糊匹配，避免 "4k webcam" 命中 "4k webcam for streaming"
                        match_idx = await options.evaluate_all(f'''(elements) => {{
                            const target = "{target_norm}";
                            const stripS = s => s.endsWith("s") ? s.slice(0, -1) : s;
                            // 第一轮：innerText 去空格后完全相等
                            for (let i = 0; i < elements.length; i++) {{
                                const text = (elements[i].innerText || "").replace(/[\\s]/g, "").toLowerCase();
                                const val = (elements[i].getAttribute("value") || "").replace(/[\\s]/g, "").toLowerCase();
                                if (text === target || val === target) return i;
                            }}
                            // 第二轮：innerText 去掉括号装饰后完全相等（例如 "4k webcam (5 ASINs)" → "4kwebcam"）
                            for (let i = 0; i < elements.length; i++) {{
                                const rawText = (elements[i].innerText || "");
                                const stripped = rawText.replace(/\\([^)]*\\)/g, "").replace(/[\\s]/g, "").toLowerCase();
                                if (stripped === target) return i;
                            }}
                            // 第三轮：末尾 s 归一化后精确匹配（解决 "product" vs "products" 单复数差异）
                            for (let i = 0; i < elements.length; i++) {{
                                const rawText = (elements[i].innerText || "");
                                const strippedText = rawText.replace(/\\([^)]*\\)/g, "").replace(/[\\s]/g, "").toLowerCase();
                                const val = (elements[i].getAttribute("value") || "").replace(/[\\s]/g, "").toLowerCase();
                                if (stripS(strippedText) === stripS(target) || stripS(val) === stripS(target)) return i;
                            }}
                            return -1;
                        }}''')
                        
                        if match_idx != -1:
                            await options.nth(match_idx).evaluate("node => node.click()")
                            await page.wait_for_timeout(50)
                            return 
                    if match_attempt == 0:
                        print(f"[{worker_id}] ⚠️ 第一次未找到精确匹配选项: '{option_text}'，等待1秒后重试...")
                        await page.wait_for_timeout(1000)
                        await dropdown_locator.click(force=True)
                        await page.wait_for_timeout(1000)
                
                # 如果 2 次循环完都没找到精确匹配
                raise Exception(f"DropdownMatchError: 找不到精确匹配选项: '{option_text}'")
            else:
                # 如果没传文本，按索引点击
                await options.nth(index).evaluate("node => node.click()")
                await page.wait_for_timeout(200)
                return
                    
            if option_text: raise Exception(f"找不到匹配选项: '{option_text}'")
            else: raise Exception("未能正常展开下拉框")

    async def navigate_and_switch_country(self, page, task, worker_id):
        """🚀 智能缓存加速版 — 同 URL 不刷新"""
        target_url = task['url']
        target_country = task['country']
        brand = task['brand']
        cache_key = (brand, target_country)

        print(f"[{worker_id}] 🌐 准备前往目标站点 [{target_country}]...")
        if not target_url or str(target_url).strip() == "" or str(target_url).lower() == "nan":
            raise Exception("❌ 传入的品牌 URL 为空！请检查 Excel。")
            
        await asyncio.sleep(random.uniform(0.5, 1.5))

        # ===== 核心优化：检查当前页面是否已在目标 URL，是则跳过导航 =====
        current_url = page.url or ""
        if cache_key in self.resolved_urls:
            fast_url = self.resolved_urls[cache_key]
            # 如果当前页面已经在目标 URL 上（域名+路径匹配），直接等按钮，不刷新
            if fast_url and current_url.startswith(fast_url.split("?")[0].rstrip("/")):
                print(f"[{worker_id}] ♻️ 当前页面已在目标站点，跳过导航，直接复用！")
                try:
                    await self.wait_for_create_button(page, worker_id, timeout=30000)
                    print(f"[{worker_id}] ✅ 页面复用成功，准备填表！")
                    return
                except Exception:
                    print(f"[{worker_id}] ⚠️ 页面复用失败，回退到正常导航...")

            print(f"[{worker_id}] ⚡ 触发加速机制！直飞已解析的 [{target_country}] 专属链接...")
            
            max_goto_retries = 2
            for retry in range(max_goto_retries):
                try:
                    await page.goto(fast_url, timeout=60000, wait_until="load")
                    break
                except Exception as e:
                    if retry < max_goto_retries - 1:
                        print(f"[{worker_id}] ⚠️ 加速链接加载失败，1秒后重试...")
                        await page.wait_for_timeout(1000)
                    else:
                        raise e
            
            content = await page.content()
            if "Server error" in content or "If you encounter this issue repeatedly" in content:
                await page.reload()
                await page.wait_for_timeout(5000) 
            
            await self.wait_for_create_button(page, worker_id, timeout=60000)
            print(f"[{worker_id}] ✅ 极速空降成功，准备填表！")
            return

        print(f"[{worker_id}] 🔗 未命中缓存，直跳原始目标进行正规鉴权: {target_url}")
        max_goto_retries = 3
        for retry in range(max_goto_retries):
            try:
                await page.goto(target_url, timeout=60000, wait_until="load")
                break
            except Exception as e:
                if retry < max_goto_retries - 1:
                    wait_time = 2 + retry * 3
                    print(f"[{worker_id}] ⚠️ 原始链接加载失败（第{retry+1}次），{wait_time}秒后重试...")
                    await page.wait_for_timeout(wait_time * 1000)
                else:
                    raise e
        
        content = await page.content()
        if "Server error" in content or "If you encounter this issue repeatedly" in content:
            await page.reload()
            await page.wait_for_timeout(5000) 
            
        await page.wait_for_selector('[data-takt-id="header_marketplace_switcher"]', state="visible", timeout=30000)
        
        country_mapping = {
            "ES": "Spain", "SPAIN": "Spain", "UK": "United Kingdom", "GB": "United Kingdom",
            "US": "United States", "DE": "Germany", "FR": "France", "IT": "Italy",
            "JP": "Japan", "CA": "Canada", "MX": "Mexico", "AU": "Australia"
        }
        ui_country_name = country_mapping.get(target_country.strip().upper(), target_country)
        country_selector = page.locator('[data-takt-id="header_marketplace_switcher"]')
        current_displayed = (await country_selector.inner_text()) or ""
        
        if ui_country_name.lower() not in current_displayed.lower(): # type: ignore
            print(f"[{worker_id}] 🔄 [开荒者] 正在通过 UI 首次切换国家至 [{ui_country_name}]...")
            await country_selector.click(force=True)
            await page.wait_for_timeout(500) 
            
            country_option = page.locator(f'div:text-is("{ui_country_name}"), span:text-is("{ui_country_name}")').first
            if await country_option.is_visible():
                await country_option.click(force=True)
                await page.wait_for_timeout(300) 
                confirm_btn = page.locator('#aac-chrome-change-country-button, button[data-takt-id="storm-ui-country-selector-footer-apply-button"]')
                if await confirm_btn.is_visible(): 
                    await confirm_btn.click(force=True)
                    print(f"[{worker_id}] ⏳ 已点击确认切换，等待跨国重载...")
                    await page.wait_for_timeout(1000)
                    await page.wait_for_load_state('domcontentloaded')
                
                await self.wait_for_create_button(page, worker_id, timeout=60000)
                print(f"[{worker_id}] ✅ 跨国切换成功！")
            else: 
                raise Exception(f"下拉列表中找不到国家: '{ui_country_name}'")
        else:
            print(f"[{worker_id}] ✅ 当前已在目标国家 [{ui_country_name}]，无需切换。")
            await self.wait_for_create_button(page, worker_id, timeout=60000)

        self.resolved_urls[cache_key] = page.url
        print(f"[{worker_id}] 💾 已提取并缓存 [{target_country}] 的真实直达链接！后续任务将全部起飞！")

    async def wait_for_create_button(self, page, worker_id, timeout=60000):
        """🛡️ 增强型按钮侦测：超时自动刷新，多选择器兼容工作"""
        # 定义核心选择器，兼容多种 UI 变体
        btn_selector = 'button:has-text("Create customized new campaigns"), button:has-text("Create new campaign"), button[data-testid="create-campaign-button"]'
        
        start_time = asyncio.get_event_loop().time()
        max_reloads = 2
        reloads_done = 0
        
        while (asyncio.get_event_loop().time() - start_time) * 1000 < timeout:
            try:
                # 每一轮尝试等待 15s
                await page.locator(btn_selector).first.wait_for(state="visible", timeout=15000)
                return True
            except:
                elapsed = (asyncio.get_event_loop().time() - start_time)
                if elapsed > 35 and reloads_done < max_reloads:
                    print(f"[{worker_id}] ⚠️ 按钮在 {elapsed:.1f}s 内未出现，疑似页面假死，正在尝试强制刷新页面...")
                    await page.reload()
                    await page.wait_for_timeout(8000)  # 刷新后给更多时间稳定
                    start_time = asyncio.get_event_loop().time()  # 重置计时器，刷新后给完整的 60s 预算
                    reloads_done += 1
                else:
                    # 检查并清理可能的遮挡模态框
                    try:
                        await page.evaluate("""
                            (document.querySelectorAll('[data-testid="ASXFeedbackFormModal"], .sc-kpKSZj') || []).forEach(m => m.remove());
                        """)
                    except: pass
        
        # 最后一次等待（此时 while 预算耗尽，再给 20s）
        await page.locator(btn_selector).first.wait_for(state="visible", timeout=20000)
        return True

    async def fill_campaign_form(self, page, task, worker_id, max_retries=3):
        """🚀 沙盒内的核心填表逻辑"""
        product_line_success = False
        
        for attempt in range(max_retries):
            try:
                if attempt > 0: print(f"[{worker_id}] 🔄 正在重试第 {attempt + 1}/{max_retries} 次...")
                await page.evaluate("""
                    const modals = document.querySelectorAll('[data-testid="ASXFeedbackFormModal"], .sc-kpKSZj');
                    modals.forEach(m => m.remove());
                """)
                
                await page.evaluate("window.scrollTo(0, 0);")
                await page.wait_for_timeout(500)
                
                create_btn = page.locator('button:has-text("Create customized new campaigns"), button:has-text("Create new campaign")').first
                brand_dropdown = page.locator('[data-testid="Brand-dropdown"]')
                
                if attempt == 0:
                    await create_btn.wait_for(state="visible", timeout=40000)
                    await create_btn.click(force=True)
                    await brand_dropdown.wait_for(state="visible", timeout=30000)
                else:
                    await page.evaluate("window.scrollTo(0, 0);")
                    await page.wait_for_timeout(500)
                    if not await brand_dropdown.is_visible(): 
                        if await create_btn.is_visible(): 
                            await create_btn.wait_for(state="visible", timeout=30000)
                            await create_btn.click(force=True)
                            await brand_dropdown.wait_for(state="visible", timeout=15000)
                        else:
                            raise Exception("❌ 页面完全丢失控制权！")
                        
                # 选品牌逻辑 (包含智能雷达)
                print(f"[{worker_id}] 执行：检查品牌选择框状态...")
                await brand_dropdown.wait_for(state="attached", timeout=10000)
                if await brand_dropdown.is_disabled():
                    await page.wait_for_timeout(2000) 
                    if await brand_dropdown.is_disabled(): raise Exception("❌ 无可用品牌！")

                await brand_dropdown.click()
                try:
                    await page.wait_for_selector('[role="option"]', state="visible", timeout=5000)
                except PlaywrightTimeoutError:
                    raise Exception("❌ 成功点击了下拉框，但页面没有弹出选项！")

                options = page.locator('[role="option"]')
                options_count = await options.count()
                target_brand_clean = str(task['brand']).strip().lower()

                # 🚀 先提取全部品牌文本，再按「Registered优先 + 与Excel品牌名相似度」排序
                brand_texts = await options.evaluate_all("(elements) => elements.map(el => (el.innerText || '').trim())")

                def _norm_brand(s: str) -> str:
                    return re.sub(r'[^a-z0-9\u4e00-\u9fa5]', '', (s or '').lower())

                target_norm = _norm_brand(target_brand_clean)
                scored_candidates: List[Tuple[int, int, float, str]] = []
                for i, text in enumerate(brand_texts):
                    text_lower = str(text).lower()
                    is_registered = 1 if 'registered' in text_lower else 0
                    option_norm = _norm_brand(str(text))

                    # 相似度：包含关系给满分，否则按字符集合重合度估算
                    if target_norm and option_norm and (target_norm in option_norm or option_norm in target_norm):
                        sim = 1.0
                    elif target_norm and option_norm:
                        inter = len(set(target_norm) & set(option_norm))
                        union = len(set(target_norm) | set(option_norm))
                        sim = (inter / union) if union else 0.0
                    else:
                        sim = 0.0

                    # 排序键：Registered(1高) > 相似度(高) > 文本长度更短(更精确)
                    scored_candidates.append((is_registered, i, sim, str(text)))

                indices_to_try: List[int] = []
                if scored_candidates:
                    ranked = sorted(scored_candidates, key=lambda x: (-x[0], -x[2], len(x[3])))
                    indices_to_try = [x[1] for x in ranked]
                    top = ranked[0]
                    print(
                        f"[{worker_id}] 🎯 品牌优先策略命中：优先={brand_texts[top[1]]} "
                        f"(registered={bool(top[0])}, sim={top[2]:.2f})"
                    )

                for i in range(options_count):
                    if i not in indices_to_try:
                        indices_to_try.append(i)
                if not indices_to_try: indices_to_try = [0]
                
                task_asins: List[Any] = task.get('asins', [])
                clean_asins = [str(a).strip() for a in task_asins if str(a).strip().lower() != 'nan' and str(a).strip() != '']
                asin_string = ",".join(clean_asins)

                asin_matched = False
                for idx in indices_to_try:
                    brand_name = brand_texts[idx]
                    print(f"[{worker_id}] 尝试品牌: {brand_name}")
                    
                    if idx != indices_to_try[0]:
                        await brand_dropdown.click()
                        await page.wait_for_selector('[role="option"]', state="visible", timeout=5000)
                        options = page.locator('[role="option"]')
                        
                    await options.nth(idx).click()
                    await page.wait_for_timeout(250) 
                    
                    await page.locator('[data-testid="custom-asin-add-hyperlink"]').click()
                    await page.wait_for_timeout(300)
                    
                    input_box = page.locator('[data-testid="asin-list-styled-input"]')
                    await input_box.clear()
                    await input_box.fill(asin_string)
                    await page.wait_for_timeout(500) 
                    
                    await page.locator('[data-testid="add-asins-main-button"]').click()
                    await page.wait_for_timeout(1000)
                    
                    try:
                        save_btn = page.locator('button:has-text("Save selection")')
                        await save_btn.wait_for(timeout=4000)
                        if await save_btn.is_visible():
                            await save_btn.click()
                            print(f"[{worker_id}] ✅ ASIN 匹配成功并保存！")
                            asin_matched = True
                            break 
                    except PlaywrightTimeoutError:
                        close_btn = page.locator('button[aria-label="Close"]') 
                        if await close_btn.is_visible(): 
                            await close_btn.click()
                            await page.wait_for_timeout(1000) 
                        continue 
                        
                if not asin_matched: raise Exception("找不到匹配的 ASIN！")

                # Apply Strategy
                print(f"[{worker_id}] 执行：应用策略...")
                await page.locator('[data-testid="ASIN segmentation-dropdown"]').click()
                await page.locator('[data-testid="ASIN segmentation-dropdown-item-Brand"]').click()
                await page.wait_for_timeout(100)

                asin_segments_box = page.locator('[data-testid="Products-multi-select"]')
                if await asin_segments_box.is_visible():
                    await asin_segments_box.click(force=True)
                    await page.wait_for_timeout(300) 
                    select_all_btn = page.locator('#Products-multi-select-bulkPanel-Btn')
                    if await select_all_btn.is_visible():
                        await select_all_btn.click()
                        await page.wait_for_timeout(100)
                    viewport = page.viewport_size
                    if viewport: await page.mouse.click(viewport['width'] - 50, 200)
                    else: await asin_segments_box.click(force=True) 
                    await page.wait_for_timeout(300)

                apply_btn = page.locator('[data-testid="apply-button"]')
                if await apply_btn.is_visible() and not await apply_btn.is_disabled():
                    await apply_btn.click()

                # 子表单填写
                last_ad_family = None
                expected_campaign_count = len(task['campaigns'])
                configured_campaign_count = 0
                skipped_campaigns = {}  # 记录被跳过的campaign
                consecutive_fail_count = 0  # 🚀 连续失败计数器：达到2则判定页面已刷新，触发从Create重开

                def register_campaign_failure(campaign: str, reason: str, count_toward_retry: bool = True) -> None:
                    nonlocal consecutive_fail_count
                    self.failed_tasks.append({'product_line': task['product_line'], 'campaign': campaign, 'reason': reason}) # type: ignore
                    skipped_campaigns[campaign] = reason
                    if not count_toward_retry:
                        print(f"[{worker_id}] ⏭️ 豁免产品线重试计数 | {campaign} | {reason}")
                        return
                    consecutive_fail_count += 1
                    print(f"[{worker_id}] ⚠️ 连续失败计数: {consecutive_fail_count}/2 | {campaign} | {reason}")
                    if consecutive_fail_count >= 2:
                        raise Exception(
                            f"🚨 连续 {consecutive_fail_count} 个Campaign失败，"
                            f"疑似 task complete 未点击导致页面刷新，触发产品线重试"
                        )
                
                print(f"[{worker_id}] 📊 [预期] 本次需要配置 {expected_campaign_count} 个 Campaign")
                
                for index, row in task['campaigns'].reset_index(drop=True).iterrows(): # type: ignore
                    ad_type = row['Ad_product'].upper() # type: ignore
                    base_name = row['Base_campaign_name']
                    campaign_name = row['Campaign_name']
                    current_family = 'SB_FAMILY' if ad_type in ['SB', 'SBV'] else ad_type
                    ad_prefix = 'sb' if ad_type in ['SB', 'SBV'] else ad_type.lower()
                    
                    try:
                        print(f"[{worker_id}] -> 配置 [{ad_type}] {campaign_name}")

                        if current_family == last_ad_family:
                            add_btn = page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-add-campaign-button"]')
                            if await add_btn.is_visible(): 
                                await add_btn.scroll_into_view_if_needed()
                                await add_btn.click(force=True)
                                await page.wait_for_timeout(400) 

                        last_ad_family = current_family

                        check_columns = ['Campaign Name', 'Campaign Name 2', 'Campaign Name 3', 'Campaign Name 4', 'Campaign Name 5', 'Campaign Name 6', 'Campaign Name4', 'Campaign Name6'] 
                        valid_columns = [col for col in check_columns if col in self.df_format.columns]
                        format_rules = self.df_format[self.df_format[valid_columns].isin([base_name]).any(axis=1)]

                        # 🚀 改进：记录被跳过的campaign及原因
                        if format_rules.empty:
                            # 诊断信息：打印format表中实际有什么值
                            actual_format_names: List[str] = []
                            for col in valid_columns:
                                actual_format_names.extend(self.df_format[col].dropna().unique().tolist())

                            preview_names = [actual_format_names[i] for i in range(min(5, len(actual_format_names)))]
                            reason = f"Format表中找不到 Base_campaign_name='{base_name}' | Format表中实际值: {preview_names}"
                            print(f"[{worker_id}] ⏭️ 跳过: {reason}")
                            register_campaign_failure(campaign_name, reason)
                            continue

                        rule = format_rules.iloc[0]

                        def get_val(col_name):
                            val = str(rule.get(col_name, '')).strip()
                            return val if val.lower() != 'nan' and val != '' else None

                        if ad_type in ['SP', 'SD']:
                            if v := get_val('Targeting'): await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-targeting-dropdown"]').nth(-1), v, worker_id=worker_id)
                            if v := get_val('Strategy'): await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-strategy-dropdown"]').nth(-1), v, worker_id=worker_id)
                            if v := (get_val('Strategy Targets') or get_val('Targets')): await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid*="-target"], button[aria-haspopup="listbox"][id^="{ad_prefix}-"][id*="Target"]').nth(-1), v, worker_id=worker_id)

                            # 🚀 新增：检测 Refine Advertised ASINs 是否为 (0 ASIN) - SP/SD
                            if v := get_val('Refine Advertised ASINs'):
                                await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-asin-type-dropdown"]').nth(-1), v, worker_id=worker_id)
                                await page.wait_for_timeout(500)

                                try:
                                    zero_asin_indicator = page.locator('text="(0 ASIN)"')
                                    if await zero_asin_indicator.is_visible():
                                        print(f"[{worker_id}] ⚠️ 检测到该产品线无合格的ASIN [(0 ASIN)]，准备删除此广告...")
                                        register_campaign_failure(campaign_name, "无合格ASIN (0 ASIN)")

                                        trash_btn = None
                                        possible_selectors = [
                                            'button[aria-label*="Delete" i], button[aria-label*="delete" i]',
                                            f'button[data-testid^="{ad_prefix}-"][data-testid*="-delete"], button[data-testid^="{ad_prefix}-"][data-testid*="-remove"]',
                                            'button:has(.far.fa-trash-alt)'
                                        ]

                                        for selector in possible_selectors:
                                            try:
                                                btn = page.locator(selector).nth(-1)
                                                if await btn.is_visible():
                                                    trash_btn = btn
                                                    break
                                            except:
                                                continue

                                        if trash_btn:
                                            await trash_btn.scroll_into_view_if_needed() # type: ignore
                                            await trash_btn.click(force=True) # type: ignore
                                            await page.wait_for_timeout(500)
                                            print(f"[{worker_id}] ✅ 已删除该广告，跳到下一个campaign...")
                                        else:
                                            print(f"[{worker_id}] ⚠️ 未找到删除按钮，但仍继续到下一个campaign...")

                                        continue
                                except Exception as check_err:
                                    pass

                        elif ad_type in ['SB', 'SBV']:
                            if v := get_val('Ad Format'): await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-adformat-dropdown"]').nth(-1), v, worker_id=worker_id)

                            # Landing Page 防崩溃雷达（对齐 Campaign Builder）
                            if v := get_val('Landing Page'):
                                await page.wait_for_timeout(300)
                                landing_btn = page.locator(
                                    'button[aria-haspopup="listbox"][aria-label*="landing" i], button[aria-label*="Landing" i]'
                                ).nth(-1)
                                try:
                                    await landing_btn.wait_for(state="visible", timeout=8000)
                                    await self._click_dropdown_option(page, landing_btn, v, worker_id=worker_id)
                                except PlaywrightTimeoutError:
                                    raise Exception(f"❌ 找不到 Landing Page！请检查 Excel 广告格式或品牌授权。")

                            if v := get_val('Targeting'): await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-targeting-dropdown"]').nth(-1), v, worker_id=worker_id)
                            if v := get_val('Strategy'): await self._click_dropdown_option(page, page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid$="-strategy-dropdown"]').nth(-1), v, worker_id=worker_id)

                            if v := (get_val('Strategy Targets') or get_val('Targets')): 
                                btn = page.locator(f'button[data-testid^="sb-"][data-testid*="-targets-"]').nth(-1)
                                if not await btn.is_visible(): btn = page.locator(f'button[aria-label*="Targets"]').nth(-1)
                                await self._click_dropdown_option(page, btn, v, worker_id=worker_id)

                            # 🚀 新增：检测 Refine Advertised ASINs 是否为 (0 ASIN)
                            if v := get_val('Refine Advertised ASINs'):
                                await self._click_dropdown_option(page, page.locator(f'button[data-testid^="sb-"][data-testid$="-asin-type-dropdown"]').nth(-1), v, worker_id=worker_id)
                                await page.wait_for_timeout(500)

                                # 🔍 检查下拉框中是否出现 "(0 ASIN)" 的提示
                                try:
                                    zero_asin_indicator = page.locator('text="(0 ASIN)"')
                                    if await zero_asin_indicator.is_visible():
                                        print(f"[{worker_id}] ⚠️ 检测到该产品线无合格的ASIN [(0 ASIN)]，准备删除此广告...")

                                        register_campaign_failure(campaign_name, "Refine Advertised ASINs: 无合格ASIN (0 ASIN) - 已删除")

                                        # 💥 点击垃圾桶删除这个campaign
                                        # 使用多个选择器策略查找垃圾桶按钮
                                        trash_btn = None
                                        possible_selectors = [
                                            'button[aria-label*="Delete" i], button[aria-label*="delete" i]',
                                            'button:has(svg [d*="268"] [d*="416"])',  # SVG path matching
                                            f'button[data-testid^="{ad_prefix}-"][data-testid*="-delete"], button[data-testid^="{ad_prefix}-"][data-testid*="-remove"]',
                                            'button:has(.far.fa-trash-alt)'
                                        ]

                                        for selector in possible_selectors:
                                            try:
                                                btn = page.locator(selector).nth(-1)
                                                if await btn.is_visible():
                                                    trash_btn = btn
                                                    break
                                            except:
                                                continue

                                        if trash_btn:
                                            await trash_btn.scroll_into_view_if_needed() # type: ignore
                                            await trash_btn.click(force=True) # type: ignore
                                            await page.wait_for_timeout(500)
                                            print(f"[{worker_id}] ✅ 已删除该广告，跳到下一个campaign...")
                                        else:
                                            print(f"[{worker_id}] ⚠️ 未找到删除按钮，但仍继续到下一个campaign...")

                                        # 跳到下一个campaign（使用continue）
                                        continue
                                except Exception as check_err:
                                    print(f"[{worker_id}] ℹ️ 检查(0 ASIN)时出错: {check_err}，继续正常流程...")
                                    pass


                            if val_creative := get_val('Creative ASINs'):
                                await page.wait_for_timeout(200) 
                                c_btn = page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid*="-creative-asin-multi-select"]:not([disabled])').nth(-1)

                                success_opening_creative_asins = False
                                opt_count = 0
                                options = page.locator(f'button[role="checkbox"][data-testid^="{ad_prefix}-"][data-testid*="-creative-asin-multi-select-item-"]')

                                for click_attempt in range(2):
                                    try:
                                        if click_attempt == 0:
                                            await c_btn.scroll_into_view_if_needed(timeout=5000)
                                        else:
                                            print(f"[{worker_id}] ⚠️ 第 1 次尝试未发现选项，1秒后原位重试点击...")
                                            await page.wait_for_timeout(1000)

                                        await c_btn.click(force=True, timeout=5000)
                                        await page.wait_for_timeout(250)

                                        opt_count = int(await options.count()) # type: ignore
                                        if opt_count > 0:
                                            success_opening_creative_asins = True
                                            break
                                    except Exception as click_err:
                                        pass

                                if not success_opening_creative_asins:
                                    print(f"[{worker_id}] ❌ 无法加载 Creative ASINs 选项，准备删除此Campaign...")
                                    reason = f"点击Creative ASINs位置失败，检查ASIN是否合规"
                                    register_campaign_failure(campaign_name, reason, count_toward_retry=False)

                                    trash_btn = None
                                    possible_selectors = [
                                        'button[aria-label*="Delete" i], button[aria-label*="delete" i]',
                                        f'button[data-testid^="{ad_prefix}-"][data-testid*="-delete"], button[data-testid^="{ad_prefix}-"][data-testid*="-remove"]',
                                        'button:has(.far.fa-trash-alt)'
                                    ]
                                    for selector in possible_selectors:
                                        try:
                                            btn = page.locator(selector).nth(-1)
                                            if await btn.is_visible(timeout=1000):
                                                trash_btn = btn
                                                break
                                        except:
                                            continue

                                    if trash_btn is not None:
                                        await trash_btn.scroll_into_view_if_needed(timeout=3000) # type: ignore
                                        await trash_btn.click(force=True, timeout=3000) # type: ignore
                                        await page.wait_for_timeout(500)
                                        print(f"[{worker_id}] ✅ 已删除该Campaign，继续下一个...")
                                    else:
                                        print(f"[{worker_id}] ⚠️ 未找到删除按钮，但仍继续到下一个Campaign...")
                                    continue

                                if opt_count > 0:
                                    # 🚀 改进逻辑：智能跳过灰色（disabled）的 ASIN，只点击可用的
                                    max_to_click = int(1 if ("第一" in str(val_creative) or "1" in str(val_creative)) else 3)
                                    successful_clicks = int(0)

                                    # 🚀 极致性能：前端批量获取合法且未打勾的 ASIN 选项索引
                                    valid_indices = await options.evaluate_all(f'''(elements) => {{
                                        let results = [];
                                        const maxClick = {max_to_click};
                                        for(let i=0; i<elements.length; i++) {{
                                            if(results.length >= maxClick) break;
                                            const el = elements[i];
                                            let disabled = el.getAttribute('disabled') !== null || el.getAttribute('aria-disabled') === 'true';
                                            if (el.parentElement && el.parentElement.className && el.parentElement.className.toLowerCase().includes('disabled')) disabled = true;
                                            const checked = el.getAttribute('aria-checked') === 'true';
                                            if(!disabled && !checked) results.push(i);
                                        }}
                                        return results;
                                    }}''')
                                    
                                    for idx in valid_indices:
                                        target_option = options.nth(idx)
                                        try:
                                            await target_option.scroll_into_view_if_needed()
                                            target_span = target_option.locator('span').first
                                            if await target_span.is_visible(): 
                                                await target_span.click()
                                            else: 
                                                await target_option.click()
                                            successful_clicks = int(successful_clicks + 1) # type: ignore
                                            # print(f"[{worker_id}] 已点取第 {successful_clicks} 个可用的 Creative ASIN...")
                                            await page.wait_for_timeout(50)
                                        except Exception as click_err:
                                            print(f"[{worker_id}] ⚠️ ASIN 点击失败 (索引 {idx})，尝试下一个...")
                                            continue

                                    # 🎯 检查是否所有ASIN都是灰色
                                    if successful_clicks == 0:
                                        print(f"[{worker_id}] ⚠️ 警告：这个Campaign的Creative ASINs 全部灰色无法选择！")

                                        # 🚀 新逻辑：只重试这个 Campaign 3 次，而不是整个表单
                                        creative_asin_retry = 3
                                        for creative_retry_idx in range(creative_asin_retry):
                                            print(f"[{worker_id}] 🔄 重试第 {creative_retry_idx + 1}/{creative_asin_retry} 次...")
                                            await page.wait_for_timeout(1500)

                                            # 重新点击 Creative ASIN 按钮，尝试重新加载选项
                                            try:
                                                c_btn = page.locator(f'button[data-testid^="{ad_prefix}-"][data-testid*="-creative-asin-multi-select"]:not([disabled])').nth(-1)
                                                await c_btn.scroll_into_view_if_needed()
                                                await c_btn.click(force=True)
                                                await page.wait_for_timeout(250)

                                                # 重新检查是否有可用的ASIN
                                                options = page.locator(f'button[role="checkbox"][data-testid^="{ad_prefix}-"][data-testid*="-creative-asin-multi-select-item-"]')
                                                opt_count = int(await options.count())
                                                retry_successful = False

                                                valid_indices = await options.evaluate_all(f'''(elements) => {{
                                                    let results = [];
                                                    const maxClick = {max_to_click};
                                                    for(let i=0; i<elements.length; i++) {{
                                                        if(results.length >= maxClick) break;
                                                        const el = elements[i];
                                                        let disabled = el.getAttribute('disabled') !== null || el.getAttribute('aria-disabled') === 'true';
                                                        if (el.parentElement && el.parentElement.className && el.parentElement.className.toLowerCase().includes('disabled')) disabled = true;
                                                        const checked = el.getAttribute('aria-checked') === 'true';
                                                        if(!disabled && !checked) results.push(i);
                                                    }}
                                                    return results;
                                                }}''')

                                                if valid_indices:
                                                    for idx in valid_indices:
                                                        target_option = options.nth(idx)
                                                        try:
                                                            await target_option.scroll_into_view_if_needed()
                                                            target_span = target_option.locator('span').first
                                                            if await target_span.is_visible(): 
                                                                await target_span.click()
                                                            else: 
                                                                await target_option.click()
                                                            retry_successful = True
                                                            print(f"[{worker_id}] ✅ 重试成功！ASIN已选择")
                                                            break
                                                        except:
                                                            continue

                                                if retry_successful:
                                                    break
                                            except Exception as retry_err:
                                                print(f"[{worker_id}] ⚠️ 重试失败: {retry_err}")
                                                continue

                                        # 如果重试 3 次都还是全灰，则删除这个 Campaign
                                        if not retry_successful:
                                            print(f"[{worker_id}] ❌ 重试 {creative_asin_retry} 次后仍无法获取可用ASIN，准备删除此Campaign...")

                                            # 记录失败原因
                                            reason = f"Creative ASINs: 重试{creative_asin_retry}次后全部灰色无法选择"
                                            register_campaign_failure(campaign_name, reason)

                                            # 找到并点击删除按钮
                                            trash_btn = None
                                            possible_selectors = [
                                                'button[aria-label*="Delete" i], button[aria-label*="delete" i]',
                                                f'button[data-testid^="{ad_prefix}-"][data-testid*="-delete"], button[data-testid^="{ad_prefix}-"][data-testid*="-remove"]',
                                                'button:has(.far.fa-trash-alt)'
                                            ]

                                            for selector in possible_selectors:
                                                try:
                                                    btn = page.locator(selector).nth(-1)
                                                    if await btn.is_visible():
                                                        trash_btn = btn
                                                        break
                                                except:
                                                    continue

                                            if trash_btn is not None:
                                                await trash_btn.scroll_into_view_if_needed() # type: ignore
                                                await trash_btn.click(force=True) # type: ignore
                                                await page.wait_for_timeout(500)
                                                print(f"[{worker_id}] ✅ 已删除该Campaign，继续下一个...")
                                            else:
                                                print(f"[{worker_id}] ⚠️ 未找到删除按钮，但仍继续到下一个Campaign...")

                                            # 跳到下一个campaign
                                            continue

                                    await page.wait_for_timeout(300)
                                else:
                                    raise Exception("❌ Creative ASINs 按钮点开了，但是没有选项！")

                        # 🎯 campaign配置成功，计数++
                        configured_campaign_count += 1
                        consecutive_fail_count = 0  # 成功则重置连续失败计数
                    except Exception as camp_err:
                        # 🚨 连续失败检测：如果是连续失败触发的重试异常，直接向上抛出触发外层产品线重试
                        if "触发产品线重试" in str(camp_err):
                            raise
                        print(f'[{worker_id}] ⚠️ 广告级别错误，跳过该Campaign跑下一个: {camp_err}')
                        register_campaign_failure(campaign_name, str(camp_err))
                        # 回退：点删除按钮
                        try:
                            trash_btn = None
                            possible_selectors = [
                                'button[aria-label*="Delete" i], button[aria-label*="delete" i]',
                                f'button[data-testid^="{ad_prefix}-"][data-testid*="-delete"], button[data-testid^="{ad_prefix}-"][data-testid*="-remove"]',
                                'button:has(.far.fa-trash-alt)'
                            ]
                            for selector in possible_selectors:
                                try:
                                    btn = page.locator(selector).nth(-1)
                                    if await btn.is_visible(timeout=1000):
                                        trash_btn = btn
                                        break
                                except:
                                    pass
                            if trash_btn:
                                await trash_btn.scroll_into_view_if_needed(timeout=1000) # type: ignore
                                await trash_btn.click(force=True, timeout=1000) # type: ignore
                        except:
                            pass
                        continue
                    print(f"[{worker_id}] ✅ Campaign配置成功 (进度: {configured_campaign_count}/{expected_campaign_count})")

                # 🚀 关键验证：对比配置完成的campaign数量
                print(f"\n[{worker_id}] 📊 [验证阶段]")
                print(f"[{worker_id}]    预期Campaign数: {expected_campaign_count}")
                print(f"[{worker_id}]    已配置Campaign数: {configured_campaign_count}")
                
                if skipped_campaigns:
                    print(f"[{worker_id}]    被跳过的Campaign ({len(skipped_campaigns)}个):")
                    for skip_name, skip_reason in skipped_campaigns.items():
                        print(f"[{worker_id}]      • {skip_name}: {skip_reason}")
                
                if configured_campaign_count != expected_campaign_count:
                    print(f"[{worker_id}] ⚠️ 警告：Campaign数量不匹配！")
                    print(f"[{worker_id}]    差异: {expected_campaign_count - configured_campaign_count} 个")
                    
                    # 在skipped_campaigns中追加这些未配置的campaign
                    missing_count = expected_campaign_count - configured_campaign_count
                    
                    # 尝试从页面上获取当前form中的campaign总数
                    try:
                        campaign_count_elem = page.locator('[data-testid*="campaign-count"], text=/共\\d+个/, text=/Campaign\\s*\\d+/')
                        if await campaign_count_elem.count() > 0:
                            page_campaign_count = await campaign_count_elem.first.inner_text()
                            print(f"[{worker_id}]    页面显示: {page_campaign_count}")
                    except:
                        pass
                    
                    if missing_count == len(skipped_campaigns):
                        print(f"[{worker_id}] ✅ 差异数量（{missing_count}个）完全匹配已知跳过的失败任务数，允许正常发射！")
                    else:
                        error_msg = f"❌ Campaign数量校验失败！预期{expected_campaign_count}个，但只配置了{configured_campaign_count}个，缺少{missing_count}个"
                        
                        # 将这个任务作为整体失败记录
                        self.failed_tasks.append({'product_line': task['product_line'], 'campaign': "所有Campaign", 'reason': error_msg})
                        
                        raise Exception(error_msg)
                else:
                    print(f"[{worker_id}] ✅ Campaign数量校验通过！所有{expected_campaign_count}个Campaign已配置完毕")

                print(f"[{worker_id}] ⏳ 表单全部就绪！移交指挥官...")
                product_line_success = True
                break # 只要循环跑完没报错，直接 Break 退出
                
            except Exception as e:
                print(f"\n[{worker_id}]  -> ⚠️ 创建过程发生意外: {e}")
                if "找不到匹配选项" in str(e): raise e 
                if attempt < max_retries - 1:
                    # 🚀 连续失败或其他异常：尝试关闭当前表单，回到列表页重新开始
                    print(f"[{worker_id}] 🔄 正在关闭当前表单，准备从头重新选择品牌...")
                    try:
                        # 尝试多种方式关闭表单回到列表页
                        close_btn = page.locator('button[aria-label="Close"], button[aria-label="close"], button:has-text("Cancel"), [data-testid*="close"], [data-testid*="cancel"]').first
                        if await close_btn.is_visible(timeout=3000):
                            await close_btn.click(force=True)
                            await page.wait_for_timeout(2000)
                        else:
                            # 没有关闭按钮，尝试按 Escape
                            await page.keyboard.press("Escape")
                            await page.wait_for_timeout(2000)
                    except:
                        pass
                    # 等待回到列表页
                    await page.wait_for_timeout(2000)
                    if await page.locator('[data-testid="Brand-dropdown"]').is_visible(): continue 
                    elif await page.locator('button:has-text("Create customized new campaigns")').first.is_visible():
                        await page.locator('button:has-text("Create customized new campaigns")').first.click(force=True)
                        continue
                    else:
                        # 最后手段：刷新页面
                        print(f"[{worker_id}] ⚠️ 表单关闭失败，尝试刷新页面...")
                        await page.reload()
                        await self.wait_for_create_button(page, worker_id, timeout=30000)
                        continue
                else: raise e 
                
        if not product_line_success: raise Exception(f"❌ 产品线创建彻底失败。")


# 🚀 新增 initial_delay 参数，默认为 0
    async def process_single_task(self, task: Dict[str, Any], worker_id: str, initial_delay: float = 0, max_form_retries: int = 3) -> None:
        """🤖 批次调度员：处理单个任务，同 URL 复用页面不刷新"""
        
        country = task['country']
        pl = task['product_line']
        ad_family = task.get('ad_family', '')
        chunk_idx = task.get('chunk_idx', 1)
        brand = task['brand']
        reuse_key = (brand, country)
        
        print(f"[{worker_id}] 📦 领到新包裹: [{country}] {pl} | {ad_family}族 (第{chunk_idx}段)")
        
        # ===== 核心优化：检查是否有可复用的页面 =====
        reused = False
        if reuse_key in self._reusable_pages:
            task_context, page = self._reusable_pages.pop(reuse_key)
            print(f"[{worker_id}] ♻️ 复用已有页面 [{country}]，跳过导航！")
            reused = True
        else:
            # 创建新的虚拟电脑沙盒
            assert self.headless_browser is not None
            task_context = await self.headless_browser.new_context(viewport={"width": 1920, "height": 10000}) # type: ignore
            if self.login_cookies:
                await task_context.add_cookies(self.login_cookies)
                
            page = await task_context.new_page()

            # 🔽 最小化新窗口（通过 CDP Browser.setWindowBounds）
            try:
                cdp = await task_context.new_cdp_session(page)
                win_info = await cdp.send("Browser.getWindowForTarget")
                await cdp.send("Browser.setWindowBounds", {
                    "windowId": win_info["windowId"],
                    "bounds": {"windowState": "minimized"}
                })
                await cdp.detach()
            except Exception as e:
                print(f"[{worker_id}] ⚠️ 最小化窗口失败（不影响运行）: {e}")

            # ================= 🚀 改进版资源拦截器 =================
            async def block_unnecessary_resources(route):
                url = route.request.url.lower()
                resource_type = route.request.resource_type
                if any(keyword in url for keyword in [
                    "/api/notifications", "/api/recommendations", "/telemetry",
                    "google-analytics", "facebook.com", "doubleclick.net",
                    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg+xml"
                ]):
                    await route.abort()
                elif resource_type in ["image", "media", "font"]:
                    if not any(keyword in url for keyword in ["amazon", "ui", "icon"]):
                        await route.abort()
                    else:
                        await route.continue_()
                else:
                    await route.continue_()
            
            await page.route("**/*", block_unnecessary_resources)
        
        try:
            if not reused:
                # 🚀 错峰起跳（仅新页面需要）
                if initial_delay > 0:
                    print(f"[{worker_id}] 🛡️ 错位起跳：已备好浏览器，随机错机 {initial_delay:.1f} 秒后开始飞跃目标国...")
                    await asyncio.sleep(initial_delay)
                    
                # 1. 过验证、切国家、跳页面
                await self.navigate_and_switch_country(page, task, worker_id)
            else:
                # 复用页面：只需等待 Create 按钮可用
                await self.wait_for_create_button(page, worker_id, timeout=30000)
            
            # 2. 开始核心填表
            await self.fill_campaign_form(page, task, worker_id, max_retries=max_form_retries)

            # 记录已成功配置的 campaign 数（用于全量校验）
            _configured_now = int(len(task.get('campaigns', [])))
            self.configured_campaign_total += _configured_now
            print(f"[{worker_id}] ✅ 本包已配置 {_configured_now} 条，累计已配置 {self.configured_campaign_total}/{self.expected_campaign_total}")
            
            # 3. 填表成功后，推入集结区，并从复用缓存中移除（页面已被占用）
            print(f"[{worker_id}] 📥 表单就绪，已推入全局集结区。")
            self.global_staging_area.append((task_context, page, task['product_line']))
            # 从复用缓存移除，因为这个页面已经进入集结区等待齐射
            self._reusable_pages.pop(reuse_key, None)
            
        except Exception as e:
            print(f"[{worker_id}] ❌ 包裹崩溃放弃: {e}")
            self.failed_task_packages.append({
                'worker_id': worker_id,
                'product_line': task.get('product_line', ''),
                'ad_family': task.get('ad_family', ''),
                'chunk_idx': task.get('chunk_idx', ''),
                'campaign_count': int(len(task.get('campaigns', []))),
                'reason': str(e),
                'task': task,  # 保留原始任务，用于重试
            })
            # 只有在发生异常报错时，才需要当场清理这台废弃的虚拟电脑
            # 成功的页面不清理，留在集结区等中控台齐射完统一清理
            try:
                await page.close()
                await task_context.close()
            except: 
                pass


    async def run_all(self) -> None:
        """🧠 中控台：分批同步齐射调度器 (9并发/批次 -> 齐射 -> 冷却8s)"""
        await self.vanguard_login()
        
        print("\n[系统] 🌐 正在初始化并发专属浏览器 (正常窗口·最小化启动)...")
        assert self.playwright is not None
        self.headless_browser = await self.playwright.chromium.launch( # type: ignore
            executable_path=self.chrome_path,
            headless=False, 
            args=[
                "--disable-blink-features=AutomationControlled",
                "--start-minimized",
                "--window-size=1920,1080",
            ] 
        )
        
        total_tasks = len(self.tasks_list)
        print("\n" + "🔥"*25)
        print(f"🔥 全局批次齐射引擎启动！共有 {total_tasks} 个任务，每批次并发 {self.concurrency} 个...")
        print("🔥"*25 + "\n")
        
        # ===== 核心优化：按 (brand, country) 分组，同组任务顺序执行复用页面 =====
        # 先按 URL 分组，同一 URL 的任务放在一起
        from collections import OrderedDict
        url_groups: OrderedDict[Tuple[str, str], List[Dict[str, Any]]] = OrderedDict()
        for task in self.tasks_list:
            key = (task['brand'], task['country'])
            if key not in url_groups:
                url_groups[key] = []
            url_groups[key].append(task)
        
        # 将分组后的任务重新排列：同组连续，便于页面复用
        sorted_tasks: List[Dict[str, Any]] = []
        for group in url_groups.values():
            sorted_tasks.extend(group)
        
        batches = [[sorted_tasks[j] for j in range(i, min(i + self.concurrency, total_tasks))] for i in range(0, total_tasks, self.concurrency)]
        
        for batch_idx, batch_tasks in enumerate(batches):
            print(f"\n=======================================================")
            print(f"🚀 开始执行第 {batch_idx + 1}/{len(batches)} 批次，本批次包含 {len(batch_tasks)} 个任务")
            print(f"=======================================================\n")
            
            # 每批次开始前，清空全局集结区
            self.global_staging_area = []
            tasks_coroutines = []
            
            for i, task in enumerate(batch_tasks):
                worker_id = f"W-{batch_idx * self.concurrency + i + 1}"
                
                # 🚀 计算每个工人的启动时间（5s 内，每个至少差 1s）
                cumulative_delay = i * 1.0 + random.uniform(0, 0.9)
                
                # 直接将带有延迟参数的协程推入发车区，主程序绝不死等！
                tasks_coroutines.append(self.process_single_task(task, worker_id, initial_delay=cumulative_delay))
                
            # 🛑 瞬间发车！6个协程同时启动，但工人们会在内部各自按秒数依次苏醒开网页
            await asyncio.gather(*tasks_coroutines)
            
            # 💥 齐射结算时间！
            ready_count = len(self.global_staging_area)
            if ready_count > 0:
                print(f"\n🔥🔥🔥 [中控指挥官] 侦测到本批次共有 {ready_count} 个包裹就绪！发动毁天灭地的全军齐射！🔥🔥🔥\n")
                pages = [p for ctx, p, name in self.global_staging_area]
                
                # 瞬间齐射：同一毫秒内按下所有页面的发送按钮
                await asyncio.gather(*[p.locator('[data-testid="send-button"]').click() for p in pages], return_exceptions=True)
                print(f"[中控指挥官] 🎉 齐射指令已瞬间发出！防崩等待后台处理并打扫战场...")

# ================= 🚀 扫尾清理内存 (智能保留版) =================
                # 检查下一批次需要哪些 URL，保留对应的页面
                next_batch_keys: Set[Tuple[str, str]] = set()
                if batch_idx < len(batches) - 1:
                    for t in batches[batch_idx + 1]:
                        next_batch_keys.add((t['brand'], t['country']))

                pages_to_clean = []
                for ctx, p, name in self.global_staging_area:
                    # 找到这个页面对应的 (brand, country) key
                    page_key = None
                    for t in batch_tasks:
                        if t['product_line'] == name:
                            page_key = (t['brand'], t['country'])
                            break
                    
                    # 如果下一批次需要同 URL 且还没保留过，且不超过并发数，保留这个页面
                    if page_key and page_key in next_batch_keys and page_key not in self._reusable_pages and len(self._reusable_pages) < self.concurrency:
                        try:
                            await p.locator('#action-status[data-takt-value*="All workflows completed"]').wait_for(state="visible", timeout=30000)
                        except:
                            pass
                        self._reusable_pages[page_key] = (ctx, p)
                        print(f"  ♻️ 保留页面供下一批次复用: {page_key}")
                    else:
                        pages_to_clean.append((ctx, p))

                async def clean_single_page(ctx, p):
                    try:
                        await p.locator('#action-status[data-takt-value*="All workflows completed"]').wait_for(state="visible", timeout=30000)
                    except:
                        pass
                    finally:
                        try:
                            await p.close()
                            await ctx.close()
                        except:
                            pass

                if pages_to_clean:
                    await asyncio.gather(*[clean_single_page(ctx, p) for ctx, p in pages_to_clean])
                # ==============================================================
                
                print(f"[中控指挥官] 🧹 战场内存瞬间清理完毕！")
            else:
                print(f"⚠️ [中控指挥官] 本批次所有任务均已崩溃，无可用包裹进行齐射。")
            
            # 清理不再需要的复用页面（下一批次用不到的）
            if batch_idx < len(batches) - 1:
                next_batch_keys_cleanup: Set[Tuple[str, str]] = set()
                for t in batches[batch_idx + 1]:
                    next_batch_keys_cleanup.add((t['brand'], t['country']))
                stale_keys = [k for k in self._reusable_pages if k not in next_batch_keys_cleanup]
                for k in stale_keys:
                    ctx_old, p_old = self._reusable_pages.pop(k)
                    try:
                        await p_old.close()
                        await ctx_old.close()
                    except:
                        pass
                    print(f"  🧹 关闭不再需要的复用页面: {k}")
            else:
                # 最后一批，关闭所有复用页面
                for k, (ctx_old, p_old) in list(self._reusable_pages.items()):
                    try:
                        await p_old.close()
                        await ctx_old.close()
                    except:
                        pass
                self._reusable_pages.clear()
                
            # ⏳ 强制冷却机制（如果是最后一批则跳过等待，直接结束）
            if batch_idx < len(batches) - 1:
                print(f"\n⏳ 第 {batch_idx + 1} 批次处理结束。进入强制冷却，挂机等待 20 秒后开启下一批次...\n")
                await asyncio.sleep(20)
        
        # ==================== 失败包裹重试机制 ====================
        if self.failed_task_packages:
            retry_tasks = [pkg['task'] for pkg in self.failed_task_packages if 'task' in pkg]
            if retry_tasks:
                retry_count = len(retry_tasks)
                print(f"\n{'🔄'*25}")
                print(f"🔄 检测到 {retry_count} 个失败包裹，启动重试轮次...")
                print(f"{'🔄'*25}\n")
                
                # 清空失败记录，重试后重新统计
                first_round_failures = list(self.failed_task_packages)
                self.failed_task_packages.clear()
                
                # 按批次重试
                retry_batches = [
                    [retry_tasks[j] for j in range(i, min(i + self.concurrency, retry_count))]
                    for i in range(0, retry_count, self.concurrency)
                ]
                
                for batch_idx, batch_tasks in enumerate(retry_batches):
                    print(f"\n=======================================================")
                    print(f"🔄 重试批次 {batch_idx + 1}/{len(retry_batches)}，本批次包含 {len(batch_tasks)} 个任务")
                    print(f"=======================================================\n")
                    
                    self.global_staging_area = []
                    tasks_coroutines = []
                    
                    for i, task in enumerate(batch_tasks):
                        worker_id = f"R-{batch_idx * self.concurrency + i + 1}"
                        cumulative_delay = i * 1.0 + random.uniform(0, 0.9)
                        tasks_coroutines.append(self.process_single_task(task, worker_id, initial_delay=cumulative_delay, max_form_retries=2))
                    
                    await asyncio.gather(*tasks_coroutines)
                    
                    ready_count = len(self.global_staging_area)
                    if ready_count > 0:
                        print(f"\n🔥 [重试指挥官] {ready_count} 个包裹就绪，齐射！\n")
                        await asyncio.gather(
                            *[p.locator('[data-testid="send-button"]').click() for ctx, p, name in self.global_staging_area],
                            return_exceptions=True
                        )
                        
                        for ctx, p, name in self.global_staging_area:
                            try:
                                await p.locator('#action-status[data-takt-value*="All workflows completed"]').wait_for(state="visible", timeout=30000)
                            except:
                                pass
                            finally:
                                try:
                                    await p.close()
                                    await ctx.close()
                                except:
                                    pass
                    else:
                        print(f"⚠️ [重试指挥官] 本批次重试全部失败。")
                    
                    self._reusable_pages.clear()
                    
                    if batch_idx < len(retry_batches) - 1:
                        print(f"\n⏳ 重试批次冷却 15 秒...\n")
                        await asyncio.sleep(15)
                
                # 统计重试结果
                retry_success = retry_count - len(self.failed_task_packages)
                print(f"\n{'='*60}")
                print(f"🔄 [重试结果] 重试 {retry_count} 个，成功 {retry_success} 个，仍失败 {len(self.failed_task_packages)} 个")
                print(f"{'='*60}")

        # 🎯 输出失败ASIN汇总报告
        print("\n" + "="*60)
        print("📊 [创建任务汇总报告]")
        print("="*60)
        
        if self.failed_tasks:
            from typing import Dict, List
            summary: Dict[str, Dict[str, int]] = {}
            details: Dict[str, List[Dict[str, str]]] = {}
            
            for ft in self.failed_tasks:
                pl = ft['product_line']
                reason = ft['reason']
                if pl not in summary:
                    summary[pl] = {}
                    details[pl] = []
                if reason not in summary[pl]:
                    summary[pl][reason] = 0
                summary[pl][reason] = int(summary[pl][reason] + 1)
                details[pl].append(ft)
                
            print(f"\n❌ 任务未完成统计报表 (共 {len(self.failed_tasks)} 个任务失败):\n")
            for pl, reasons_dict in summary.items():
                print(f"📦 产品线: {pl}")
                for reason, count in reasons_dict.items():
                    print(f"    ⚠️ 原因: {reason} | 数量: {count}个")
                    # 打印具体任务名称
                    failed_camps = [t['campaign'] for t in details[pl] if t['reason'] == reason] # type: ignore
                    for camp in failed_camps:
                        print(f"       - {camp}")
                print("-" * 40)
        else:
            print("\n✅ 所有广告均已成功创建！")

        print("\n📌 [全量校验]")
        print(f"   预期总数: {self.expected_campaign_total}")
        print(f"   已配置总数: {self.configured_campaign_total}")
        print(f"   失败包裹数: {len(self.failed_task_packages)}")
        _missing = self.expected_campaign_total - self.configured_campaign_total
        if _missing != 0 or self.failed_task_packages:
            print(f"❌ 全量校验失败：缺少 {_missing} 条，或存在失败包裹。")
            if self.failed_task_packages:
                print("   失败包裹明细（最多显示前10条）：")
                for _pkg in self.failed_task_packages[:10]:
                    print(
                        f"      - {_pkg.get('worker_id')} | {_pkg.get('product_line')} | "
                        f"{_pkg.get('ad_family')} 第{_pkg.get('chunk_idx')}段 | "
                        f"{_pkg.get('campaign_count')}条 | {_pkg.get('reason')}"
                    )
            raise RuntimeError(
                f"全量创建未完成：expected={self.expected_campaign_total}, "
                f"configured={self.configured_campaign_total}, "
                f"failed_packages={len(self.failed_task_packages)}"
            )
        else:
            print("✅ 全量校验通过：所有 campaign 均已配置完成。")
        
        print("="*60)
        print("\n✅ 所有批次处理完毕！系统即将安全关闭...")

    async def close(self) -> None:
        try:
            if self.playwright is not None:
                await self.playwright.stop() # type: ignore
        except:
            pass

# ================= Main Execution =================
def main():
    print("="*50)
    print("  Amazon Campaign Auto-Creator V5.0 (Ultimate Async)")
    print("="*50)
    
    bot = None
    try:
        processor = DataProcessor(MEDIA_PLAN_PATH, ASIN_INFO_PATH, FORMAT_PATH)
        df_plan, df_asin, df_format = processor.process()
        
        bot = AsyncAmazonAdBot(df_plan, df_asin, df_format, concurrency=5)
        bot.start_local_chrome()
        
        asyncio.run(bot.run_all())
        
    except Exception as e:
        print(f"\n❌ 运行中发生严重错误: {e}")
    finally:
        if bot: asyncio.run(bot.close())
        print("\n🏁 任务结束。")

if __name__ == "__main__":
    main()