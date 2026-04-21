import os
import sys
import re
import time
import tempfile
import asyncio
import pandas as pd
import numpy as np
from typing import List, Dict
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ================= Configuration =================
# Portable path relative to current script
_BASE = os.path.dirname(os.path.abspath(__file__))
CHROME_USER_DATA = os.path.normpath(os.path.join(_BASE, "..", "browser_data"))
if not os.path.exists(CHROME_USER_DATA):
    try: os.makedirs(CHROME_USER_DATA, exist_ok=True)
    except: CHROME_USER_DATA = os.path.join(tempfile.gettempdir(), "sel_chrome_cases")
TARGET_URL = "https://advertising.amazon.com/case-manager?cmPanelView=newCase&cmPanelOpen=false"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "Account information For Case Creator.xlsx"))

# ================= 模块 1: 数据中心 =================
class CaseDataProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        
    def get_tasks(self) -> List[Dict]:
        print("\n[数据中心] 正在读取 Case Information Excel...")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"找不到文件: {self.file_path}")
            
        # 读取 ID information sheet
        df = pd.read_excel(self.file_path, sheet_name='ID information')
        
        # 清理列名，去除前后空格
        df.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in df.columns]

        # ── Auto-fill logic: greedy ffill until the last row with a record type ──
        _last_case_idx = -1
        target_col = 'Case record type*'
        if target_col in df.columns:
            _non_empty = df[df[target_col].notna() & (df[target_col].astype(str).str.strip().str.lower().replace(['nan','none',''], np.nan).notna())].index
            if not _non_empty.empty:
                _last_case_idx = _non_empty.max()

        for col in df.columns:
            if '(auto fill)' in str(col).lower():
                if _last_case_idx != -1:
                    df.loc[:_last_case_idx, col] = df.loc[:_last_case_idx, col].ffill()
                else:
                    df[col] = df[col].ffill()
        
        def _get(row, col, default=''):
            v = str(row.get(col, default) or default).strip()
            return '' if v.lower() in ('nan', 'none') else v

        # 提取所有列数据
        # 任务数判定：以 D 列 'Advertiser ID*' 有值的行数为准
        tasks = []
        for index, row in df.iterrows():
            advertiser_id = _get(row, 'Advertiser ID*')
            if not advertiser_id:
                continue  # D 列没 Advertiser ID 就跳过（不是一个有效 case）

            record_type = _get(row, 'Case record type*')
            account_name = _get(row, 'Advertiser account name')

            tasks.append({
                'task_id': index + 1,
                'record_type': record_type,
                'account_name': account_name,
                'brand_name': _get(row, 'Brand name(optional)'),
                'advertiser_id': advertiser_id,
                'marketplace': _get(row, 'Optimization marketplace*'),
                'optimization_type': _get(row, 'Optimization type'),
                'case_description': _get(row, 'Case description(optional)'),
                'submitting_team': _get(row, 'Submitting team'),
                'optimization_delivery': _get(row, 'Optimization delivery'),
                'case_status': _get(row, 'Case Status(auto fill)'),
                'submitted_by': _get(row, 'Submitted by(auto fill)'),
                'assignee': _get(row, 'Assignee(auto fill)'),
                'submitter_email': _get(row, 'Submitter email address is required(auto fill)'),
            })

        print(f"成功加载 {len(tasks)} 个 Case 创建任务（按 D 列 Advertiser ID 行数判定）！")
        return tasks

# ================= 模块 2: 自动化机器人 =================
class AsyncCaseCreatorBot:
    def __init__(self, tasks, concurrency=5, excel_path=None):
        self.tasks = tasks
        self.concurrency = concurrency
        self.excel_path = excel_path or FILE_PATH
        self.playwright = None
        self.browser = None
        self.chrome_path = None
        self.context = None
        self.page = None  # 复用已有页面
        self.tasks_queue = asyncio.Queue()
        self.completed_count = 0
        self.failed_tasks = []
        self._nav_lock = asyncio.Lock()  # 错峰导航锁
        
        # 将任务推入队列
        for t in self.tasks:
            self.tasks_queue.put_nowait(t)

        # 国家映射字典 (缩写 -> 缩写，用于标准化)
        self.country_mapping = {
            "ES": "ES", "UK": "UK", "GB": "GB",
            "US": "US", "DE": "DE", "FR": "FR", "IT": "IT",
            "JP": "JP", "CA": "CA", "MX": "MX", "AU": "AU",
            "AE": "AE", "BR": "BR", "CL": "CL", "CO": "CO", "EG": "EG",
        }

    def _clean_chrome_cache(self):
        """清理会导致版本降级冲突的缓存目录（保留登录数据）"""
        if not os.path.exists(CHROME_USER_DATA):
            os.makedirs(CHROME_USER_DATA, exist_ok=True)
            return

        # 这些目录/文件会触发 Chrome 降级检测，但不影响登录
        dirs_to_remove = ['ShaderCache', 'Snapshots', 'GrShaderCache', 'GraphiteDawnCache']
        # 同时清理 Chrome 降级失败产生的残留目录
        for item in os.listdir(CHROME_USER_DATA):
            if item.endswith('.CHROME_DELETE'):
                dirs_to_remove.append(item)

        for d in dirs_to_remove:
            target = os.path.join(CHROME_USER_DATA, d)
            if os.path.exists(target):
                try:
                    shutil.rmtree(target)
                    print(f"[清理] 移除缓存目录: {d}")
                except Exception:
                    pass

        # 删除版本标记文件（触发降级检测的根源）
        version_files = ['Last Version', 'Last Browser']
        for vf in version_files:
            vf_path = os.path.join(CHROME_USER_DATA, vf)
            if os.path.exists(vf_path):
                try:
                    os.remove(vf_path)
                    print(f"[清理] 移除版本标记: {vf}")
                except Exception:
                    pass

        # 清理锁文件（包括 LevelDB 的 LOCK 文件）
        for root, dirs, files in os.walk(CHROME_USER_DATA):
            for f in files:
                if f in ['Lockfile', 'SingletonLock', '.lockfile', '.lock', 'LOCK',
                         'DevToolsActivePort']:
                    try:
                        os.remove(os.path.join(root, f))
                    except Exception:
                        pass

        # 清理 Service Worker 注册缓存（防止启动时 unregister 失败导致崩溃）
        sw_dir = os.path.join(CHROME_USER_DATA, 'Default', 'Service Worker')
        if os.path.isdir(sw_dir):
            try:
                shutil.rmtree(sw_dir)
                print('[清理] 移除 Service Worker 缓存')
            except Exception:
                pass

    def _find_chrome(self):
        """查找本地 Google Chrome 可执行文件"""
        possible_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
        ]
        self.chrome_path = next((p for p in possible_paths if os.path.exists(p)), None)
        if not self.chrome_path:
            raise FileNotFoundError("未找到本地 Google Chrome，请安装后重试。")

    async def vanguard_login(self):
        """连接已打开的 Chrome，复用已有页面（不打开新页面）"""
        self.playwright = await async_playwright().start()

        print("[系统] 探测 Chrome 调试端口...")
        from chrome_port_finder import get_cdp_url
        cdp_url = get_cdp_url()

        self.browser = None
        max_retries = 5
        for i in range(max_retries):
            try:
                self.browser = await self.playwright.chromium.connect_over_cdp(
                    cdp_url, timeout=10000
                )
                break
            except Exception:
                if i == max_retries - 1: raise
                await asyncio.sleep(2)

        self.context = self.browser.contexts[0]
        pages = self.context.pages
        print(f"[系统] 共 {len(pages)} 个页面标签")

        # 优先找 case-manager 页面
        target_page = None
        for p in pages:
            url = p.url or ""
            print(f"  - {url[:100]}")
            if "case-manager" in url:
                target_page = p
                break
        
        # 回退：advertising.amazon.com 任意页面
        if target_page is None:
            for p in pages:
                if "advertising.amazon" in (p.url or ""):
                    target_page = p
                    break
        
        if target_page is None:
            raise RuntimeError("未找到 case-manager 页面，请在浏览器中打开 https://advertising.amazon.com/case-manager 后重试")

        self.page = target_page
        print(f"[系统] 选中页面: {self.page.url}")

        # 如果当前不在 case-manager 页面，导航过去
        if "case-manager" not in (self.page.url or ""):
            print("[系统] 当前页面不是 case-manager，正在导航...")
            await self.page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)

        # 检测登录状态
        try:
            await self.page.wait_for_selector('button:has-text("New Case")', state="visible", timeout=8000)
            print("[系统] 登录有效！")
        except:
            print("[系统] 请在浏览器中手动完成登录，等待 New Case 按钮出现...")
            await self.page.wait_for_selector('button:has-text("New Case")', state="visible", timeout=0)
            print("[系统] 登录成功！")

    # ================= 核心交互工具 =================
    async def _type_into_field(self, page, locator, text, delay=20):
        """模拟真实用户输入：click → 全选清空 → 逐字输入，确保 React state 更新"""
        await locator.wait_for(state="visible", timeout=10000)
        await locator.scroll_into_view_if_needed()
        await locator.click()
        await page.wait_for_timeout(200)
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.wait_for_timeout(100)
        await locator.type(text, delay=delay)
        await page.wait_for_timeout(300)

    async def _select_dropdown(self, page, trigger_locator, option_text, exact=False):
        """智能下拉框选择引擎 (支持过滤搜索)"""
        await trigger_locator.wait_for(state="visible", timeout=10000)
        await trigger_locator.scroll_into_view_if_needed()
        await page.wait_for_timeout(300)
        await trigger_locator.click(force=True)
        await page.wait_for_timeout(500)

        # 检查是否有搜索/过滤输入框 (Amazon Storm UI 特性)
        search_input_selectors = [
            'input[type="search"]',
            'input[placeholder*="Search"]',
            'input[placeholder*="Select"]',
            '[role="listbox"] input'
        ]
        
        search_input = None
        for sel in search_input_selectors:
            try:
                found = page.locator(sel).first
                if await found.is_visible():
                    search_input = found
                    break
            except: pass
            
        if search_input:
            await search_input.fill(option_text)
            await page.wait_for_timeout(500)

        # 选择选项
        if exact:
            option = page.locator(f'[role="option"]:has-text("{option_text}")').first
        else:
            # 尝试通过 data-takt-id 匹配 (最稳定)
            takt_id_val = option_text.replace(" ", " ")
            takt_option = page.locator(f'button[data-takt-id*="{takt_id_val}"], [role="option"][value*="{option_text}"]').first
            
            if await takt_option.is_visible():
                option = takt_option
            else:
                option = page.locator('[role="option"]').filter(has_text=option_text).first
            
        await option.wait_for(state="visible", timeout=5000)
        await option.scroll_into_view_if_needed()
        await option.click(force=True)
        await page.wait_for_timeout(300)

    async def _select_marketplace(self, page, country_options):
        """通过点击下拉按钮，在弹出搜索框中输入国家代码后选择"""
        if isinstance(country_options, str):
            country_options = [country_options]
        
        country_code = country_options[0].upper()
        print(f"[选择国家] 尝试选择: {country_code}")
        
        # 1. 点击下拉按钮打开列表
        btn = page.locator('[data-testid="advertiser.marketplaceId-field"]')
        await btn.wait_for(state="visible", timeout=10000)
        await btn.scroll_into_view_if_needed()
        await btn.click()
        await page.wait_for_timeout(400)
        
        # 2. 等待搜索框出现后输入国家代码
        search_input = page.locator('#filter-dropdown-search-advertiser\\.marketplaceId')
        await search_input.wait_for(state="visible", timeout=5000)
        await search_input.fill(country_code)
        await page.wait_for_timeout(500)
        
        # 3. 选择过滤后的第一个选项
        option = page.locator('[role="option"]').filter(has_text=country_code).first
        await option.wait_for(state="visible", timeout=5000)
        await option.click(force=True)
        print(f"[选择国家] 成功选择: {country_code}")
        await page.wait_for_timeout(300)

    async def _staggered_goto(self, page, url, worker_id):
        """优化后的错峰导航：缩短锁占用时间"""
        # 在进锁前先等一个极短的随机时间，分散请求起点
        await asyncio.sleep(random.uniform(0.1, 0.5))
        
        async with self._nav_lock:
            # 获得锁后仅等待 0.5-1.2 秒即发起导航
            delay = random.uniform(0.5, 1.2)
            print(f"[{worker_id}] 错峰导航 (准备加载)...")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            # 导航开始后释放锁，让下一个机器人可以排队导航，而不需要等前一个 load 完
            await asyncio.sleep(delay)

    async def _save_error_screenshot(self, page, task_id, account):
        """发生错误时保存截图，用于诊断"""
        debug_dir = os.path.join(_BASE, "error_debug")
        os.makedirs(debug_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"error_row_{task_id}_{account}_{timestamp}.png"
        filepath = os.path.join(debug_dir, filename)
        try:
            await page.screenshot(path=filepath, full_page=True)
            print(f"  [诊断] 错误截图已保存: {filename}")
        except: pass

    async def process_single_worker(self, worker_id: str):
        """在已有页面上顺序执行所有 Case 创建任务（复用浏览器页面）"""
        page = self.page
        
        # 确保在 case-manager 页面
        if "case-manager" not in (page.url or ""):
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
        
        while True:
            try:
                task = self.tasks_queue.get_nowait()
            except asyncio.QueueEmpty:
                print(f"[{worker_id}] 队列已空，机器人休息。")
                break
            
            print(f"\n[{worker_id}] 开始处理行 {task['task_id']}: [{task['account_name']}]")
            
            try:
                # 1. 点击 New Case 按钮（不刷新页面，超时才刷新重试）
                new_case_btn = page.locator('button:has-text("New Case")').first
                try:
                    await new_case_btn.wait_for(state="visible", timeout=10000)
                except PlaywrightTimeoutError:
                    # New Case 按钮不可见，刷新页面重试
                    print(f"[{worker_id}] New Case 按钮不可见，刷新页面...")
                    await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
                    await new_case_btn.wait_for(state="visible", timeout=20000)
                await new_case_btn.click(force=True)
                
                # 等待侧边栏划出
                await page.wait_for_selector('[data-testid="case-record-type-dropdown"]', state="visible", timeout=10000)
                
                # 2. Case record type
                record_type_dropdown = page.locator('[data-testid="case-record-type-dropdown"]')
                await self._select_dropdown(page, record_type_dropdown, task['record_type'])

                # 3. Assignee
                assignee_val = task.get('assignee', '')
                if assignee_val:
                    assignee_input = page.locator('[data-testid="assignee-field"]')
                    await self._type_into_field(page, assignee_input, assignee_val)
                    await page.keyboard.press("Tab")
                    await page.wait_for_timeout(500)

                # 4. Case Status (Default to Optimization Complete if empty)
                case_status_val = task.get('case_status', '')
                if case_status_val:
                    try:
                        status_takt_id = case_status_val.replace(' ', '-')
                        status_btn = page.locator(f'button[data-takt-id="status:option-{status_takt_id}"]')
                        if await status_btn.is_visible():
                            await status_btn.click()
                        else:
                            status_dropdown = page.locator('[data-takt-id="status:options"]')
                            await self._select_dropdown(page, status_dropdown, case_status_val)
                    except: pass
                await page.wait_for_timeout(300)

                # 5. Case description
                desc_val = task.get('case_description', '')
                if desc_val:
                    desc_input = page.locator('[data-testid="caseDescription-field"]')
                    await self._type_into_field(page, desc_input, desc_val)
                
                # 6. Account name
                account_input = page.locator('[data-testid="accountName-field"]')
                await self._type_into_field(page, account_input, task['account_name'])
                await page.keyboard.press("Tab")
                await page.wait_for_timeout(1000)
                
                # 7. Optimization marketplace (映射国家后选择)
                ui_market = self.country_mapping.get(task['marketplace'].upper(), task['marketplace'])
                await self._select_marketplace(page, ui_market)
                await page.wait_for_timeout(1000)
                
                # 7.5. Brand name (optional, fill after marketplace)
                brand_name_val = task.get('brand_name', '')
                if brand_name_val:
                    brand_input = page.locator('[data-testid="brandName-field"]')
                    try:
                        await self._type_into_field(page, brand_input, brand_name_val)
                    except: pass

                # 8. Advertiser ID — 清洗：去除非法字符，仅保留字母数字和连字符
                raw_adv_id = str(task['advertiser_id'])
                # 去掉 Excel 浮点数后缀 (如 "12345.0" -> "12345")
                if re.match(r'^\d+\.0$', raw_adv_id):
                    raw_adv_id = raw_adv_id.split('.')[0]
                clean_adv_id = re.sub(r'[^a-zA-Z0-9\s\-]', '', raw_adv_id).strip()
                print(f"[{worker_id}] Advertiser ID: '{task['advertiser_id']}' -> '{clean_adv_id}'")
                adv_id_input = page.locator('[data-testid="advertiser.id-field"]')
                await self._type_into_field(page, adv_id_input, clean_adv_id)
                
                # 9. Advertiser type -> SELLER
                seller_radio = page.locator('input#advertiser\\.type-SELLER, input[value="SELLER"]').first
                await seller_radio.click(force=True)
                await page.wait_for_timeout(500)
                
                # 滚动到页面底部确保字段加载
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await page.wait_for_timeout(1000)
                
                # 10. Submitting team (Logic: SSPA vs Accelerator)
                if "SSPA" in task['record_type']:
                    team_dropdown = page.locator('[data-testid="submittedByTeam-field"]')
                    await team_dropdown.wait_for(state="visible", timeout=10000)
                    await team_dropdown.click()
                    await page.wait_for_timeout(300)
                    await page.keyboard.type("GGS")
                    await page.wait_for_timeout(500)
                    # 强力匹配 GGS
                    await page.locator('[role="option"]:has-text("GGS")').first.click(force=True)
                elif "Accelerator" in task['record_type']:
                    # 尝试多种方式定位 AAP CN (根据 HTML 修正为 AAP)
                    selectors = [
                        'input[value="AAP CN"]',
                        'input#submittedByTeam-AAP\ CN',
                        'label:has-text("AAP CN")',
                        'label:has-text("APP CN")' # 容错
                    ]
                    clicked_radio = False
                    for sel in selectors:
                        try:
                            target = page.locator(sel).first
                            if await target.is_visible():
                                await target.click(force=True)
                                clicked_radio = True
                                print(f"[{worker_id}] 已选中单选框: {sel}")
                                break
                        except: pass
                    
                    if not clicked_radio:
                        # 最终尝试模糊寻找
                        await page.locator('input[type="radio"]').filter(has_text="CN").first.click(force=True)
                else:
                    team_val = task.get('submitting_team', '')
                    if team_val:
                        team_dropdown = page.locator('[data-testid="submittedByTeam-field"]')
                        if await team_dropdown.is_visible():
                            await self._select_dropdown(page, team_dropdown, team_val)

                await page.wait_for_timeout(500)

                # 11. Optimization type (增加模糊匹配Keywords/Keyword)
                opt_type_val = task.get('optimization_type', '')
                if opt_type_val:
                    # 如果是 Keywords Reporting，去掉 s 再试
                    if "Keywords" in opt_type_val:
                        opt_type_val = opt_type_val.replace("Keywords", "Keyword")
                    
                    opt_type_dropdown = page.locator('[data-takt-id="optimizationType:options"], [data-testid="optimizationType-field"]')
                    await self._select_dropdown(page, opt_type_dropdown, opt_type_val)
                await page.wait_for_timeout(300)

                # 12. Submitted by 
                submitted_by_val = task.get('submitted_by', '')
                if submitted_by_val:
                    submitted_by_input = page.locator('[data-testid="submittedBy-field"]')
                    await self._type_into_field(page, submitted_by_input, submitted_by_val)

                # 13. Optimization delivery 
                delivery_val = task.get('optimization_delivery', '')
                if delivery_val:
                    delivery_dropdown = page.locator('[data-testid="optimizationDelivery-field"]')
                    await self._select_dropdown(page, delivery_dropdown, delivery_val)

                # 13.5. Submitter email address
                email_val = task.get('submitter_email', '')
                if email_val:
                    email_input = page.locator('[data-testid="submittedByEmailAddress-field"]')
                    await self._type_into_field(page, email_input, email_val)
                
                # 14. Submit
                print(f"[{worker_id}] 表单填写完毕，准备提交...")
                submit_btn = page.locator('button[data-testid="form-submit"]:has-text("Submit")').first
                await submit_btn.wait_for(state="visible", timeout=10000)
                await submit_btn.scroll_into_view_if_needed()
                await page.wait_for_timeout(500)
                await submit_btn.click(force=True)
                print(f"[{worker_id}] ✅ Submit 点击成功")
                await page.wait_for_timeout(3000)
                
                self.completed_count += 1
                print(f"[{worker_id}] 继续下一个任务 (已完成: {self.completed_count}/{len(self.tasks)})")
                
            except Exception as e:
                # 截图保存现场
                await self._save_error_screenshot(page, task['task_id'], task['account_name'])
                self.failed_tasks.append({'task_id': task['task_id'], 'account': task['account_name'], 'error': str(e)[:150]})
                print(f"[{worker_id}] 任务 {task['task_id']} 失败: {str(e)[:150]}")
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(1000)

    async def run_all(self):
        """中控台：复用已有浏览器页面，单机器人顺序执行"""
        await self.vanguard_login()
        
        print("\n" + "="*60)
        print(f"任务引擎启动！共 {len(self.tasks)} 个任务")
        print(f"在已有浏览器页面上执行...")
        print("="*60 + "\n")
        
        # 单机器人顺序执行（复用同一个页面，不支持并发）
        await self.process_single_worker("W-1")

        # 汇总报告
        print("\n" + "="*60)
        print("[Case 创建任务汇总报告]")
        print("="*60)
        print(f"  总任务数: {len(self.tasks)}")
        print(f"  成功完成: {self.completed_count}")
        print(f"  失败任务: {len(self.failed_tasks)}")
        if self.failed_tasks:
            print(f"\n失败明细:")
            for ft in self.failed_tasks:
                print(f"    行 {ft['task_id']} [{ft['account']}]: {ft['error']}")
        else:
            print("\n所有 Case 均已成功创建！")
        print("="*60)
        print("\n所有机器人任务执行完毕！系统安全关闭。")

    async def close(self):
        """不关闭浏览器和页面，保持打开状态（与 Campaign Namer Optimizer 一致）"""
        # 不调用 browser.close() 或 pw.stop()，保持浏览器和页面打开
        pass

# ================= Main =================
# ================= Main =================
async def main_async():
    """将原本的同步 main 包装成全异步，确保共享同一个事件循环"""
    print("="*50)
    print(" Amazon Case Auto-Creator (Queue Mode)")
    print("="*50)
    
    try:
        processor = CaseDataProcessor(FILE_PATH)
        tasks = processor.get_tasks()
        
        if tasks:
            bot = AsyncCaseCreatorBot(tasks, concurrency=1)
            await bot.run_all()
            
    except Exception as e:
        print(f"\n运行异常: {e}")
    finally:
        print("\n任务结束，页面保持打开。")

def main():
    # 全局只允许出现一次 asyncio.run()
    asyncio.run(main_async())

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️ 手动中断，页面保持打开")
    except Exception as e:
        print(f"\n❌ 未捕获异常: {e}")
        print("页面保持打开，可手动检查")