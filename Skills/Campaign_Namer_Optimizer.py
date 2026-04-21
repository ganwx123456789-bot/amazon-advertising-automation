"""
Campaign Namer-Optimizer 合并脚本
================================
将 auton named campaign async.py（命名器）和 test_optimizer.py（优化器）的核心逻辑
合并为统一的自动化流程。

流程：数据加载 → 连接浏览器 → 全局扫描 → Media Plan 对比排除 → 逐行处理
     （ASIN 提取 → 命名判定 → 关键词/受众优化 → 预算和名称修改）
"""

import asyncio
import math
import os
import random
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

# ================= 路径常量 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_PLAN_PATH = os.path.normpath(
    os.path.join(BASE_DIR, "..", "Excel Template", "Media Plan For Campaign Builder.xlsx")
)
ASIN_INFO_PATH = os.path.normpath(
    os.path.join(BASE_DIR, "..", "Excel Template", "ASIN_Input_Template For Campaign Builder.xlsx")
)
FORMAT_PATH = os.path.join(BASE_DIR, "campaign format.xlsx")
KEYWORD_ANALYSIS_DIR = os.path.normpath(
    os.path.join(BASE_DIR, "..", "Reporting-keyword")
)

DEFAULT_BID = 1.35
TOP_KEYWORD_LIMIT = 10
TARGET_AD_TYPES = {"SB", "SBV", "SP", "SD"}


# ==================== 出价配置（手动填写区）====================
BID_CONFIG = {
    "base_cpc": 1,                       # 平均CPC
    "ad_type_coefficient": {                 # 各广告类型基准价格 = base_cpc × 系数
        "SD": 0.8,
        "SB": 0.7,
        "SP": 0.6,
    },
    "target_coefficient": {                  # 各target方式出价 = 广告类型基准 × 系数
        "SD": {"Remarketing_Audience": 1.2, "Product": 0.8},
        "SB": {"KW": 1.0, "Product": 0.8},
        "SP": {"KW": 1.0, "Automatic": 0.8},
    },
    "match_coefficient": {                   # SP匹配方式系数
        "Broad": 0.8, "Phrase": 1.0, "Exact": 1.1,
    },
    "conversion_coefficient": {              # SP转化类型系数
        "高转化": 1.1, "高加购": 1.0, "高潜力": 0.9,
    },
}

# 预算规则
BUDGET_RULES = {"SD": 16, "SB": 10, "SP": 10, "SBV": 10}

# 关键词来源标题映射
SOURCE_HEADINGS = {
    "高转化本品词": "Own - High conversion rate",
    "高转化竞品词": "Competitor - High conversion rate",
    "高加购本品词": "Own - High add-to-cart rate",
    "高加购竞品词": "Competitor - High add-to-cart rate",
    "高潜力本品词": "Own - High potential",
    "高潜力竞品词": "Competitor - High potential",
}

# 非关键词标签映射
NON_KEYWORD_LABELS = {
    "类目定向": "Product", "商品定向": "Product", "产品定向": "Product",
    "受众": "Audience", "受众定向": "Audience",
    "自动": "Auto", "自动投放": "Auto",
}

STOPWORDS = {"a", "an", "and", "at", "by", "for", "from", "in", "of", "on", "or", "the", "to", "with"}

REMARKETING_SUBTYPES = {
    "本品浏览再营销": "本品浏览再营销",
    "相似品浏览再营销": "相似品浏览再营销",
    "类目浏览再营销": "类目浏览再营销",
}


# ==================== 数据类 ====================
@dataclass
class CampaignMetadata:
    campaign_name: str
    product_line: str
    ad_type: str
    optimization_type: str
    keyword_label: Optional[str]
    match_type: Optional[str]
    cost_type: Optional[str]
    remarketing_subtype: Optional[str] = None

    @property
    def is_keyword_campaign(self):
        return self.optimization_type == "Keyword"


@dataclass
class RawMetric:
    keyword: str
    purchase_share: Optional[float]
    atc_share: Optional[float]
    click_purchase_funnel: Optional[float]
    atc_purchase_funnel: Optional[float]
    search_share: Optional[float] = None


@dataclass
class KeywordCandidate:
    keyword: str
    normalized_keyword: str
    source: str                          # "excel" | "system"
    labels: Set[str]
    source_order: int
    purchase_share: Optional[float] = None
    atc_share: Optional[float] = None
    click_purchase_funnel: Optional[float] = None
    atc_purchase_funnel: Optional[float] = None
    search_share: Optional[float] = None
    is_system_keyword: bool = False
    excel_tier: Optional[int] = None
    final_score: float = 0.0
    exclusion_reason: Optional[str] = None


@dataclass
class AudienceRow:
    description: str
    audience_type: str   # "advertised_products" | "similar_products" | "category" | "unknown"
    bid: Optional[float]
    row_index: int


# ==================== 工具函数 ====================
def clean_text(text, keep_space=False):
    """核心数据清洗：保留中文数字英文"""
    if pd.isna(text):
        return ""
    value = str(text).strip()
    if keep_space:
        return re.sub(r"\s+", " ", value)
    return re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9]", "", value)


def normalize_text(text):
    if pd.isna(text):
        return ""
    return re.sub(r"\s+", " ", str(text).strip()).lower()


def tokenize(text):
    value = normalize_text(text)
    if not value:
        return []
    parts = re.split(r"[^\u4e00-\u9fa5a-zA-Z0-9]+", value)
    return [p for p in parts if p and p not in STOPWORDS]


def median_or_default(values, default):
    cleaned = [v for v in values if v is not None]
    return float(statistics.median(cleaned)) if cleaned else default


def dehydrate(s):
    """脱水处理：用于字符串匹配"""
    if pd.isna(s) or s is None:
        return ""
    s_str = str(s).strip()
    if s_str.lower() in ("nan", "none", ""):
        return ""
    return re.sub(r"[^a-zA-Z0-9]", "", s_str).lower()


# ==================== DataProcessor ====================
class DataProcessor:
    """从三个 Excel 文件加载数据，返回 (df_media_plan, df_asin, df_format)"""

    def __init__(self, media_plan_path, asin_info_path, format_path):
        self.media_plan_path = media_plan_path
        self.asin_info_path = asin_info_path
        self.format_path = format_path

    def process(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # 文件校验
        for p in [self.media_plan_path, self.asin_info_path, self.format_path]:
            if not os.path.exists(p):
                print(f"❌ 找不到文件: {p}")
                sys.exit(1)

        # ---- ASIN Info ----
        df_asin = pd.read_excel(self.asin_info_path, sheet_name="ASIN information")
        df_asin.columns = [str(c).strip().replace("\n", "").replace("\r", "") for c in df_asin.columns]
        col_map = {c.replace(" ", "").lower(): c for c in df_asin.columns}
        brand_col = next((col_map[k] for k in col_map if "brandname" in k), "Brand name")
        url_col = next((col_map[k] for k in col_map if "urlsite" in k), "url site")
        pl_col = next((col_map[k] for k in col_map if "productline" in k or "parentasin" in k), "Parent ASIN or Product line")
        country_col = next((col_map[k] for k in col_map if "country" in k), "Country")
        df_asin = df_asin.rename(columns={
            brand_col: "Brand name", url_col: "url site",
            pl_col: "Parent ASIN or Product line", country_col: "Country",
        })
        for col in ["Parent ASIN or Product line", "Country", "Brand name", "url site"]:
            if col in df_asin.columns:
                df_asin[col] = df_asin[col].replace(r"^\s*$", pd.NA, regex=True).ffill()
        df_asin["Parent ASIN or Product line"] = df_asin["Parent ASIN or Product line"].apply(
            lambda v: str(v).strip() if pd.notna(v) else ""
        )
        df_asin["Country"] = df_asin["Country"].apply(clean_text)
        df_asin["Brand name"] = df_asin["Brand name"].apply(clean_text)
        df_asin["url site"] = df_asin["url site"].apply(lambda v: re.sub(r"\s+", "", str(v)) if pd.notna(v) else "")
        if "Child ASIN" in df_asin.columns:
            df_asin["Child ASIN"] = df_asin["Child ASIN"].apply(lambda v: clean_text(v).upper())

        # ---- Campaign Format ----
        df_format = pd.read_excel(self.format_path)
        df_format.columns = [str(c).strip() for c in df_format.columns]

        # ---- Media Plan ----
        wb = pd.ExcelFile(self.media_plan_path)
        rows_list: List[dict] = []
        unique_brands = df_asin["Brand name"].replace("", pd.NA).dropna().unique()
        for brand in unique_brands:
            sheet = next((s for s in wb.sheet_names if clean_text(s) == brand), None)
            if not sheet:
                continue
            df_bp = pd.read_excel(self.media_plan_path, sheet_name=sheet)
            for pl_col_name in df_bp.columns[7:]:
                pl = str(pl_col_name).strip()
                for _, row in df_bp.iterrows():
                    base = str(row.iloc[0]).strip()
                    if not base or base.lower() == "nan":
                        continue
                    if str(row[pl_col_name]).strip().upper() == "Y":
                        rows_list.append({"Brand": brand, "Product_line": pl, "Base_campaign_name": base})
        df_mp = pd.DataFrame(rows_list)
        if not df_mp.empty:
            pl_to_country = df_asin.dropna(subset=["Country"]).set_index("Parent ASIN or Product line")["Country"].to_dict()
            df_mp["Country"] = df_mp["Product_line"].map(pl_to_country)
            df_mp = df_mp.dropna(subset=["Country"])
        return df_mp, df_asin, df_format


# ==================== MediaPlanComparator ====================
class MediaPlanComparator:
    """将扫描结果与 Media Plan 对比，生成排除集合"""

    def __init__(self, df_media_plan: pd.DataFrame):
        self.df_media_plan = df_media_plan

    def compare(self, scanned_names: Set[str]) -> Tuple[Set[str], Set[str]]:
        """
        返回 (exclusion_set, pending_names)
        - exclusion_set: 已存在于页面上的 campaign name
        - pending_names: 尚未存在、需要优化的 campaign name
        """
        # 从 Media Plan 生成所有可能的 campaign name（替换 Productline 占位符）
        media_plan_names: Set[str] = set()
        if self.df_media_plan.empty:
            return set(), set()
        for _, row in self.df_media_plan.iterrows():
            base = str(row["Base_campaign_name"]).strip()
            pl = str(row["Product_line"]).strip()
            # 将模板中的 Productline 替换为实际 product line
            resolved = base.replace("Productline", pl)
            media_plan_names.add(resolved)

        exclusion_set = media_plan_names & scanned_names
        pending_names = media_plan_names - scanned_names
        print(f"  📊 Media Plan 对比: 已排除 {len(exclusion_set)} 个, 待优化 {len(pending_names)} 个")
        return exclusion_set, pending_names


# ==================== CampaignNameParser ====================
class CampaignNameParser:
    """从 campaign name 字符串提取 metadata"""

    def parse(self, name: str) -> CampaignMetadata:
        parts = [p.strip() for p in name.split("_") if p.strip()]
        pl = parts[0] if parts else name
        ad_type = next((p for p in parts if p in {"SD", "SP", "SB", "SBV"}), None)
        # 兼容空格分隔的 campaign name（如系统推荐的 "SP Campaign Recommendation"）
        if not ad_type:
            words = name.split()
            ad_type = next((w for w in words if w in {"SD", "SP", "SB", "SBV"}), "Unknown")
        kw_label = next((p for p in parts if p in SOURCE_HEADINGS), None)
        match_type = next((p for p in parts if p in {"Broad", "Phrase", "Exact"}), None)
        cost_type = next((p for p in parts if p in {"CPC", "CPM", "vCPM"}), None)
        remarketing_subtype = None
        for token in REMARKETING_SUBTYPES:
            if token in name:
                remarketing_subtype = token
                break
        if kw_label:
            opt_type = "Keyword"
        elif remarketing_subtype:
            opt_type = "Remarketing"
        else:
            opt_type = "Unknown"
            for token, mapped in NON_KEYWORD_LABELS.items():
                if token in name:
                    opt_type = mapped
                    break
        return CampaignMetadata(name, pl, ad_type, opt_type, kw_label, match_type, cost_type, remarketing_subtype)


# ==================== ASIN 查找与冲突解决 ====================
def find_product_line_by_asin(asin: str, country: str, df_asin: pd.DataFrame) -> Optional[str]:
    """根据 ASIN + Country 在 df_asin 中查找 product line，未找到返回 None"""
    target_asin = str(asin).strip().upper()
    target_country = dehydrate(country)
    match = df_asin[
        (df_asin["Child ASIN"].astype(str).str.strip().str.upper() == target_asin)
        & (df_asin["Country"].apply(dehydrate) == target_country)
    ]
    if not match.empty:
        unique_pls = match["Parent ASIN or Product line"].dropna().unique().tolist()
        if unique_pls:
            return str(unique_pls[0])
    return None


def resolve_candidate_conflict(candidate_a: str, candidate_b: str, exclusion_set: Set[str]) -> str:
    """双候选冲突解决：一个在 exclusion_set 中则选另一个，都不在则随机选择，选中后加入 exclusion_set"""
    a_excluded = candidate_a in exclusion_set
    b_excluded = candidate_b in exclusion_set
    if a_excluded and not b_excluded:
        chosen = candidate_b
    elif b_excluded and not a_excluded:
        chosen = candidate_a
    elif a_excluded and b_excluded:
        # 都已排除，选第一个（无更好选择）
        chosen = candidate_a
    else:
        # 都不在 exclusion_set 中，随机选择
        chosen = random.choice([candidate_a, candidate_b])
    exclusion_set.add(chosen)
    return chosen


# ==================== KeywordScoringEngine ====================
class KeywordScoringEngine:
    """关键词评分排名引擎：build_candidates() 合并去重，rank() 评分取前10"""

    def __init__(self, metadata: CampaignMetadata, title: str):
        self.metadata = metadata
        self.title = title or ""

    def build_candidates(self, excel_kws, current_kws, raw_metrics):
        """Excel 词 + 系统词合并去重"""
        candidates: Dict[str, KeywordCandidate] = {}
        for i, kw in enumerate(excel_kws):
            nk = normalize_text(kw)
            if not nk:
                continue
            m = raw_metrics.get(nk)
            labels = self._build_excel_labels()
            c = candidates.get(nk)
            if c is None:
                candidates[nk] = KeywordCandidate(
                    kw, nk, "excel", labels, i,
                    m.purchase_share if m else None, m.atc_share if m else None,
                    m.click_purchase_funnel if m else None, m.atc_purchase_funnel if m else None,
                    m.search_share if m else None,
                    excel_tier=self._excel_tier(labels),
                )
                continue
            c.labels.update(labels)
            if m:
                c.purchase_share = m.purchase_share
                c.atc_share = m.atc_share
                c.click_purchase_funnel = m.click_purchase_funnel
                c.atc_purchase_funnel = m.atc_purchase_funnel
                c.search_share = m.search_share
        for i, (kw, _mt, _bid) in enumerate(current_kws):
            nk = normalize_text(kw)
            if not nk:
                continue
            m = raw_metrics.get(nk)
            c = candidates.get(nk)
            if c is None:
                candidates[nk] = KeywordCandidate(
                    kw, nk, "system", {"system"}, i,
                    m.purchase_share if m else None, m.atc_share if m else None,
                    m.click_purchase_funnel if m else None, m.atc_purchase_funnel if m else None,
                    m.search_share if m else None,
                    is_system_keyword=True,
                )
                continue
            if c.excel_tier == 1:
                continue
            c.source = "system"
            c.is_system_keyword = True
            c.labels.add("system")
            if c.excel_tier == 3:
                c.labels.discard("high_potential")
        for c in candidates.values():
            if "own" in c.labels and "competitor" in c.labels:
                c.labels.discard("competitor")
        return list(candidates.values())

    def rank(self, candidates):
        """Core_Share × Funnel × Relevance × Exclusion × Search_Weight 评分，取前 10"""
        hb = [
            self._core(c) for c in candidates
            if not c.is_system_keyword and self._core(c) is not None and self._core(c) > 0
        ]
        sys_base = median_or_default(hb, 1.0)
        exc_heads = self._detect_competitor_heads(candidates)
        for c in candidates:
            cb = self._core(c)
            # 系统词冷启动：Core_Share_Base 为 None 或 0 时，用 Excel 词中位数兜底
            if c.is_system_keyword and (cb is None or cb == 0):
                cb = sys_base
            elif cb is None:
                cb = 1.0
            fm = self._funnel(c) or 1.0
            # 系统词冷启动：Funnel 为 0 时，中性期望 1.0
            if c.is_system_keyword and fm == 0:
                fm = 1.0
            rel = self._relevance(c)
            exc = self._exclusion(c, exc_heads)
            ss = c.search_share if c.search_share is not None else 0.0
            search_weight = 1.0 + math.log(1.0 + ss * 100) if ss > 0 else 1.0
            c.final_score = cb * fm * rel * exc * search_weight
            if exc == 0.0:
                c.exclusion_reason = "competitor_brand_exclusion"
        ranked = [c for c in candidates if c.final_score > 0]
        ranked.sort(key=lambda c: (-c.final_score, c.source_order, c.keyword.lower()))
        return ranked[:TOP_KEYWORD_LIMIT]

    def _build_excel_labels(self):
        labels = {"excel"}
        kl = self.metadata.keyword_label or ""
        if "本品" in kl:
            labels.add("own")
        if "竞品" in kl:
            labels.add("competitor")
        if "高转化" in kl:
            labels.add("high_conversion")
        if "高加购" in kl:
            labels.add("high_atc")
        if "高潜力" in kl:
            labels.add("high_potential")
        return labels

    @staticmethod
    def _excel_tier(labels):
        if "high_conversion" in labels or "high_atc" in labels:
            return 1
        if "high_potential" in labels:
            return 3
        return None

    @staticmethod
    def _core(c):
        if "high_atc" in c.labels:
            return c.atc_share * 100 if c.atc_share is not None else None
        return c.purchase_share * 100 if c.purchase_share is not None else None

    @staticmethod
    def _funnel(c):
        if "high_atc" in c.labels:
            return c.atc_purchase_funnel
        return c.click_purchase_funnel

    def _relevance(self, c):
        tt = set(tokenize(self.title))
        kt = set(tokenize(c.keyword))
        if not tt or not kt:
            return 0.5
        overlap = kt & tt
        if overlap == kt:
            return 3.0
        if overlap:
            return 1.5
        return 0.5 if not c.is_system_keyword else 0.0

    def _detect_competitor_heads(self, candidates):
        """竞品品牌词动态排异"""
        tt = tokenize(self.title)
        if not tt:
            return set()
        safe = set(tt) | STOPWORDS
        suspect_words = set()
        for c in candidates:
            if "competitor" not in c.labels:
                continue
            for token in tokenize(c.keyword):
                if token not in safe:
                    suspect_words.add(token)
        if not suspect_words:
            return set()
        head_freq: Dict[str, int] = {}
        for c in sorted(candidates, key=lambda x: x.source_order):
            if "competitor" not in c.labels:
                continue
            ct = tokenize(c.keyword)
            if not ct:
                continue
            h = ct[0]
            if h in suspect_words:
                head_freq[h] = head_freq.get(h, 0) + 1
        excluded = {t for t, n in head_freq.items() if n >= 2}
        if excluded:
            print(f"  🚫 竞品品牌词排异: {excluded}")
        return excluded

    @staticmethod
    def _exclusion(c, exc_heads):
        if "competitor" not in c.labels:
            return 1.0
        t = tokenize(c.keyword)
        if t and t[0] in exc_heads:
            return 0.0
        return 1.0


# ==================== KeywordAnalysisResolver ====================
class KeywordAnalysisResolver:
    """发现并加载 keyword analysis workbooks"""

    def __init__(self, working_dir, df_asin):
        self.working_dir = Path(working_dir)
        self.df_asin = df_asin

    def discover_workbooks(self):
        return sorted([
            p for p in self.working_dir.glob("*.xlsx")
            if "keyword analysis" in p.name.lower() and not p.name.startswith("~$")
        ])

    def find_brand_for_product_line(self, pl):
        match = self.df_asin[self.df_asin["Parent ASIN or Product line"].astype(str).str.strip() == pl]
        if match.empty:
            return None
        brands = match["Brand name"].dropna().astype(str).str.strip().tolist()
        return brands[0] if brands else None

    def select_workbook(self, pl):
        brand = self.find_brand_for_product_line(pl)
        if not brand:
            raise ValueError(f"无法为产品线找到品牌: {pl}")
        norm_brand = clean_text(brand).lower()
        scored = []
        for wb in self.discover_workbooks():
            score = 0
            norm_name = clean_text(wb.stem).lower()
            if norm_brand and norm_brand in norm_name:
                score += 2
            if wb.name.lower().startswith("internal_"):
                score += 1
            scored.append((score, wb))
        if not scored:
            raise FileNotFoundError("目录中未发现 Keyword analysis 文件")
        scored.sort(key=lambda x: (-x[0], x[1].name.lower()))
        if scored[0][0] <= 0:
            raise FileNotFoundError(f"未找到与品牌匹配的 Keyword analysis 文件: {brand}")
        return scored[0][1]

    def select_sheet(self, wb_path, pl):
        wb = pd.ExcelFile(wb_path)
        norm_pl = clean_text(pl).lower()
        for s in wb.sheet_names:
            if norm_pl and norm_pl in clean_text(s).lower():
                return s
        raise ValueError(f"文件 {wb_path.name} 中找不到产品线 sheet: {pl}")

    def extract_keywords(self, wb_path, sheet_name, heading):
        df = pd.read_excel(wb_path, sheet_name=sheet_name, header=None)
        known = set(SOURCE_HEADINGS.values())
        positions = []
        for r in range(df.shape[0]):
            for c in range(df.shape[1]):
                v = df.iat[r, c]
                if pd.isna(v):
                    continue
                if str(v).strip() == heading:
                    positions.append((r, c))
        if not positions:
            raise ValueError(f"sheet {sheet_name} 中找不到标题: {heading}")
        r, c = positions[0]
        results: List[str] = []
        blank = 0
        scan = r + 1
        while scan < df.shape[0]:
            v = df.iat[scan, c]
            text = str(v).strip() if pd.notna(v) else ""
            if text in known:
                break
            if not text:
                blank += 1
                if blank >= 3 and results:
                    break
                scan += 1
                continue
            blank = 0
            results.append(text)
            scan += 1
        return results

    def load_raw_metrics(self, wb_path, product_line=None, asin_category=None):
        raw = pd.read_excel(wb_path, sheet_name="Raw Data")
        raw.columns = [str(c).strip() for c in raw.columns]
        # 按产品线过滤
        pl_col = next((c for c in raw.columns if normalize_text(c) in {"product line", "productline"}), None)
        if pl_col and product_line:
            norm_pl = clean_text(product_line).lower()
            raw = raw[raw[pl_col].apply(lambda v: clean_text(v).lower()) == norm_pl]
            if raw.empty:
                raise ValueError(f"{wb_path.name} Raw Data 中没有产品线 '{product_line}' 的数据")
            print(f"  📊 Raw Data 过滤产品线 '{product_line}': {len(raw)} 行")
        # 按 ASIN Category 过滤
        cat_col = next((c for c in raw.columns if normalize_text(c) in {"asin category", "asincategory"}), None)
        if cat_col and asin_category:
            raw = raw[raw[cat_col].astype(str).str.strip() == asin_category]
            if raw.empty:
                raise ValueError(f"过滤 ASIN Category='{asin_category}' 后无数据")
            print(f"  📊 过滤 ASIN Category='{asin_category}': {len(raw)} 行")
        kw_col = next((c for c in raw.columns if "keyword" in normalize_text(c) or "searchterm" in normalize_text(c)), None)
        ps_col = next((c for c in raw.columns if normalize_text(c) in {"purchase share", "purchaseshare"}), None)
        atc_col = next((c for c in raw.columns if normalize_text(c) in {"atc share", "atcshare"}), None)
        cpf_col = next((c for c in raw.columns if "clickpurchasefunnel" in normalize_text(c)), None)
        apf_col = next((c for c in raw.columns if "atcpurchasefunnel" in normalize_text(c)), None)
        ss_col = next((c for c in raw.columns if normalize_text(c) in {"search share", "searchshare"}), None)
        if kw_col is None:
            raise ValueError(f"{wb_path.name} 的 Raw Data 没找到关键词列")
        metrics: Dict[str, RawMetric] = {}
        for _, row in raw.iterrows():
            kw = str(row.get(kw_col, "")).strip()
            if not kw:
                continue
            nk = normalize_text(kw)
            metrics[nk] = RawMetric(
                kw,
                self._f(row.get(ps_col)) if ps_col else None,
                self._f(row.get(atc_col)) if atc_col else None,
                self._f(row.get(cpf_col)) if cpf_col else None,
                self._f(row.get(apf_col)) if apf_col else None,
                self._f(row.get(ss_col)) if ss_col else None,
            )
        return metrics

    @staticmethod
    def _f(v):
        if pd.isna(v):
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


# ==================== 受众优化纯逻辑 ====================
def classify_audience_type(description: str) -> str:
    """描述是多行文本，需要全文匹配"""
    text = description.replace("\n", " ")
    if "Similar to advertised products" in text:
        return "similar_products"
    if "Advertised products" in text:
        return "advertised_products"
    if "Category:" in text:
        return "category"
    return "unknown"


def filter_audiences_by_subtype(audiences, remarketing_subtype):
    if remarketing_subtype == "本品浏览再营销":
        to_retain = [a for a in audiences if a.audience_type == "advertised_products"]
        to_delete = [a for a in audiences if a.audience_type != "advertised_products"]
    elif remarketing_subtype == "相似品浏览再营销":
        to_retain = [a for a in audiences if a.audience_type == "similar_products"]
        to_delete = [a for a in audiences if a.audience_type != "similar_products"]
    elif remarketing_subtype == "类目浏览再营销":
        to_retain = [a for a in audiences if a.audience_type == "category"]
        to_delete = [a for a in audiences if a.audience_type in ("advertised_products", "similar_products")]
    else:
        to_retain, to_delete = audiences, []
    return to_retain, to_delete


def score_category_audience(category_text: str, title: str) -> float:
    cat_tokens = set(tokenize(category_text))
    title_tokens = set(tokenize(title))
    if not cat_tokens or not title_tokens:
        return 0.0
    return len(cat_tokens & title_tokens) / len(cat_tokens)


def rank_and_trim_categories(category_audiences, title, min_keep=3, max_keep=5):
    scored = [(a, score_category_audience(a.description, title)) for a in category_audiences]
    scored.sort(key=lambda x: -x[1])
    n = len(scored)
    keep_count = min(n, max_keep) if n >= min_keep else n
    return [a for a, _ in scored[:keep_count]], [a for a, _ in scored[keep_count:]]


def _detect_conversion_label(keyword_label):
    if not keyword_label:
        return None
    if "高转化" in keyword_label:
        return "高转化"
    if "高加购" in keyword_label:
        return "高加购"
    if "高潜力" in keyword_label:
        return "高潜力"
    return None


def _map_optimization_to_target_key(metadata):
    mapping = {"Keyword": "KW", "Auto": "Automatic", "Product": "Product", "Remarketing": "Remarketing_Audience"}
    return mapping.get(metadata.optimization_type, "KW")


def calculate_bid(metadata: CampaignMetadata) -> float:
    """根据 BID_CONFIG 和 CampaignMetadata 计算最终出价"""
    base = BID_CONFIG["base_cpc"]
    ad_coeff = BID_CONFIG["ad_type_coefficient"].get(metadata.ad_type, 1.0)
    ad_base = base * ad_coeff
    if metadata.ad_type == "SP" and metadata.optimization_type == "Keyword":
        target_coeff = BID_CONFIG["target_coefficient"]["SP"].get("KW", 1.0)
        match_coeff = BID_CONFIG["match_coefficient"].get(metadata.match_type or "Broad", 1.0)
        conv_label = _detect_conversion_label(metadata.keyword_label)
        conv_coeff = BID_CONFIG["conversion_coefficient"].get(conv_label, 1.0) if conv_label else 1.0
        result = ad_base * target_coeff * match_coeff * conv_coeff
    elif metadata.optimization_type == "Remarketing":
        target_coeff = BID_CONFIG["target_coefficient"].get(metadata.ad_type, {}).get("Remarketing_Audience", 1.0)
        result = ad_base * target_coeff
    else:
        target_key = _map_optimization_to_target_key(metadata)
        target_coeff = BID_CONFIG["target_coefficient"].get(metadata.ad_type, {}).get(target_key, 1.0)
        result = ad_base * target_coeff
    result = round(result, 2)
    return result


# ==================== 浏览器连接 ====================
async def connect_browser_and_find_page(pw):
    """自动探测端口，连接 Chrome，查找 advertising.amazon 页面"""
    from chrome_port_finder import get_cdp_url
    cdp_url = get_cdp_url()
    browser = await pw.chromium.connect_over_cdp(cdp_url)
    context = browser.contexts[0]
    pages = context.pages
    print(f"  共 {len(pages)} 个页面标签")
    page = None
    for p in pages:
        url = p.url or ""
        print(f"    - {url[:80]}")
        if "advertising.amazon" in url or "global-action-center" in url:
            page = p
    if not page:
        # 回退到最后一个非 chrome:// 页面
        for p in reversed(pages):
            if not (p.url or "").startswith("chrome://"):
                page = p
                break
    if not page:
        page = pages[-1]
    print(f"  选中页面: {page.url}")
    return browser, page


async def release_browser_memory(page):
    """释放浏览器内存"""
    try:
        await page.evaluate("""() => {
            if (window.gc) window.gc();
            var containers = document.querySelectorAll('.ag-body-viewport');
            containers.forEach(function(c) {
                c.dispatchEvent(new Event('scroll'));
            });
        }""")
    except Exception:
        pass


# ==================== Scanner ====================
class Scanner:
    """全局扫描 AG_Grid 表格中所有 campaign name"""

    STATUS_SELECTOR = "#tactical-recommendations-table\\:pagination-page-status"
    NEXT_BTN_SELECTOR = 'button[id="tactical-recommendations-table:pagination-next"]'

    async def scan_all_campaign_names(self, page) -> Set[str]:
        """从第一行开始逐行向下滚动读取 campaign name，支持分页"""
        all_names: Set[str] = set()
        total_results = 0
        enable_pagination = False

        # 等待表格加载
        try:
            await page.wait_for_selector(".ag-center-cols-container > .ag-row", timeout=15000)
        except PlaywrightTimeoutError:
            print("  ❌ 页面上没找到表格行")
            return all_names

        # 读取分页状态
        try:
            status_el = page.locator(self.STATUS_SELECTOR).first
            if await status_el.count() > 0:
                status_text = (await status_el.inner_text()).strip()
                m = re.search(r"(\d+)\s*-\s*(\d+)\s+of\s+(\d+)\s+results", status_text, re.IGNORECASE)
                if m:
                    total_results = int(m.group(3))
                    enable_pagination = total_results > 100
                    print(f"  📊 分页状态: {status_text} (总计 {total_results})")
        except Exception:
            pass

        if total_results == 0:
            rows = page.locator(".ag-center-cols-container > .ag-row")
            total_results = await rows.count()
            print(f"  可见 {total_results} 行")

        # 逐页扫描
        current_page = 1
        while True:
            page_names = await self._scan_current_page(page, total_results)
            all_names.update(page_names)
            print(f"  📄 第 {current_page} 页扫描到 {len(page_names)} 个 campaign name")

            if not enable_pagination:
                break

            # 检查是否还有下一页
            next_btn = page.locator(self.NEXT_BTN_SELECTOR).first
            if await next_btn.count() == 0 or await next_btn.is_disabled():
                break

            # 翻页
            before_status = ""
            try:
                status_el = page.locator(self.STATUS_SELECTOR).first
                if await status_el.count() > 0:
                    before_status = (await status_el.inner_text()).strip()
            except Exception:
                pass

            await next_btn.scroll_into_view_if_needed()
            await next_btn.click(force=True)
            await page.wait_for_timeout(3000)

            # 等待分页状态变化
            for _ in range(20):
                try:
                    status_el = page.locator(self.STATUS_SELECTOR).first
                    if await status_el.count() > 0:
                        new_status = (await status_el.inner_text()).strip()
                        if new_status and new_status != before_status:
                            break
                except Exception:
                    pass
                await page.wait_for_timeout(500)

            # 滚动到顶部
            try:
                await page.evaluate("""() => {
                    const vp = document.querySelector('.ag-body-viewport');
                    if (vp) vp.scrollTop = 0;
                }""")
            except Exception:
                pass
            await page.wait_for_timeout(1000)
            current_page += 1

        print(f"  ✅ 扫描完成: 共 {len(all_names)} 个唯一 campaign name")
        if all_names:
            for n in sorted(all_names):
                print(f"    - {n[:80]}")
        
        # 扫描完成后回到第一页并滚动到顶部
        if current_page > 1:
            print(f"  🔄 扫描结束在第 {current_page} 页，正在回到第 1 页...")
            prev_btn_selector = 'button[id="tactical-recommendations-table:pagination-prev"]'
            for _ in range(current_page - 1):
                prev_btn = page.locator(prev_btn_selector).first
                if await prev_btn.count() > 0 and not await prev_btn.is_disabled():
                    await prev_btn.click(force=True)
                    await page.wait_for_timeout(2000)
                else:
                    break
        # 滚动到顶部
        try:
            await page.evaluate("""() => {
                const vp = document.querySelector('.ag-body-viewport');
                if (vp) vp.scrollTop = 0;
            }""")
        except Exception:
            pass
        await page.wait_for_timeout(1000)
        
        return all_names

    async def _scan_current_page(self, page, max_rows) -> Set[str]:
        """用 AG_Grid API 批量提取当前页所有 campaign name，秒级完成"""
        names: Set[str] = set()
        
        # 方法1：通过 AG_Grid 内部 API 直接获取所有行数据（最快）
        try:
            all_names = await page.evaluate("""() => {
                const results = [];
                // 尝试获取 AG_Grid 实例
                const gridEl = document.querySelector('.ag-root-wrapper');
                if (gridEl && gridEl.__agComponent) {
                    const api = gridEl.__agComponent.gridOptions.api;
                    if (api) {
                        api.forEachNode(node => {
                            if (node.data && node.data.campaignName) {
                                results.push(node.data.campaignName);
                            }
                        });
                        if (results.length > 0) return results;
                    }
                }
                // 备用：遍历所有 ag-row 的 DOM
                const rows = document.querySelectorAll('.ag-center-cols-container > .ag-row');
                rows.forEach(row => {
                    const cell = row.querySelector("[col-id='campaignName'] .cell-renderer-content-text");
                    if (cell && cell.innerText) {
                        results.push(cell.innerText.trim());
                    }
                });
                return results;
            }""")
            if all_names:
                names.update(n for n in all_names if n)
                if len(names) > 0:
                    return names
        except Exception:
            pass

        # 方法2：如果 JS API 失败，用滚动方式逐行读取
        idx = 0
        consecutive_miss = 0
        while idx < max_rows and consecutive_miss < 15:
            row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{idx}']").first
            if await row.count() == 0 or not await row.is_visible():
                found = False
                for _ in range(5):
                    await page.mouse.wheel(0, 600)
                    await page.wait_for_timeout(400)
                    if await row.count() > 0 and await row.is_visible():
                        found = True
                        break
                if not found:
                    # 滚动后再用 JS 批量抓一次当前 DOM 中的所有行
                    try:
                        batch = await page.evaluate("""() => {
                            const results = [];
                            document.querySelectorAll('.ag-center-cols-container > .ag-row').forEach(row => {
                                const cell = row.querySelector("[col-id='campaignName'] .cell-renderer-content-text");
                                if (cell && cell.innerText) results.push(cell.innerText.trim());
                            });
                            return results;
                        }""")
                        names.update(n for n in batch if n)
                    except Exception:
                        pass
                    consecutive_miss += 1
                    idx += 1
                    continue
            consecutive_miss = 0
            try:
                name_cell = row.locator("[col-id='campaignName'] .cell-renderer-content-text").first
                if await name_cell.count() > 0:
                    text = (await name_cell.inner_text(timeout=2000)).strip()
                    if text:
                        names.add(text)
            except Exception:
                pass
            idx += 1
        return names


# ==================== ASINResolver ====================
class ASINResolver:
    """点蓝色按钮 → 提取 ASIN/title → 查找 product line → 判定 campaign name → 冲突解决"""

    def __init__(self, df_asin, df_format, df_media_plan, exclusion_set: Set[str]):
        self.df_asin = df_asin
        self.df_format = df_format
        self.df_media_plan = df_media_plan
        self.exclusion_set = exclusion_set
        self.naming_tracker: Dict[str, Dict[str, int]] = {}

    async def resolve(self, page, row, campaign_name: str, metadata: CampaignMetadata, country: str) -> Tuple[Optional[str], Optional[str], List[str]]:
        """
        返回 (final_campaign_name, title, asins)
        点蓝色按钮 → 提取 ASIN + title → 查找 product line → 判定 campaign name
        """
        asins, title = await self._open_popup_and_read_info(page, row, campaign_name)
        if not asins:
            return None, None, []

        # 从弹窗内读取真实的 Ad product 类型（SP/SB/SD）
        real_ad_type = metadata.ad_type
        try:
            ad_label = page.locator("label[title='SP'], label[title='SB'], label[title='SD'], label[title='SBV']").first
            if await ad_label.count() > 0:
                real_ad_type = (await ad_label.get_attribute("title") or "").strip()
                if real_ad_type:
                    print(f"  🔍 弹窗内 Ad product: {real_ad_type}")
        except Exception:
            pass

        # 从弹窗内读取关键词的 match type（Broad/Phrase/Exact）
        # 尝试从弹窗内的 Keyword tab 或 AG_Grid 数据中读取
        popup_match_type = None
        try:
            # 方法1：直接从当前可见的 matchType 列读取
            mt_cell = page.locator("[col-id='matchType'] .cell-renderer-content-text").first
            if await mt_cell.count() > 0 and await mt_cell.is_visible():
                popup_match_type = (await mt_cell.inner_text(timeout=2000)).strip()
        except Exception:
            pass
        if not popup_match_type:
            try:
                # 方法2：用 JS 从所有 matchType 单元格中读取第一个
                popup_match_type = await page.evaluate("""() => {
                    const cells = document.querySelectorAll("[col-id='matchType'] .cell-renderer-content-text");
                    for (const c of cells) {
                        const t = (c.textContent || '').trim();
                        if (t === 'Broad' || t === 'Phrase' || t === 'Exact') return t;
                    }
                    return null;
                }""")
            except Exception:
                pass
        if not popup_match_type:
            try:
                # 方法3：尝试切到 Keyword tab 读取，然后切回来
                kw_btn = page.locator("button#KEYWORDS").first
                if await kw_btn.count() > 0 and await kw_btn.is_visible():
                    await kw_btn.click()
                    await page.wait_for_timeout(800)
                    mt_cell = page.locator("[col-id='matchType'] .cell-renderer-content-text").first
                    if await mt_cell.count() > 0:
                        popup_match_type = (await mt_cell.inner_text(timeout=2000)).strip()
            except Exception:
                pass
        if popup_match_type:
            print(f"  🔍 弹窗内 Match Type: {popup_match_type}")

        # 查找 product line
        product_line = None
        for asin in asins:
            pl = find_product_line_by_asin(asin, country, self.df_asin)
            if pl:
                product_line = pl
                break
        if not product_line:
            print(f"  ⚠️ 未找到 ASIN {asins} 的产品线")
            return None, title, asins

        # 判定 campaign name（用弹窗内检测到的真实 ad_type 和 match_type）
        final_name = self._determine_campaign_name(product_line, metadata, real_ad_type, popup_match_type)
        return final_name, title, asins

    def _determine_campaign_name(self, product_line: str, metadata: CampaignMetadata, real_ad_type: str = "", popup_match_type: str = None, campaign_type_filter: str = None) -> Optional[str]:
        """根据 product line + format 表 + media plan + match_type 判定 campaign name
        
        campaign_type_filter: 可选，用于 SD 类型下筛选候选名
          - "Remarketing": 只保留含"再营销"的候选名
          - "Product": 只保留含"ASIN定向"或"类目"的候选名（排除再营销）
        """
        ad_type = real_ad_type if real_ad_type else metadata.ad_type
        print(f"  🔍 判定命名: product_line={product_line}, ad_type={ad_type}, match_type={popup_match_type}, type_filter={campaign_type_filter}")

        # 根据 ad_type 匹配 format 表
        potential_names = self._match_format_table(ad_type, metadata)
        if not potential_names:
            print(f"  ⚠️ Format 表无匹配规则 (ad_type={ad_type})")
            return None

        # 按 campaign_type_filter 筛选（Remarketing vs Product）
        if campaign_type_filter == "Remarketing":
            filtered = [n for n in potential_names if "再营销" in n]
            if filtered:
                print(f"  📋 按类型 'Remarketing' 筛选: {len(filtered)} 个候选")
                potential_names = filtered
        elif campaign_type_filter == "Product":
            filtered = [n for n in potential_names if "再营销" not in n]
            if filtered:
                print(f"  📋 按类型 'Product' 筛选: {len(filtered)} 个候选")
                potential_names = filtered
        
        # 如果检测到了 match_type，优先筛选包含该 match_type 的候选名
        if popup_match_type:
            mt_filtered = [n for n in potential_names if popup_match_type in n]
            if mt_filtered:
                print(f"  📋 按 Match Type '{popup_match_type}' 筛选: {len(mt_filtered)} 个候选")
                potential_names = mt_filtered
            else:
                print(f"  ⚠️ 无候选名包含 Match Type '{popup_match_type}'，使用全部 {len(potential_names)} 个候选")
        
        print(f"  📋 Format 表匹配到 {len(potential_names)} 个候选: {potential_names[:3]}...")

        # 从 Media Plan 验证授权命名
        authorized_names = self._filter_authorized_names(potential_names, product_line)
        if not authorized_names:
            # 调试：打印 media plan 中该产品线的所有记录
            pl_records = self.df_media_plan[self.df_media_plan["Product_line"] == product_line]
            if pl_records.empty:
                # 尝试模糊匹配
                all_pls = self.df_media_plan["Product_line"].unique().tolist()
                print(f"  ⚠️ Media Plan 中找不到 Product_line='{product_line}'")
                print(f"  📋 Media Plan 中所有 Product_line: {all_pls[:10]}")
                # 尝试 clean_text 匹配
                target_clean = clean_text(product_line).lower()
                for pl in all_pls:
                    if clean_text(pl).lower() == target_clean:
                        print(f"  🔄 模糊匹配到: '{pl}' (原始: '{product_line}')")
                        product_line = pl
                        authorized_names = self._filter_authorized_names(potential_names, product_line)
                        break
            else:
                existing_bases = pl_records["Base_campaign_name"].unique().tolist()
                print(f"  ⚠️ 该规则的命名均未在 Media Plan 中打 Y")
                print(f"  📋 Media Plan 中 '{product_line}' 的 Base_campaign_name: {existing_bases[:5]}")
                print(f"  📋 Format 表候选: {potential_names[:5]}")
        
        if not authorized_names:
            return None

        # 从授权名中选择第一个不在 exclusion_set 中的
        chosen_template = None
        for name in authorized_names:
            resolved = str(name).replace("Productline", str(product_line))
            if resolved not in self.exclusion_set:
                chosen_template = name
                break
        
        if not chosen_template:
            print(f"  ⚠️ 所有授权名均已在 exclusion_set 中，无可用名字")
            return None

        final_name = str(chosen_template).replace("Productline", str(product_line))
        self.exclusion_set.add(final_name)

        return final_name

    def _match_format_table(self, ad_type: str, metadata: CampaignMetadata) -> List[str]:
        """从 format 表匹配命名规则，返回所有匹配行的所有候选名（去重）"""
        df_clean = self.df_format.copy()
        df_clean["c_ad_prod"] = df_clean["Ad product"].apply(dehydrate)

        ad_prod_map = {"SP": "sponsoredproduct", "SB": "sponsoredbrand", "SBV": "sponsoredbrand", "SD": "sponsoreddisplay"}
        target_ad_prod = ad_prod_map.get(ad_type, "")

        matched = self.df_format[df_clean["c_ad_prod"].str.contains(target_ad_prod, na=False)]
        if matched.empty:
            return []

        name_cols = ["Campaign Name", "Campaign Name 2", "Campaign Name 3",
                     "Campaign Name 4", "Campaign Name 5", "Campaign Name 6"]
        # 遍历所有匹配行，收集所有候选名（去重保序）
        seen = set()
        potential_names = []
        for _, format_row in matched.iterrows():
            for col in name_cols:
                val = format_row.get(col)
                if pd.notna(val) and str(val).strip():
                    name = str(val).strip()
                    if name not in seen:
                        seen.add(name)
                        potential_names.append(name)
        return potential_names

    def _filter_authorized_names(self, potential_names: List[str], product_line: str) -> List[str]:
        """在 Media Plan 中查找并验证授权名字"""
        authorized = []
        for name in potential_names:
            plan_match = self.df_media_plan[
                (self.df_media_plan["Product_line"] == product_line)
                & (self.df_media_plan["Base_campaign_name"] == name)
            ]
            if not plan_match.empty:
                authorized.append(name)
        return authorized

    async def _open_popup_and_read_info(self, page, row, campaign_name: str) -> Tuple[List[str], Optional[str]]:
        """点蓝色数字按钮 → 自动检测弹窗类型 → 读 ASIN + title"""
        blue_btn = row.locator("[col-id='numberOfAsinGroups'] button").first
        if await blue_btn.count() == 0:
            print("  ❌ 没找到蓝色数字按钮")
            return [], None
        await blue_btn.click(force=True)
        await page.wait_for_timeout(800)

        # 自动检测弹窗内有哪些 tab，不依赖 campaign name
        # 可能的 tab: PRODUCTS, LANDING_PAGE, CREATIVE, KEYWORDS, AUDIENCE_TARGETS
        has_products = await page.locator("button#PRODUCTS").count() > 0
        has_landing_page = await page.locator("button#LANDING_PAGE").count() > 0
        has_creative = await page.locator("button#CREATIVE").count() > 0

        # 策略：优先从能看到 ASIN 的 tab 读取
        if has_landing_page:
            # SB/SBV 普通模式：先看 Landing page
            print("  📋 检测到 Landing Page tab，从此读取...")
            lp_tab = page.locator("button#LANDING_PAGE").first
            await lp_tab.click()
            await page.wait_for_timeout(600)
            # 检查 landing page type，如果是 Home page 则切到 Creative
            try:
                lp_type_el = page.locator("h5:has-text('Landing page type:')").locator("..").locator("~ div > p").first
                if await lp_type_el.count() > 0:
                    raw_lp = (await lp_type_el.inner_text()).strip()
                    if "store" in raw_lp.lower() or "home page" in raw_lp.lower():
                        if has_creative:
                            print("  📋 Landing page 是 Home page，切到 Creative tab...")
                            await page.locator("button#CREATIVE").first.click()
                            await page.wait_for_timeout(1000)
            except Exception:
                pass
        elif has_products:
            # Products tab（SP 或 SD Product/Remarketing 模式）
            print("  📋 检测到 Products tab...")
            await page.locator("button#PRODUCTS").first.click()
            await page.wait_for_timeout(600)
        else:
            # 没有明确的 tab，可能 ASIN 直接显示在默认页面
            print("  📋 未检测到特定 tab，尝试直接读取...")
            await page.wait_for_timeout(600)

        # 读取 ASIN（通用逻辑，3 次重试）
        asins = []
        for attempt in range(3):
            try:
                await page.wait_for_selector("p:has-text('ASIN:')", state="visible", timeout=10000)
                asins_text = await page.locator("p:has-text('ASIN:')").all_inner_texts()
                asins = [t.replace("ASIN:", "").strip() for t in asins_text if "ASIN:" in t]
                if asins:
                    break
            except PlaywrightTimeoutError:
                if attempt < 2:
                    await page.wait_for_timeout(500)

        # 从 <a href*="/dp/"> 链接读取 title + 提取 ASIN
        titles = []
        asin_links = page.locator("a[href*='/dp/']")
        link_count = await asin_links.count()
        for j in range(link_count):
            try:
                text = (await asin_links.nth(j).inner_text()).strip()
                href = await asin_links.nth(j).get_attribute("href") or ""
                if text and len(text) > 5:
                    titles.append(text)
                asin_match = re.search(r"/dp/([A-Z0-9]{10})", href)
                if asin_match and asin_match.group(1) not in asins:
                    asins.append(asin_match.group(1))
            except Exception:
                continue

        combined_title = " ".join(titles) if titles else None
        return asins, combined_title


# ==================== KeywordOptimizer ====================
class KeywordOptimizer:
    """整合关键词优化完整链路：读词 → 评分排名 → 弹窗内替换"""

    def __init__(self, resolver: KeywordAnalysisResolver, ranking_cache: dict):
        self.resolver = resolver
        self.ranking_cache = ranking_cache
        # 跨 label 追踪每个产品线内已被"纯 system"词占用过的关键词
        # key=product_line, value=set(小写关键词)
        # 跨 match type 不限制（因为 Broad/Phrase/Exact 本来就是同一词的不同匹配方式）
        self.used_system_keywords: Dict[str, Set[str]] = {}
        # 记录哪些 (product_line, label) 已经应用过 → 再次命中同 label 不重复限制
        self.applied_cache_keys: Set[str] = set()

    async def optimize(self, page, row, metadata: CampaignMetadata, title: Optional[str]) -> bool:
        """切 Keyword tab → 读现有词 → 评分排名 → 弹窗内替换"""
        # 读取现有关键词
        current_keywords = await self._read_keywords_from_popup(page)
        if not current_keywords:
            print(f"  ⚠️ 没读取到关键词")
            return False
        print(f"  🔑 当前关键词: {len(current_keywords)} 个")

        # 生成 cache key：用 product_line + keyword_label 精确区分
        # 确保高转化/高潜力/高加购 + 本品/竞品 各自独立缓存
        cache_key = f"{metadata.product_line}::{metadata.keyword_label}"
        print(f"  🔑 Cache key: {cache_key}")

        if cache_key in self.ranking_cache:
            ranked = self.ranking_cache[cache_key]
            print(f"  ⚡ 命中缓存！复用已计算的 {len(ranked)} 个关键词 (label={metadata.keyword_label})")
        else:
            try:
                if not metadata.keyword_label or metadata.keyword_label not in SOURCE_HEADINGS:
                    print(f"  ❌ keyword_label 无效: '{metadata.keyword_label}'，无法选词")
                    return False
                wb_path = self.resolver.select_workbook(metadata.product_line)
                print(f"    workbook: {wb_path.name}")
                sheet = self.resolver.select_sheet(wb_path, metadata.product_line)
                print(f"    sheet: {sheet}")
                heading = SOURCE_HEADINGS[metadata.keyword_label]
                print(f"    📌 选词标题: '{heading}' (来自 keyword_label='{metadata.keyword_label}')")
                excel_kws = self.resolver.extract_keywords(wb_path, sheet, heading)
                print(f"    Excel 候选词: {len(excel_kws)} 个")
                kw_label = metadata.keyword_label or ""
                if "本品" in kw_label:
                    asin_cat = "Child ASIN"
                elif "竞品" in kw_label:
                    asin_cat = "Competitor ASIN"
                else:
                    asin_cat = None
                raw_metrics = self.resolver.load_raw_metrics(wb_path, product_line=metadata.product_line, asin_category=asin_cat)
                print(f"    Raw Data 指标: {len(raw_metrics)} 个词有数据")
                scoring = KeywordScoringEngine(metadata, title)
                candidates = scoring.build_candidates(excel_kws, current_keywords, raw_metrics)
                print(f"    候选池: {len(candidates)} 个")
                ranked = scoring.rank(candidates)
                print(f"    排名结果: {len(ranked)} 个")
                self.ranking_cache[cache_key] = ranked
            except (FileNotFoundError, ValueError) as e:
                print(f"  ❌ 关键词数据加载失败: {e}")
                return False

        if not ranked:
            print(f"  ⚠️ 排名结果为空")
            return False

        # ===== system 词跨 label 去重 =====
        # 同一产品线内，一个 system 词被用过一次（任意 label+任意 match），其他 label 就不能再用
        # 但同一 label 下不同 match type 不限制
        product_line_key = metadata.product_line or ""
        if cache_key in self.applied_cache_keys:
            # 同一个 (product_line, label) 再次命中（通常是不同 match type），不二次过滤
            print(f"  � 同 label 再次应用，跳过 system 词去重过滤")
            filtered_ranked = ranked
        else:
            used_set = self.used_system_keywords.get(product_line_key, set())
            filtered_ranked = []
            skipped_names = []
            for c in ranked:
                # 只限制"纯 system"词（source=system 且 Excel 里没有该词）
                is_pure_system = (c.source == "system") and ("excel" not in c.labels)
                kw_lower = c.keyword.lower()
                if is_pure_system and kw_lower in used_set:
                    skipped_names.append(c.keyword)
                    continue
                filtered_ranked.append(c)
            if skipped_names:
                print(f"  🚫 system 词跨 label 去重: 排除 {len(skipped_names)} 个已被占用的词: {skipped_names[:5]}")

        if not filtered_ranked:
            print(f"  ⚠️ 过滤后无可用关键词")
            return False

        print(f"  �📊 最终使用 {len(filtered_ranked)} 词:")
        for rank_i, c in enumerate(filtered_ranked, 1):
            print(f"    {rank_i:02d}. {c.keyword} | score={c.final_score:.2f} | src={c.source}")

        # 记录本次使用的 system 词（仅当是新 cache_key 时）
        if cache_key not in self.applied_cache_keys:
            used_set = self.used_system_keywords.setdefault(product_line_key, set())
            for c in filtered_ranked:
                is_pure_system = (c.source == "system") and ("excel" not in c.labels)
                if is_pure_system:
                    used_set.add(c.keyword.lower())
            self.applied_cache_keys.add(cache_key)

        # 执行弹窗内替换
        await self._apply_keyword_optimization(page, filtered_ranked, metadata.match_type, metadata, is_sp=(metadata.ad_type == "SP"))
        return True

    async def _read_keywords_from_popup(self, page) -> List[Tuple[str, str, Optional[float]]]:
        """切到 Keyword tab，读取所有现有关键词"""
        print("  🔄 切换到 Keyword tab...")
        kw_btn = page.locator("button#KEYWORDS").first
        if await kw_btn.count() > 0 and await kw_btn.is_visible():
            await kw_btn.click()
            await page.wait_for_timeout(800)
        else:
            kw_tab = page.get_by_role("tab", name=re.compile(r"^Keyword$", re.IGNORECASE)).first
            if await kw_tab.count() > 0 and await kw_tab.is_visible():
                await kw_tab.click()
                await page.wait_for_timeout(800)
            else:
                print("  ❌ 没找到 Keyword tab")
                return []

        kw_cell_selector = "[col-id='keywordText'] .cell-renderer-content-text"
        try:
            await page.wait_for_selector(kw_cell_selector, state="visible", timeout=10000)
        except PlaywrightTimeoutError:
            return []

        kw_cells = page.locator(kw_cell_selector)
        row_count = await kw_cells.count()
        results = []
        for j in range(row_count):
            try:
                kw = (await kw_cells.nth(j).inner_text()).strip()
                if not kw:
                    continue
                row_el = kw_cells.nth(j).locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
                mt = ""
                bid = None
                try:
                    mt_el = row_el.locator("[col-id='matchType'] .cell-renderer-content-text").first
                    if await mt_el.count() > 0:
                        mt = (await mt_el.inner_text()).strip()
                except Exception:
                    pass
                try:
                    bid_el = row_el.locator("[col-id='bid'] .cell-renderer-content-text").first
                    if await bid_el.count() > 0:
                        bid_text = (await bid_el.inner_text()).strip()
                        bid_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", bid_text)
                        bid = float(bid_match.group(1)) if bid_match else None
                except Exception:
                    pass
                results.append((kw, mt, bid))
            except Exception:
                continue
        return results

    async def _apply_keyword_optimization(self, page, ranked, match_type, metadata, is_sp=False):
        """
        新逻辑（参照 SD Product 模式）：
        1. 读第一行关键词（保留占位）
        2. 删除除第一行外的所有行
        3. 判断第一行关键词是否在 ranked 列表里
           - 重复：输入 ranked 去掉重复词 → Save → 改第一行 bid
           - 不重复：输入全部 ranked → Save → 删除第一行
        """
        kw_cells = page.locator("[col-id='keywordText'] .cell-renderer-content-text")
        row_count = await kw_cells.count()
        print(f"  📋 当前弹窗内关键词: {row_count} 个")

        # 读取第一行关键词（保留作为占位）
        first_kw_text = ""
        try:
            if row_count > 0:
                first_kw_text = (await kw_cells.nth(0).inner_text()).strip()
                print(f"  📌 保留第一行关键词: {first_kw_text}")
        except Exception:
            pass

        # 步骤1: 选中除第一行外的所有行并删除
        if row_count > 1:
            print(f"  🗑️ 删除第2~{row_count}行...")
            select_all = page.locator(
                "#tactical-recommendations-table\\:tactical-recommendations-table\\:bulkActions\\:selectAllCheckbox"
            ).first
            await select_all.click(force=True)
            await page.wait_for_timeout(800)
            first_row_el = kw_cells.nth(0).locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
            first_cb = first_row_el.locator(
                "[col-id='selectRow'] input[type='checkbox'], [col-id='select'] input[type='checkbox']"
            ).first
            if await first_cb.count() > 0:
                await first_cb.click(force=True)
                await page.wait_for_timeout(800)
            rm_btn = page.get_by_role("button", name=re.compile(r"Remove keywords", re.IGNORECASE)).first
            for _ in range(10):
                if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                    break
                await page.wait_for_timeout(500)
            await rm_btn.click(force=True)
            await page.wait_for_timeout(1500)

        # 步骤2: 判断第一行关键词是否在 ranked 列表里（忽略大小写）
        first_kw_lower = first_kw_text.lower()
        has_overlap = bool(first_kw_lower) and any(c.keyword.lower() == first_kw_lower for c in ranked)
        if has_overlap:
            print(f"  🔗 第一行 '{first_kw_text}' 与 ranked 列表重复，输入时跳过该词")
            keywords_to_input = [c for c in ranked if c.keyword.lower() != first_kw_lower]
        else:
            print(f"  ✨ 第一行 '{first_kw_text}' 与 ranked 列表无重复，完整输入后再删第一行")
            keywords_to_input = list(ranked)

        # 步骤3: 点击 Add keywords 打开输入面板
        print(f"  ➕ 添加 {len(keywords_to_input)} 个新关键词...")
        add_btn = page.get_by_role("button", name=re.compile(r"Add keywords", re.IGNORECASE)).first
        add_ok = False
        for attempt in range(3):
            try:
                await add_btn.wait_for(state="visible", timeout=10000)
                await page.wait_for_timeout(500 + attempt * 500)
                await add_btn.click(force=True)
                await page.wait_for_selector("#add_keywords_input", timeout=10000)
                add_ok = True
                break
            except PlaywrightTimeoutError:
                await page.wait_for_timeout(2000)
        if not add_ok:
            print("  ❌ Add keywords 3次重试均失败")
            return

        await page.wait_for_timeout(500)
        # 选择 match type
        mt_map = {
            "Broad": "#add_keywords_checkbox_broad",
            "Phrase": "#add_keywords_checkbox_phrase",
            "Exact": "#add_keywords_checkbox_exact",
        }
        target_sel = mt_map.get(match_type or "Broad", "#add_keywords_checkbox_broad")
        for sel in mt_map.values():
            cb = page.locator(sel).first
            if await cb.count() == 0:
                continue
            checked = await cb.is_checked()
            should = sel == target_sel
            if checked != should:
                await cb.click(force=True)
            await page.wait_for_timeout(200)

        # 填 bid
        bid_value = calculate_bid(metadata)
        await page.locator("#add_keywords_bid").click()
        await page.keyboard.press("Control+A")
        await page.keyboard.press("Backspace")
        await page.locator("#add_keywords_bid").type(str(bid_value), delay=20)
        await page.wait_for_timeout(300)

        # 填关键词
        if keywords_to_input:
            await page.locator("#add_keywords_input").fill("\n".join(c.keyword for c in keywords_to_input))
            await page.wait_for_timeout(500)
            save_btn = page.get_by_role("button", name=re.compile(r"Save", re.IGNORECASE)).last
            await save_btn.click(force=True)
            await page.wait_for_timeout(2000)
        else:
            # 没有新词要加（所有 ranked 都和第一行重复），直接关闭输入面板
            print(f"  ⚠️ 无新词可加（全部重复或 ranked 为空），关闭输入面板")
            cancel_btn = page.get_by_role("button", name=re.compile(r"^Cancel$", re.IGNORECASE)).last
            if await cancel_btn.count() > 0:
                await cancel_btn.click(force=True)
                await page.wait_for_timeout(1000)

        # 错误处理：检测错误提示框（Invalid keyword / Maximum 10 words 等）
        if keywords_to_input:
            for retry in range(3):
                error_keywords = set()
                try:
                    error_texts = await page.evaluate("""() => {
                        const results = [];
                        const errorBoxes = document.querySelectorAll('.sc-storm-ui-30103358__sc-j3wigw-0');
                        for (const box of errorBoxes) {
                            const h4 = box.querySelector('h4');
                            if (h4) {
                                const lis = box.querySelectorAll('li p');
                                for (const p of lis) {
                                    results.push(p.textContent.trim());
                                }
                            }
                        }
                        if (results.length === 0) {
                            const h4s = document.querySelectorAll('h4');
                            for (const h4 of h4s) {
                                const text = h4.textContent || '';
                                if (text.includes('Invalid') || text.includes('Maximum') || text.includes('Error')) {
                                    const container = h4.closest('div').parentElement;
                                    if (container) {
                                        const lis = container.querySelectorAll('li p');
                                        for (const p of lis) {
                                            results.push(p.textContent.trim());
                                        }
                                    }
                                }
                            }
                        }
                        return results;
                    }""")
                    for raw in error_texts:
                        for prefix in ["Broad ", "Phrase ", "Exact "]:
                            if raw.startswith(prefix):
                                raw = raw[len(prefix):]
                                break
                        if raw:
                            error_keywords.add(raw.lower())
                except Exception:
                    pass
                
                if not error_keywords:
                    break
                
                print(f"  ⚠️ 检测到 {len(error_keywords)} 个问题关键词，删除后重试: {list(error_keywords)[:3]}...")
                valid_keywords = [c.keyword for c in keywords_to_input if c.keyword.lower() not in error_keywords]
                input_box = page.locator("#add_keywords_input")
                await input_box.fill("\n".join(valid_keywords))
                await page.wait_for_timeout(500)
                save_btn = page.get_by_role("button", name=re.compile(r"Save", re.IGNORECASE)).last
                await save_btn.click(force=True)
                await page.wait_for_timeout(2000)

        await page.wait_for_timeout(1500)

        # 步骤4: 根据是否重复，处理保留的第一行
        if has_overlap:
            # 重复：修改保留的那行 bid 为计算值
            print(f"  💰 修改保留行 '{first_kw_text}' 的 bid 为 {bid_value}...")
            await self._modify_keyword_bid(page, first_kw_text, bid_value)
        elif first_kw_text:
            # 不重复：删除保留的第一行
            print(f"  🗑️ 删除第一行占位词 '{first_kw_text}'...")
            await self._delete_keyword_row(page, first_kw_text)

        print("  ✅ 关键词替换完成")

    async def _modify_keyword_bid(self, page, keyword_text, new_bid):
        """找到指定关键词的行，修改其 bid
        
        注意：AG Grid 有 pinned-left 和 center 两个容器，避免重复匹配。
        用 JS 一次性拿到所有 center 容器里的行数据，找到目标行的 row-index，
        然后用 row-index 精确定位该行的 bid trigger。
        """
        target_lower = keyword_text.strip().lower()
        # 用 JS 在 center 容器里找到目标关键词所在的 row-index
        row_index_str = await page.evaluate("""(target) => {
            const rows = document.querySelectorAll('.ag-center-cols-container > .ag-row');
            for (const row of rows) {
                const cell = row.querySelector("[col-id='keywordText'] .cell-renderer-content-text");
                if (cell) {
                    const text = (cell.innerText || '').trim().toLowerCase();
                    if (text === target) {
                        return row.getAttribute('row-index');
                    }
                }
            }
            return null;
        }""", target_lower)

        if row_index_str is None:
            print(f"  ⚠️ 未找到关键词 '{keyword_text}' 来修改 bid")
            return False

        # 用 row-index 精确定位目标行（center 容器）
        target_row = page.locator(
            f".ag-center-cols-container > .ag-row[row-index='{row_index_str}']"
        ).first
        try:
            await target_row.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)
            bid_trigger = target_row.locator(
                "[data-e2e-id*='cell-bid:edit'], "
                "[col-id='bid'] .cell-renderer-trigger"
            ).first
            if await bid_trigger.count() == 0:
                bid_cell = target_row.locator("[col-id='bid']").first
                await bid_cell.click(force=True)
            else:
                await bid_trigger.click(force=True)
            await page.wait_for_timeout(800)
            bid_inp = page.locator("input[type='number']").last
            await bid_inp.wait_for(state="visible", timeout=8000)
            await bid_inp.click()
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            await bid_inp.type(str(new_bid))
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(1000)
            print(f"  ✅ 已修改 '{keyword_text}' (row={row_index_str}) bid 为 {new_bid}")
            return True
        except PlaywrightTimeoutError:
            print(f"  ⚠️ 修改 '{keyword_text}' bid 超时")
            return False
        except Exception as e:
            print(f"  ⚠️ 修改 '{keyword_text}' bid 失败: {e}")
            return False

    async def _delete_keyword_row(self, page, keyword_text):
        """找到指定关键词的行，勾选后删除"""
        for scroll_attempt in range(5):
            kw_cells = page.locator("[col-id='keywordText'] .cell-renderer-content-text")
            count = await kw_cells.count()
            for i in range(count):
                try:
                    text = (await kw_cells.nth(i).inner_text()).strip()
                    if text == keyword_text:
                        row_el = kw_cells.nth(i).locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
                        await row_el.scroll_into_view_if_needed()
                        await page.wait_for_timeout(500)
                        cb = row_el.locator("input[type='checkbox']").first
                        if await cb.count() > 0:
                            await cb.click(force=True)
                            await page.wait_for_timeout(500)
                        rm_btn = page.get_by_role("button", name=re.compile(r"Remove keywords", re.IGNORECASE)).first
                        for _ in range(10):
                            if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                                break
                            await page.wait_for_timeout(500)
                        if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                            await rm_btn.click(force=True)
                            await page.wait_for_timeout(1500)
                            print(f"  ✅ 已删除 '{keyword_text}'")
                            return True
                except Exception:
                    continue
            # 没找到就滚动一下再试
            await page.evaluate("""() => {
                const vp = document.querySelector('.ag-body-viewport');
                if (vp) vp.scrollTop = vp.scrollHeight;
            }""")
            await page.wait_for_timeout(800)
        print(f"  ⚠️ 未找到关键词 '{keyword_text}' 来删除")
        return False


# ==================== AudienceOptimizer ====================
class AudienceOptimizer:
    """SD 再营销受众优化"""

    async def optimize(self, page, row, metadata: CampaignMetadata, bid: float) -> bool:
        subtype = metadata.remarketing_subtype
        print(f"  🎯 SD再营销子类型: {subtype}")

        title = None
        if subtype == "类目浏览再营销":
            print("  📋 类目再营销: 先从 Product tab 读取 ASIN title...")
            products_tab = page.locator("button#PRODUCTS").first
            if await products_tab.count() > 0:
                await products_tab.click(force=True)
                await page.wait_for_timeout(1000)
                titles = []
                asin_links = page.locator("a[href*='/dp/']")
                for j in range(await asin_links.count()):
                    try:
                        text = (await asin_links.nth(j).inner_text()).strip()
                        if text and len(text) > 5:
                            titles.append(text)
                    except Exception:
                        continue
                title = " ".join(titles) if titles else None

        if not await self._navigate_to_audience_tab(page):
            return False
        audiences = await self._read_audiences(page)
        if not audiences:
            print("  ⚠️ 没有audience行，跳过")
            return False

        if subtype == "本品浏览再营销":
            await self._delete_audiences_except(page, "advertised_products")
            await self._modify_all_bids(page, bid)
        elif subtype == "相似品浏览再营销":
            await self._delete_audiences_except(page, "similar_products")
            await self._modify_all_bids(page, bid)
        elif subtype == "类目浏览再营销":
            await self._delete_audiences_by_types(page, {"advertised_products", "similar_products"})
            remaining = await self._read_audiences(page)
            category_rows = [a for a in remaining if a.audience_type == "category"]
            if category_rows and title:
                to_keep, to_remove = rank_and_trim_categories(category_rows, title)
                print(f"  📊 类目打分: 保留{len(to_keep)}个，删除{len(to_remove)}个")
                keep_descs = {a.description for a in to_keep}
                for _ in range(30):
                    current = await self._read_audiences(page)
                    target = None
                    for a in current:
                        if a.audience_type == "category" and a.description not in keep_descs:
                            target = a
                            break
                    if target is None:
                        break
                    await self._delete_row_by_index(page, target.row_index)
            await self._modify_all_bids(page, bid)

        print("  ✅ SD再营销 audience 优化完成")
        return True

    async def _navigate_to_audience_tab(self, page) -> bool:
        for attempt in range(3):
            try:
                aud_tab = page.locator("button#AUDIENCE_TARGETS").first
                if await aud_tab.count() == 0:
                    await page.wait_for_timeout(1500)
                    continue
                await aud_tab.click(force=True)
                await page.wait_for_timeout(1000)
                await page.wait_for_selector(".ag-center-cols-container .ag-row", state="visible", timeout=10000)
                print("  ✅ 已切换到 Audiences tab")
                return True
            except PlaywrightTimeoutError:
                await page.wait_for_timeout(1500)
        print("  ❌ Audiences tab 3次重试均失败")
        return False

    async def _read_audiences(self, page) -> List[AudienceRow]:
        container = page.locator(".ag-center-cols-container")
        rows = container.locator("> .ag-row")
        try:
            await rows.first.wait_for(state="visible", timeout=10000)
        except PlaywrightTimeoutError:
            return []
        count = await rows.count()
        audiences = []
        seen_indices: Set[str] = set()
        for i in range(count):
            try:
                r = rows.nth(i)
                ri = await r.get_attribute("row-index")
                if ri in seen_indices:
                    continue
                seen_indices.add(ri)
                desc_cell = r.locator("[col-id='audienceExpression']").first
                if await desc_cell.count() == 0:
                    continue
                desc = (await desc_cell.inner_text()).strip()
                if not desc or len(desc) < 5:
                    continue
                bid_val = None
                bid_el = r.locator("[col-id='bid'] .cell-renderer-content-text").first
                if await bid_el.count() > 0:
                    bid_text = (await bid_el.inner_text()).strip()
                    m = re.search(r"([0-9]+(?:\.[0-9]+)?)", bid_text)
                    bid_val = float(m.group(1)) if m else None
                audiences.append(AudienceRow(desc, classify_audience_type(desc), bid_val, int(ri)))
            except Exception:
                pass
        return audiences

    async def _delete_row_by_index(self, page, row_index) -> bool:
        for attempt in range(3):
            try:
                target_row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{row_index}']").first
                if await target_row.count() == 0:
                    await page.evaluate("""(idx) => {
                        const vp = document.querySelector('.ag-body-viewport');
                        if (vp) vp.scrollTop = idx * 60;
                    }""", row_index)
                    await page.wait_for_timeout(300)
                    if await target_row.count() == 0:
                        return False
                del_btn = target_row.locator("[col-id='delete'] button").first
                if await del_btn.count() == 0:
                    return False
                # 记录删除前的行数
                before_count = await page.locator(".ag-center-cols-container > .ag-row").count()
                await del_btn.click(force=True)
                # 等待行数减少（最多 2 秒）
                for _ in range(10):
                    await page.wait_for_timeout(200)
                    after_count = await page.locator(".ag-center-cols-container > .ag-row").count()
                    if after_count < before_count:
                        return True
                # 超时也返回 True，继续下一次循环
                return True
            except Exception:
                await page.wait_for_timeout(500)
        return False

    async def _delete_audiences_except(self, page, keep_type):
        """按 row_index 从大到小删除：避免 index 变化导致漏删"""
        max_iter = 50
        for iteration in range(max_iter):
            audiences = await self._read_audiences(page)
            to_delete = sorted(
                [a for a in audiences if a.audience_type != keep_type],
                key=lambda a: -a.row_index  # 倒序（大 index 先删）
            )
            if not to_delete:
                return
            target = to_delete[0]
            print(f"  🗑️ 删除受众 row={target.row_index} type={target.audience_type}: {target.description[:60]}")
            success = await self._delete_row_by_index(page, target.row_index)
            if not success:
                print(f"  ⚠️ 删除失败 row={target.row_index}，跳过")
                # 强制退出，避免死循环
                break

    async def _delete_audiences_by_types(self, page, types_to_delete):
        for _ in range(30):
            audiences = await self._read_audiences(page)
            target = None
            for a in audiences:
                if a.audience_type in types_to_delete:
                    target = a
                    break
            if target is None:
                return
            await self._delete_row_by_index(page, target.row_index)

    async def _modify_audience_bid(self, page, row_index, new_bid) -> bool:
        for attempt in range(3):
            delay = 800 + attempt * 500
            try:
                target_row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{row_index}']").first
                if await target_row.count() == 0:
                    await page.evaluate("""(idx) => {
                        const vp = document.querySelector('.ag-body-viewport');
                        if (vp) vp.scrollTop = idx * 60;
                    }""", row_index)
                    await page.wait_for_timeout(800)
                    if await target_row.count() == 0:
                        return True
                bid_trigger = target_row.locator(
                    "[data-e2e-id*='cell-bid:edit'], "
                    "[col-id='bid'] .cell-renderer-trigger, "
                    "[col-id='bid'] .cell-renderer-main-content-interactive"
                ).first
                await bid_trigger.click(force=True, timeout=5000)
                await page.wait_for_timeout(delay)
                bid_input = page.locator("input[type='number'], input[type='text']").last
                await bid_input.wait_for(state="visible", timeout=5000)
                await bid_input.click()
                await page.keyboard.press("Control+A")
                await page.keyboard.press("Backspace")
                await bid_input.type(str(new_bid))
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(800)
                return True
            except Exception:
                try:
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(500)
                except Exception:
                    pass
                await page.wait_for_timeout(1500)
        return False

    async def _modify_all_bids(self, page, new_bid):
        audiences = await self._read_audiences(page)
        for a in audiences:
            await self._modify_audience_bid(page, a.row_index, new_bid)


# ==================== ProductTargetOptimizer ====================
class ProductTargetOptimizer:
    """SD Product 定向优化：替换 target products 为本品/竞品 ASIN，删除 similar to advertised 类目"""

    def __init__(self, df_asin: pd.DataFrame):
        self.df_asin = df_asin

    def _get_asins_for_product_line(self, product_line: str, country: str, asin_type: str) -> List[str]:
        """从 df_asin 获取指定产品线的 Child ASIN 或 Competitor ASIN 列表"""
        target_country = dehydrate(country)
        mask = (
            (self.df_asin["Parent ASIN or Product line"].astype(str).str.strip() == product_line)
            & (self.df_asin["Country"].apply(dehydrate) == target_country)
        )
        matched = self.df_asin[mask]
        if asin_type not in matched.columns:
            print(f"  ⚠️ df_asin 中没有 '{asin_type}' 列")
            return []
        asins = matched[asin_type].dropna().astype(str).str.strip().str.upper().tolist()
        return [a for a in asins if a and a != "NAN" and len(a) == 10]

    async def optimize(self, page, metadata: CampaignMetadata, product_line: str, country: str, bid: float) -> bool:
        """
        完整 SD Product 优化流程：
        1. Target categories → 删 "Similar to advertised products"
        2. Target products → 保留第1个 → remove 其余 → add 新 ASIN → 修改保留 ASIN 的 bid
        """
        # 根据命名判断用本品还是竞品 ASIN
        name_lower = metadata.campaign_name.lower()
        if "本品" in metadata.campaign_name or "childasin" in name_lower:
            asin_type = "Child ASIN"
            print(f"  📦 SD Product 本品ASIN定向")
        elif "竞品" in metadata.campaign_name or "competitorasin" in name_lower:
            asin_type = "Competitor ASIN"
            print(f"  📦 SD Product 竞品ASIN定向")
        else:
            asin_type = "Child ASIN"
            print(f"  📦 SD Product 默认使用本品ASIN")

        target_asins = self._get_asins_for_product_line(product_line, country, asin_type)
        if not target_asins:
            print(f"  ⚠️ 未找到 {asin_type} for {product_line}/{country}")
            return False
        print(f"  📋 待输入 ASIN ({asin_type}): {target_asins}")

        # ===== 步骤1: Target categories → 删 "Similar to advertised products" =====
        print(f"  🔄 切换到 Target categories tab...")
        tc_tab = page.locator("button#TARGET_CATEGORIES").first
        if await tc_tab.count() > 0:
            await tc_tab.click(force=True)
            await page.wait_for_timeout(1500)
            # 找到 "Similar to advertised products" 行的删除按钮
            for attempt in range(5):
                rows = page.locator(".ag-center-cols-container > .ag-row")
                count = await rows.count()
                found_similar = False
                for i in range(count):
                    r = rows.nth(i)
                    type_cell = r.locator("[col-id='type'] .cell-renderer-content-text").first
                    if await type_cell.count() > 0:
                        text = (await type_cell.inner_text()).strip()
                        if "Similar to advertised products" in text:
                            del_btn = r.locator("[col-id='delete'] button").first
                            if await del_btn.count() > 0:
                                await del_btn.click(force=True)
                                await page.wait_for_timeout(1200)
                                found_similar = True
                                print(f"  ✅ 已删除 'Similar to advertised products' segment")
                                break
                if not found_similar:
                    break
        else:
            print(f"  ⚠️ 未找到 Target categories tab")

        # ===== 步骤2: Target products → 清理并替换 =====
        print(f"  🔄 切换到 Target products tab...")
        tp_tab = page.locator("button#TARGET_PRODUCTS").first
        if await tp_tab.count() == 0:
            print(f"  ❌ 未找到 Target products tab")
            return False
        # 多次点击直到 tab 真正激活
        tp_activated = False
        for attempt in range(5):
            await tp_tab.click(force=True)
            await page.wait_for_timeout(800)
            try:
                aria_selected = await tp_tab.get_attribute("aria-selected")
                if aria_selected == "true":
                    tp_activated = True
                    break
            except Exception:
                pass
        if not tp_activated:
            print(f"  ⚠️ Target products tab 未成功激活，继续尝试读取...")
        await page.wait_for_timeout(1500)

        # 等待表格加载：用 ASIN 列的内容作为信号（比 checkbox 更可靠）
        table_loaded = False
        try:
            await page.wait_for_selector(
                "div[role='tabpanel'][aria-labelledby='TARGET_PRODUCTS'] [col-id='asin']",
                state="visible", timeout=10000
            )
            table_loaded = True
        except PlaywrightTimeoutError:
            # 备用：等 Add target products 按钮（只在 TARGET_PRODUCTS tab 下显示）
            try:
                await page.wait_for_selector(
                    "button:has-text('Add target products')",
                    state="visible", timeout=5000
                )
                table_loaded = True
            except PlaywrightTimeoutError:
                pass
        if not table_loaded:
            print(f"  ❌ Target products 表格未加载")
            return False
        print(f"  ✅ Target products 表格已加载")

        # 读取第一个 product 的 ASIN（保留它作为占位）
        first_asin = ""
        try:
            first_row = page.locator(".ag-center-cols-container > .ag-row[row-index='0']").first
            asin_text_el = first_row.locator("p:has-text('ASIN:')").first
            if await asin_text_el.count() > 0:
                first_asin = (await asin_text_el.inner_text()).replace("ASIN:", "").strip().upper()
                print(f"  📌 保留第一个 product ASIN: {first_asin}")
        except Exception:
            pass

        # 全选 → 取消勾选第一个 → Remove
        select_all_cb = page.locator(
            "#tactical-recommendations-table\\:tactical-recommendations-table\\:bulkActions\\:selectAllCheckbox"
        ).first
        if await select_all_cb.count() > 0:
            await select_all_cb.click(force=True)
            await page.wait_for_timeout(800)

            # 取消勾选第一个
            first_cb = page.locator(
                ".ag-center-cols-container > .ag-row[row-index='0'] [col-id='selectRow'] input[type='checkbox']"
            ).first
            if await first_cb.count() > 0:
                await first_cb.click(force=True)
                await page.wait_for_timeout(500)

            # 点 Remove target products
            rm_btn = page.get_by_role("button", name=re.compile(r"Remove target products", re.IGNORECASE)).first
            for _ in range(10):
                if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                    break
                await page.wait_for_timeout(500)
            if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                await rm_btn.click(force=True)
                await page.wait_for_timeout(2000)
                print(f"  ✅ 已 Remove 除第一个外的所有 target products")
            else:
                print(f"  ⚠️ Remove 按钮不可用（可能只有1个product）")

        # ===== 步骤3: Add target products =====
        # 检查保留的 ASIN 是否和要输入的重复，如果重复则从列表中去掉
        has_overlap = first_asin and first_asin in [a.upper() for a in target_asins]
        asins_to_input = [a for a in target_asins if a.upper() != first_asin.upper()] if has_overlap else list(target_asins)

        if has_overlap:
            print(f"  📌 保留的 ASIN {first_asin} 与输入列表重复，已从输入中去除")

        if asins_to_input:
            add_btn = page.get_by_role("button", name=re.compile(r"Add target products", re.IGNORECASE)).first
            
            # 打开 "Add target products" 弹窗：点按钮后确认 textarea 真的出现了
            panel_opened = False
            for add_attempt in range(3):
                # 等按钮可用
                for _ in range(10):
                    if await add_btn.count() > 0 and await add_btn.is_enabled():
                        break
                    await page.wait_for_timeout(500)
                if await add_btn.count() == 0 or not await add_btn.is_enabled():
                    print(f"  ⚠️ Add target products 按钮不可用，重试 {add_attempt+1}/3")
                    await page.wait_for_timeout(1500)
                    continue
                await add_btn.click(force=True)
                # 等 textarea 出现说明弹窗已开
                try:
                    await page.wait_for_selector("#add_target_product_asins", state="visible", timeout=8000)
                    panel_opened = True
                    break
                except PlaywrightTimeoutError:
                    print(f"  ⚠️ Add 弹窗未打开，重试 {add_attempt+1}/3")
                    await page.wait_for_timeout(1500)
            
            if not panel_opened:
                print(f"  ❌ Add target products 弹窗 3 次都没打开")
                return False
            
            await page.wait_for_timeout(500)

            # 填 bid
            bid_input = page.locator("#add_target_product_bid")
            await bid_input.click()
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            await bid_input.type(str(bid), delay=20)
            await page.wait_for_timeout(300)

            # 填 ASIN
            asin_textarea = page.locator("#add_target_product_asins")
            await asin_textarea.fill("\n".join(asins_to_input))
            await page.wait_for_timeout(800)

            # Save：等按钮 enabled + 点击 + 等弹窗关闭
            save_btn = page.get_by_role("button", name=re.compile(r"^Save$", re.IGNORECASE)).last
            # 延长等待时间到 10 秒
            save_enabled = False
            for _ in range(20):
                if await save_btn.count() > 0 and await save_btn.is_enabled():
                    save_enabled = True
                    break
                await page.wait_for_timeout(500)
            if not save_enabled:
                print(f"  ⚠️ Save 按钮等待超时，强制点击")
            
            # Save 可能需要多次尝试（网络慢/校验延迟/部分 ASIN 无效）
            save_ok = False
            current_asins = list(asins_to_input)
            for save_attempt in range(4):
                try:
                    await save_btn.click(force=True)
                except Exception:
                    pass
                await page.wait_for_timeout(2000)
                
                # 检测 "ASIN not found" 错误提示，提取问题 ASIN
                invalid_asins = set()
                try:
                    error_asins = await page.evaluate("""() => {
                        const results = [];
                        // 查找所有错误提示框中的 h4（ASIN not found / Invalid ASIN 等）
                        const errorBoxes = document.querySelectorAll('.sc-storm-ui-30103358__sc-j3wigw-0');
                        for (const box of errorBoxes) {
                            const h4 = box.querySelector('h4');
                            if (h4) {
                                const text = h4.textContent || '';
                                if (text.includes('ASIN') || text.includes('not found') || text.includes('Invalid')) {
                                    const lis = box.querySelectorAll('li p');
                                    for (const p of lis) {
                                        results.push(p.textContent.trim());
                                    }
                                }
                            }
                        }
                        return results;
                    }""")
                    for raw in error_asins:
                        # 提取 10 位 ASIN（B0XXXXXXXX 格式）
                        import_re = True
                        asin_match = raw.strip().upper()
                        if len(asin_match) == 10 and asin_match.startswith("B"):
                            invalid_asins.add(asin_match)
                        else:
                            # 兜底：用正则提取
                            import re as _re
                            for m in _re.findall(r"B[0-9A-Z]{9}", raw.upper()):
                                invalid_asins.add(m)
                except Exception:
                    pass
                
                if invalid_asins:
                    print(f"  ⚠️ 检测到 {len(invalid_asins)} 个无效 ASIN，移除后重试: {list(invalid_asins)[:3]}...")
                    current_asins = [a for a in current_asins if a.upper() not in invalid_asins]
                    if not current_asins:
                        print(f"  ❌ 所有 ASIN 都无效，放弃添加")
                        # 尝试关闭弹窗
                        cancel_btn = page.get_by_role("button", name=re.compile(r"^Cancel$", re.IGNORECASE)).last
                        if await cancel_btn.count() > 0:
                            await cancel_btn.click(force=True)
                            await page.wait_for_timeout(1000)
                        save_ok = False
                        break
                    # 重新填入 textarea
                    await asin_textarea.click()
                    await page.keyboard.press("Control+A")
                    await page.keyboard.press("Backspace")
                    await asin_textarea.fill("\n".join(current_asins))
                    await page.wait_for_timeout(800)
                    continue
                
                # 没有错误 → 检查弹窗是否关闭
                try:
                    await page.wait_for_selector("#add_target_product_asins", state="hidden", timeout=5000)
                    save_ok = True
                    break
                except PlaywrightTimeoutError:
                    print(f"  ⚠️ Save 后弹窗未关闭（无错误提示），重试 {save_attempt+1}/4")
                    await page.wait_for_timeout(1500)
            
            await page.wait_for_timeout(1500)
            if save_ok:
                print(f"  ✅ 已添加 {len(current_asins)} 个 target products")
            else:
                print(f"  ⚠️ Save 疑似未成功，继续往下走")
        else:
            print(f"  📌 所有 ASIN 都和保留的重复，无需添加")

        # ===== 步骤4: 处理保留的第一个 ASIN =====
        if not has_overlap and first_asin:
            # 没有重复 → 需要单独删掉保留的那个占位 ASIN
            print(f"  🗑️ 删除占位 ASIN {first_asin}...")
            await page.wait_for_timeout(1000)
            # 找到这个 ASIN 的行并勾选
            rows = page.locator(".ag-center-cols-container > .ag-row")
            count = await rows.count()
            for i in range(count):
                r = rows.nth(i)
                asin_p = r.locator("p:has-text('ASIN:')").first
                if await asin_p.count() > 0:
                    text = (await asin_p.inner_text()).replace("ASIN:", "").strip().upper()
                    if text == first_asin:
                        cb = r.locator("[col-id='selectRow'] input[type='checkbox']").first
                        if await cb.count() > 0:
                            await cb.click(force=True)
                            await page.wait_for_timeout(500)
                        break
            rm_btn = page.get_by_role("button", name=re.compile(r"Remove target products", re.IGNORECASE)).first
            for _ in range(10):
                if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                    break
                await page.wait_for_timeout(500)
            if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                await rm_btn.click(force=True)
                await page.wait_for_timeout(1500)
                print(f"  ✅ 已删除占位 ASIN {first_asin}")
        elif has_overlap:
            # 有重复 → 保留的 ASIN 已在列表中，只需修改它的 bid
            print(f"  💰 修改保留 ASIN {first_asin} 的 bid 为 {bid}...")
            # 等表格充分渲染（刚加了 N 个新行）
            await page.wait_for_timeout(2000)
            
            modified = False
            for retry in range(3):
                try:
                    # 每次重新定位行（AG Grid 可能重排）
                    rows = page.locator(".ag-center-cols-container > .ag-row")
                    count = await rows.count()
                    target_row = None
                    for i in range(count):
                        r = rows.nth(i)
                        asin_p = r.locator("p:has-text('ASIN:')").first
                        if await asin_p.count() > 0:
                            text = (await asin_p.inner_text()).replace("ASIN:", "").strip().upper()
                            if text == first_asin:
                                target_row = r
                                break
                    if not target_row:
                        # 滚动找行
                        await page.evaluate("""() => {
                            const vp = document.querySelector('.ag-body-viewport');
                            if (vp) vp.scrollTop = 0;
                        }""")
                        await page.wait_for_timeout(500)
                        continue
                    
                    await target_row.scroll_into_view_if_needed()
                    await page.wait_for_timeout(500)
                    
                    # 点击 bid 编辑触发器
                    bid_trigger = target_row.locator(
                        "[data-e2e-id*='cell-bid:edit'], "
                        "[col-id='bid'] .cell-renderer-trigger"
                    ).first
                    if await bid_trigger.count() == 0:
                        # 备用：直接点 bid 单元格
                        bid_cell = target_row.locator("[col-id='bid']").first
                        await bid_cell.click(force=True)
                    else:
                        await bid_trigger.click(force=True)
                    
                    await page.wait_for_timeout(1000)
                    
                    # 等 input 出现，延长到 10 秒
                    bid_inp = page.locator("input[type='number']").last
                    try:
                        await bid_inp.wait_for(state="visible", timeout=10000)
                    except PlaywrightTimeoutError:
                        # 再次点击 trigger 重试
                        await page.wait_for_timeout(500)
                        if await bid_trigger.count() > 0:
                            await bid_trigger.click(force=True)
                        await bid_inp.wait_for(state="visible", timeout=5000)
                    
                    await bid_inp.click()
                    await page.keyboard.press("Control+A")
                    await page.keyboard.press("Backspace")
                    await bid_inp.type(str(bid))
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(1000)
                    print(f"  ✅ 已修改 {first_asin} bid 为 {bid}")
                    modified = True
                    break
                except Exception as e:
                    print(f"  ⚠️ 修改 bid 第 {retry+1} 次失败: {e}")
                    try:
                        await page.keyboard.press("Escape")
                    except Exception:
                        pass
                    await page.wait_for_timeout(1000)
            if not modified:
                print(f"  ❌ 无法修改 {first_asin} 的 bid，跳过")

        print(f"  ✅ SD Product 定向优化完成")
        return True


# ==================== BudgetAndNameModifier ====================
class BudgetAndNameModifier:
    """修改 campaign 的预算和名称"""

    async def modify_budget(self, page, row, ad_type: str) -> bool:
        """根据 BUDGET_RULES 修改预算，最多重试 3 次"""
        budget = BUDGET_RULES.get(ad_type, 10)
        budget_trigger = row.locator(
            "[data-e2e-id='tactical-recommendations-table:cell-campaignBudget:edit'], "
            "[data-takt-id='tactical-recommendations-table:cell-campaignBudget:edit']"
        ).first
        for attempt in range(3):
            try:
                await budget_trigger.wait_for(state="visible", timeout=5000)
                await page.wait_for_timeout(500 + attempt * 500)
                await budget_trigger.click(force=True)
                await page.wait_for_timeout(800)
                budget_input = page.locator("input[type='number']").last
                await budget_input.wait_for(state="visible", timeout=3000)
                await budget_input.click()
                await page.keyboard.press("Control+A")
                await page.keyboard.press("Backspace")
                await budget_input.type(str(budget))
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(500)
                print(f"    ✅ 预算已修改为 {budget}")
                return True
            except PlaywrightTimeoutError:
                print(f"    ⏳ 预算修改第{attempt+1}次超时，重试...")
                await page.wait_for_timeout(1500)
        print("    ⚠️ 预算修改3次均超时")
        return False

    async def modify_campaign_name(self, page, row, new_name: str) -> bool:
        """点击编辑触发器 → 清空 → 输入新名称 → Enter 保存，最多重试 3 次"""
        name_trigger = row.locator("[col-id='campaignName'] .cell-renderer-content-text").first
        for attempt in range(3):
            try:
                await name_trigger.wait_for(state="visible", timeout=8000)
                # 先等页面稳定（预算修改后表格可能在重新渲染）
                await page.wait_for_timeout(500 + attempt * 500)
                
                # 双击触发编辑模式
                await name_trigger.dblclick(force=True)
                await page.wait_for_timeout(800)

                # 查找输入框（多种选择器兼容）
                input_selectors = [
                    "[data-e2e-id='tactical-recommendations-table:cell-campaignName:input']",
                    "[col-id='campaignName'] input",
                    ".ag-cell-edit-wrapper input",
                ]
                input_box = None
                for sel in input_selectors:
                    el = page.locator(sel).first
                    if await el.count() > 0 and await el.is_visible():
                        input_box = el
                        break
                
                if not input_box:
                    # 如果双击没触发编辑，尝试单击
                    await name_trigger.click(force=True)
                    await page.wait_for_timeout(800)
                    for sel in input_selectors:
                        el = page.locator(sel).first
                        if await el.count() > 0 and await el.is_visible():
                            input_box = el
                            break

                if not input_box:
                    raise PlaywrightTimeoutError("未找到 campaign name 输入框")

                await input_box.click()
                await page.wait_for_timeout(300)
                await page.keyboard.press("Control+A")
                await page.wait_for_timeout(200)
                await page.keyboard.press("Backspace")
                await page.wait_for_timeout(200)
                await input_box.type(new_name, delay=20)
                await page.wait_for_timeout(500)
                await page.keyboard.press("Enter")
                await page.wait_for_timeout(1000)
                print(f"    ✅ Campaign name 已修改为: {new_name}")
                return True
            except Exception as e:
                print(f"    ⏳ Campaign name 修改第{attempt+1}次失败: {str(e)[:80]}，重试...")
                try:
                    await page.keyboard.press("Escape")
                except Exception:
                    pass
                await page.wait_for_timeout(2000)
        print("    ⚠️ Campaign name 修改3次均失败")
        return False


# ==================== 弹窗关闭 ====================
async def close_popup(page):
    """点 X 按钮关闭弹窗，回到列表页面"""
    try:
        # 优先用精确的 X 按钮 SVG path 定位（最可靠）
        x_path = page.locator("svg path[d*='M20.44 367.51']")
        x_count = await x_path.count()
        for i in range(x_count):
            btn = x_path.nth(i)
            # 排除 ag-row 内部的按钮（那些是行内操作按钮）
            in_grid = btn.locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
            if await in_grid.count() > 0:
                continue
            if await btn.is_visible():
                # 点击 SVG 的父级可点击元素
                parent_btn = btn.locator("xpath=ancestor::*[@role='button' or self::button or self::a][1]").first
                if await parent_btn.count() > 0:
                    await parent_btn.click(force=True)
                else:
                    await btn.click(force=True)
                await page.wait_for_timeout(1000)
                return
        
        # 备用：Escape 键
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(800)
    except Exception:
        try:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(800)
        except Exception:
            pass


# ==================== 主流程 ====================
async def main():
    print("=" * 50)
    print(" 🚀 Campaign Namer-Optimizer V1.0")
    print("=" * 50)

    # 1. 数据加载
    print("\n[1/5] 加载配置数据...")
    processor = DataProcessor(MEDIA_PLAN_PATH, ASIN_INFO_PATH, FORMAT_PATH)
    df_mp, df_asin, df_format = processor.process()
    print(f"  ✅ Media Plan: {len(df_mp)} 条, ASIN Info: {len(df_asin)} 条, Format: {len(df_format)} 条")

    # 2. 连接浏览器（先连接，再从 URL 识别国家）
    print("\n[2/5] 连接 Chrome (localhost:9222)...")
    pw = await async_playwright().start()
    browser, page = await connect_browser_and_find_page(pw)

    # 从当前页面 URL 识别国家
    url_to_country = {
        "amazon.co.uk": "UK",
        "amazon.de": "DE",
        "amazon.fr": "FR",
        "amazon.it": "IT",
        "amazon.es": "ES",
        "amazon.nl": "NL",
        "amazon.se": "SE",
        "amazon.pl": "PL",
        "amazon.com.tr": "TR",
        "amazon.com.be": "BE",
        "amazon.com": "US",
        "amazon.ca": "CA",
        "amazon.com.mx": "MX",
        "amazon.com.au": "AU",
        "amazon.co.jp": "JP",
        "amazon.ae": "AE",
        "amazon.sa": "SA",
        "amazon.in": "IN",
    }
    current_url = page.url or ""
    country = ""
    for domain, cc in url_to_country.items():
        if domain in current_url:
            country = cc
            break
    if not country:
        # 兜底：从 df_asin 取第一个
        if "Country" in df_asin.columns:
            countries = df_asin["Country"].dropna().unique()
            if len(countries) > 0:
                country = str(countries[0])
    print(f"  📍 从 URL 识别国家: {country} (url={current_url[:80]})")

    # 按识别到的国家过滤 df_asin（Media Plan 按 brand+product_line 查找，不按国家过滤）
    if country:
        target_cc = dehydrate(country)
        before_asin = len(df_asin)
        df_asin = df_asin[df_asin["Country"].apply(dehydrate) == target_cc].reset_index(drop=True)
        print(f"  🔍 df_asin 按国家 {country} 过滤: {before_asin} → {len(df_asin)} 条")

    # 3. 全局扫描
    print("\n[3/5] 全局扫描所有 campaign name...")
    scanner = Scanner()
    scanned_names = await scanner.scan_all_campaign_names(page)

    # 4. Media Plan 对比排除
    print("\n[4/5] Media Plan 对比排除...")
    comparator = MediaPlanComparator(df_mp)
    exclusion_set, pending_names = comparator.compare(scanned_names)

    # 5. 逐行处理
    print("\n[5/5] 开始逐行处理...")
    parser = CampaignNameParser()
    kw_resolver = KeywordAnalysisResolver(KEYWORD_ANALYSIS_DIR, df_asin)
    ranking_cache: Dict[str, list] = {}
    asin_resolver = ASINResolver(df_asin, df_format, df_mp, exclusion_set)
    kw_optimizer = KeywordOptimizer(kw_resolver, ranking_cache)
    aud_optimizer = AudienceOptimizer()
    product_optimizer = ProductTargetOptimizer(df_asin)
    modifier = BudgetAndNameModifier()

    # 读取总行数（多种方式尝试）
    total_campaigns = 0
    try:
        # 方法1：从分页状态读取
        status_el = page.locator("#tactical-recommendations-table\\:pagination-page-status").first
        if await status_el.count() > 0:
            status_text = (await status_el.inner_text()).strip()
            m = re.search(r"of\s+(\d+)\s+results", status_text)
            if m:
                total_campaigns = int(m.group(1))
    except Exception:
        pass
    if total_campaigns == 0:
        try:
            # 方法2：从 "Selected X recommendations of Y recommendations" 读取
            sel_text = await page.evaluate("""() => {
                const els = document.querySelectorAll('div');
                for (const el of els) {
                    const t = el.textContent || '';
                    const m = t.match(/(\\d+)\\s+recommendations\\s+of\\s+(\\d+)\\s+recommendations/);
                    if (m) return parseInt(m[2]);
                }
                return 0;
            }""")
            if sel_text > 0:
                total_campaigns = sel_text
        except Exception:
            pass
    if total_campaigns == 0:
        rows = page.locator(".ag-center-cols-container > .ag-row")
        total_campaigns = await rows.count()
    print(f"  总共 {total_campaigns} 个 campaign")

    # 等待表格加载
    try:
        await page.wait_for_selector(".ag-center-cols-container > .ag-row", timeout=15000)
    except PlaywrightTimeoutError:
        print("  ❌ 页面上没找到表格行")
        return

    processed_count = 0
    success_count = 0
    error_count = 0
    last_memory_release = 0
    processed_rows: Set[str] = set()  # 已处理的 campaign 标识（用 ASIN 集合的 frozenset 字符串）

    while True:
        # 每 15 行释放内存
        if processed_count > 0 and processed_count % 15 == 0 and processed_count != last_memory_release:
            last_memory_release = processed_count
            print(f"\n  🧹 已处理 {processed_count} 个，释放浏览器内存...")
            await release_browser_memory(page)
            await page.wait_for_timeout(500)

        # ===== 核心改进：每次从头扫描找到下一个未处理的行 =====
        found_target = False
        target_idx = -1
        target_original_name = ""
        
        for scan_idx in range(total_campaigns):
            row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{scan_idx}']").first
            if await row.count() == 0 or not await row.is_visible():
                # 向下滚动找到这一行（用 AG_Grid viewport 滚动，比 mouse.wheel 更可靠）
                for scroll_try in range(15):
                    try:
                        await page.evaluate(f"""(idx) => {{
                            const vp = document.querySelector('.ag-body-viewport');
                            if (vp) vp.scrollTop = idx * 50;
                        }}""", scan_idx)
                    except Exception:
                        await page.mouse.wheel(0, 600)
                    await page.wait_for_timeout(400)
                    if await row.count() > 0 and await row.is_visible():
                        break
                if await row.count() == 0 or not await row.is_visible():
                    continue

            try:
                name = (await row.locator("[col-id='campaignName'] .cell-renderer-content-text").inner_text(timeout=2000)).strip()
            except Exception:
                continue

            # 检查是否已命名完成（跳过已优化的）
            if "Campaign Recommendation" not in name and "_" in name:
                continue

            # 检查是否已在本次运行中处理过（用原始名字 + row-index 组合，因为同名 campaign 可能有多个）
            # 但 row-index 会变，所以用一个递增计数器
            row_key = f"{name}::{scan_idx}"
            if row_key in processed_rows:
                continue
            # 也检查这个 scan_idx 是否已经被处理过（行可能重排但内容不变）
            if any(k.endswith(f"::{scan_idx}") for k in processed_rows):
                # 这个 row-index 已经处理过，但名字可能变了（因为我们改了名字）
                # 重新检查：如果名字已经被命名过，跳过
                if "_" in name and "Campaign Recommendation" not in name:
                    continue

            # 找到了一个未处理的行
            found_target = True
            target_idx = scan_idx
            target_original_name = name
            break

        if not found_target:
            print(f"\n  ✅ 所有行已处理完毕或已跳过。共处理 {processed_count} 个")
            break

        # 定位到目标行
        idx = target_idx
        original_name = target_original_name
        row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{idx}']").first
        await row.scroll_into_view_if_needed()
        await page.wait_for_timeout(300)

        # ==================== 核心流程：先检测类型，再进去识别，再优化 ====================
        print(f"\n[row {idx}] ▶ {original_name}")
        try:
            # 步骤1: 点蓝色按钮 → 提取 ASIN + title → 判定 campaign name
            temp_metadata = parser.parse(original_name)
            final_name, title, asins = await asin_resolver.resolve(page, row, original_name, temp_metadata, country)
            
            if not asins:
                print(f"  ⚠️ 没读取到 ASIN，跳过")
                await close_popup(page)
                idx += 1
                continue
            
            print(f"  📦 ASIN: {asins}")
            print(f"  📝 title: {(title or 'N/A')[:120]}")

            if not final_name:
                print(f"  ⚠️ 无法判定 campaign name，跳过")
                await close_popup(page)
                idx += 1
                continue
            
            print(f"  🏷️ 判定 campaign name: {final_name}")
            
            # 步骤2: 用判定出的 campaign name 解析真正的 metadata
            metadata = parser.parse(final_name)
            print(f"  解析: ad={metadata.ad_type} | opt={metadata.optimization_type} | label={metadata.keyword_label} | match={metadata.match_type}")
            
            # 步骤3: 按类型路由优化（弹窗还开着）
            # 等弹窗内容充分渲染后再检测 tab
            await page.wait_for_timeout(500)
            
            # 先尝试等 Keyword tab 出现（最多 1.5 秒）
            has_keyword_tab = False
            try:
                kw_btn = page.locator("button#KEYWORDS").first
                kw_tab = page.get_by_role("tab", name=re.compile(r"^Keyword$", re.IGNORECASE)).first
                for _ in range(3):
                    if (await kw_btn.count() > 0 and await kw_btn.is_visible()) or \
                       (await kw_tab.count() > 0 and await kw_tab.is_visible()):
                        has_keyword_tab = True
                        break
                    await page.wait_for_timeout(500)
            except Exception:
                pass
            
            has_settings_tab = False
            try:
                settings_el = page.locator("p:has-text('Settings')").first
                has_settings_tab = await settings_el.count() > 0 and await settings_el.is_visible()
            except Exception:
                pass
            
            is_sp_auto = (not has_keyword_tab) and has_settings_tab and metadata.ad_type in ("SP", "Unknown")
            print(f"  🔍 弹窗检测: Keyword={has_keyword_tab}, Settings={has_settings_tab}, Auto={is_sp_auto}, ad_type={metadata.ad_type}")
            
            # 检测 TARGET_PRODUCTS tab（SD Product 类型标志）
            has_target_products_tab = False
            try:
                tp_btn = page.locator("button#TARGET_PRODUCTS").first
                has_target_products_tab = await tp_btn.count() > 0 and await tp_btn.is_visible()
            except Exception:
                pass
            # 检测 AUDIENCE_TARGETS tab（SD Remarketing 类型标志）
            has_audience_tab = False
            try:
                aud_btn = page.locator("button#AUDIENCE_TARGETS").first
                has_audience_tab = await aud_btn.count() > 0 and await aud_btn.is_visible()
            except Exception:
                pass
            is_sd_product = has_target_products_tab and metadata.ad_type == "SD"
            is_sd_remarketing = has_audience_tab and metadata.ad_type == "SD" and not has_target_products_tab
            if is_sd_product:
                print(f"  🔍 弹窗检测: TARGET_PRODUCTS tab 存在 → SD Product 类型")
            elif is_sd_remarketing:
                print(f"  🔍 弹窗检测: AUDIENCE_TARGETS tab 存在 → SD Remarketing 类型")
            
            if is_sd_product:
                # 如果初始命名分配的是 Remarketing 类型（错误），重新用 Product 过滤
                if "再营销" in final_name:
                    print(f"  📌 SD Product: 初始命名为再营销类型（错误），重新从 Product 候选分配...")
                    if final_name in asin_resolver.exclusion_set:
                        asin_resolver.exclusion_set.discard(final_name)
                    prod_line = None
                    for asin in asins:
                        pl = find_product_line_by_asin(asin, country, df_asin)
                        if pl:
                            prod_line = pl
                            break
                    if prod_line:
                        new_name = asin_resolver._determine_campaign_name(
                            prod_line, metadata, metadata.ad_type, None, campaign_type_filter="Product"
                        )
                        if new_name:
                            final_name = new_name
                            metadata = parser.parse(final_name)
                            print(f"  🏷️ SD Product 命名: {final_name}")
                # SD Product 定向（本品ASIN / 竞品ASIN）
                bid = calculate_bid(metadata)
                print(f"  💰 SD Product 出价: {bid}")
                sd_product_line = None
                for asin in asins:
                    pl = find_product_line_by_asin(asin, country, df_asin)
                    if pl:
                        sd_product_line = pl
                        break
                if sd_product_line:
                    ok = await product_optimizer.optimize(page, metadata, sd_product_line, country, bid)
                else:
                    print(f"  ⚠️ 未找到产品线，跳过 SD Product 优化")
                    ok = True
                await close_popup(page)
            elif metadata.optimization_type == "Remarketing" or is_sd_remarketing:
                # 如果是通过弹窗检测到的 SD Remarketing（而非命名解析），需要先确定命名
                if is_sd_remarketing and not metadata.remarketing_subtype:
                    print(f"  📌 SD Remarketing: 命名中无子类型，从再营销命名顺位分配...")
                    # 查找 product_line
                    rm_product_line = None
                    for asin in asins:
                        pl = find_product_line_by_asin(asin, country, df_asin)
                        if pl:
                            rm_product_line = pl
                            break
                    if rm_product_line:
                        # 先从 exclusion_set 中移除之前错误分配的 Product 类型名
                        if final_name in asin_resolver.exclusion_set:
                            asin_resolver.exclusion_set.discard(final_name)
                        # 只从 Remarketing 类型候选中分配
                        new_name = asin_resolver._determine_campaign_name(
                            rm_product_line, metadata, metadata.ad_type, None, campaign_type_filter="Remarketing"
                        )
                        if new_name:
                            final_name = new_name
                            metadata = parser.parse(final_name)
                            print(f"  🏷️ SD Remarketing 命名: {final_name}")
                            print(f"  🎯 子类型: {metadata.remarketing_subtype}")
                        else:
                            print(f"  ⚠️ 无可用再营销命名")
                bid = calculate_bid(metadata)
                print(f"  💰 计算出价: {bid}")
                # 弹窗已开着，直接优化，不要关闭重开
                ok = await aud_optimizer.optimize(page, row, metadata, bid)
                await close_popup(page)
            elif is_sp_auto:
                print(f"  🔍 弹窗内检测: 无 Keyword tab + 有 Settings → SP Auto")
                product_line_for_auto = None
                for asin in asins:
                    pl = find_product_line_by_asin(asin, country, df_asin)
                    if pl:
                        product_line_for_auto = pl
                        break
                if product_line_for_auto:
                    final_name = f"{product_line_for_auto}_SP_Automatic_自动_CPC_AST"
                    print(f"  🏷️ SP Auto 命名: {final_name}")
                    metadata = parser.parse(final_name)
                bid = calculate_bid(metadata)
                print(f"  💰 计算出价: {bid}")
                try:
                    settings_btn = page.locator("p:has-text('Settings')").first
                    await settings_btn.click(force=True)
                    await page.wait_for_timeout(1500)
                    bid_input = page.locator("input[type='number']").first
                    if await bid_input.count() > 0 and await bid_input.is_visible():
                        await bid_input.click()
                        await page.keyboard.press("Control+A")
                        await page.keyboard.press("Backspace")
                        await bid_input.type(str(bid))
                        await page.wait_for_timeout(300)
                    save_btn = page.get_by_role("button", name=re.compile(r"Save", re.IGNORECASE)).last
                    if await save_btn.count() > 0:
                        await save_btn.click(force=True)
                        await page.wait_for_timeout(2000)
                    ok = True
                    print(f"  ✅ SP Auto bid 修改完成")
                except Exception as e:
                    print(f"  ⚠️ SP Auto Settings 操作失败: {e}")
                    ok = False
                await close_popup(page)
            elif metadata.is_keyword_campaign:
                print(f"  📌 关键词优化: label={metadata.keyword_label}, match={metadata.match_type}, name={final_name}")
                ok = await kw_optimizer.optimize(page, row, metadata, title)
                await close_popup(page)
            else:
                await close_popup(page)
                ok = True
            
            # 步骤4: 修改预算 + campaign name
            if ok:
                await modifier.modify_budget(page, row, metadata.ad_type)
                # 预算修改不会让行重排，直接用原 row 改名（最可靠）
                await page.wait_for_timeout(500)
                name_ok = await modifier.modify_campaign_name(page, row, final_name)
                if not name_ok:
                    # 原 row 失效才重新定位：优先用 row-id（最稳定的唯一标识）
                    print(f"  🔄 原 row 失效，尝试用 row-id 重新定位...")
                    original_row_id = None
                    try:
                        original_row_id = await row.get_attribute("row-id")
                    except Exception:
                        pass
                    relocated_row = None
                    if original_row_id:
                        try:
                            cand = page.locator(
                                f".ag-center-cols-container > .ag-row[row-id='{original_row_id}']"
                            ).first
                            if await cand.count() > 0:
                                relocated_row = cand
                        except Exception:
                            pass
                    if relocated_row:
                        await modifier.modify_campaign_name(page, relocated_row, final_name)
                    else:
                        print(f"  ⚠️ 无法定位到原行，命名可能失败")
                success_count += 1
                print(f"[row {idx}] ✅ 完成（name={final_name}）")
            else:
                print(f"[row {idx}] ⚠️ 优化未成功，跳过命名和预算修改")

        except Exception as e:
            print(f"[row {idx}] ❌ 处理失败: {type(e).__name__}: {e}")
            error_count += 1
            await close_popup(page)

        # 标记当前行为已处理
        processed_rows.add(f"{original_name}::{idx}")
        processed_count += 1

        # 等待表格重新渲染（行可能重排）
        await page.wait_for_timeout(1000)
        try:
            await page.wait_for_selector(".ag-center-cols-container > .ag-row", state="visible", timeout=15000)
        except Exception:
            await page.wait_for_timeout(3000)
        
        # 滚动回顶部，下一轮从头扫描
        try:
            await page.evaluate("""() => {
                const vp = document.querySelector('.ag-body-viewport');
                if (vp) vp.scrollTop = 0;
            }""")
        except Exception:
            pass
        await page.wait_for_timeout(500)

    # 输出统计
    print(f"\n{'=' * 50}")
    print(f" ✅ 全部完成！")
    print(f"    处理总数: {processed_count}")
    print(f"    成功: {success_count}")
    print(f"    错误: {error_count}")
    print(f"    跳过: {processed_count - success_count - error_count}")
    print(f"{'=' * 50}")
    # 不调用 pw.stop()，保持浏览器和页面打开


# ==================== 入口 ====================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️ 手动中断，页面保持打开")
    except Exception as e:
        print(f"\n❌ 未捕获异常: {e}")
        print("页面保持打开，可手动检查")
