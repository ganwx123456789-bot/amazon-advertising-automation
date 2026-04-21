"""
完整优化链路测试副本：
- 直接连接已打开的Chrome端口，从当前命名界面开始
- 流程：读命名 → 点蓝色1 → Landing page读ASIN → 抓title → 回Keyword tab
       → 读现有词 → 评分排名 → 删除除第一行外所有词 → 第一行改名"1"
       → 添加新词 → 删掉"1" → 关闭弹窗 → 下一个
"""
import asyncio
import os
import re
import statistics
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MEDIA_PLAN_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "Media Plan For Campaign Builder.xlsx"))
ASIN_INFO_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "Excel Template", "ASIN_Input_Template For Campaign Builder.xlsx"))
FORMAT_PATH = os.path.join(BASE_DIR, "campaign format.xlsx")
KEYWORD_ANALYSIS_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "Reporting-keyword"))
DEFAULT_BID = 0.35
TOP_KEYWORD_LIMIT = 10
DEFAULT_DRY_RUN = False
TARGET_AD_TYPES = {"SB", "SBV", "SP", "SD"}

SOURCE_HEADINGS = {
    "高转化本品词": "Own - High conversion rate",
    "高转化竞品词": "Competitor - High conversion rate",
    "高加购本品词": "Own - High add-to-cart rate",
    "高加购竞品词": "Competitor - High add-to-cart rate",
    "高潜力本品词": "Own - High potential",
    "高潜力竞品词": "Competitor - High potential",
}
BUDGET_RULES = {"SD": 16, "SB": 10, "SP": 10, "SBV": 10}
NON_KEYWORD_LABELS = {
    "类目定向": "Product", "商品定向": "Product", "产品定向": "Product",
    "受众": "Audience", "受众定向": "Audience",
    "自动": "Auto", "自动投放": "Auto",
}
STOPWORDS = {"a","an","and","at","by","for","from","in","of","on","or","the","to","with"}

# ==================== 出价配置（手动填写区）====================
BID_CONFIG = {
    "base_cpc": 0.38,                       # 平均CPC
    "ad_type_coefficient": {                 # 各广告类型基准价格 = base_cpc × 系数
        "SD": 1.0,
        "SB": 0.8,
        "SP": 0.7,
    },
    "target_coefficient": {                  # 各target方式出价 = 广告类型基准 × 系数
        "SD": {"Remarketing_Audience": 1.2, "Product": 0.8},
        "SB": {"KW": 1.0, "Product": 0.8},
        "SP": {"KW": 1.0, "Automatic": 0.6},
    },
    "match_coefficient": {                   # SP匹配方式系数
        "Broad": 0.8, "Phrase": 1.0, "Exact": 1.1,
    },
    "conversion_coefficient": {              # SP转化类型系数
        "高转化": 1.2, "高加购": 1.0, "高潜力": 0.8,
    },
}

REMARKETING_SUBTYPES = {
    "本品浏览再营销": "本品浏览再营销",
    "相似品浏览再营销": "相似品浏览再营销",
    "类目浏览再营销": "类目浏览再营销",
}

# ==================== 工具函数 ====================
def clean_text(text, keep_space=False):
    if pd.isna(text): return ""
    value = str(text).strip()
    if keep_space: return re.sub(r"\s+", " ", value)
    return re.sub(r"[^\u4e00-\u9fa5a-zA-Z0-9]", "", value)

def normalize_text(text):
    if pd.isna(text): return ""
    return re.sub(r"\s+", " ", str(text).strip()).lower()

def tokenize(text):
    value = normalize_text(text)
    if not value: return []
    parts = re.split(r"[^\u4e00-\u9fa5a-zA-Z0-9]+", value)
    return [p for p in parts if p and p not in STOPWORDS]

def median_or_default(values, default):
    cleaned = [v for v in values if v is not None]
    return float(statistics.median(cleaned)) if cleaned else default


def _detect_conversion_label(keyword_label):
    if not keyword_label: return None
    if "高转化" in keyword_label: return "高转化"
    if "高加购" in keyword_label: return "高加购"
    if "高潜力" in keyword_label: return "高潜力"
    return None

def _map_optimization_to_target_key(metadata):
    mapping = {"Keyword": "KW", "Auto": "Automatic", "Product": "Product", "Remarketing": "Remarketing_Audience"}
    return mapping.get(metadata.optimization_type, "KW")

def calculate_bid(metadata):
    """根据BID_CONFIG和CampaignMetadata计算最终出价"""
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
    return result if result > 0 else DEFAULT_BID


# ==================== 数据类 ====================
@dataclass
class CampaignMetadata:
    campaign_name: str; product_line: str; ad_type: str
    optimization_type: str; keyword_label: Optional[str]
    match_type: Optional[str]; cost_type: Optional[str]
    remarketing_subtype: Optional[str] = None
    @property
    def is_keyword_campaign(self): return self.optimization_type == "Keyword"

@dataclass
class RawMetric:
    keyword: str; purchase_share: Optional[float]; atc_share: Optional[float]
    click_purchase_funnel: Optional[float]; atc_purchase_funnel: Optional[float]
    search_share: Optional[float] = None

@dataclass
class KeywordCandidate:
    keyword: str; normalized_keyword: str; source: str; labels: Set[str]; source_order: int
    purchase_share: Optional[float] = None; atc_share: Optional[float] = None
    click_purchase_funnel: Optional[float] = None; atc_purchase_funnel: Optional[float] = None
    search_share: Optional[float] = None
    is_system_keyword: bool = False; excel_tier: Optional[int] = None
    final_score: float = 0.0; exclusion_reason: Optional[str] = None

@dataclass
class CampaignPlan:
    metadata: CampaignMetadata; workbook_path: Path; sheet_name: str
    source_heading: str; title: Optional[str]
    current_keywords: List[Tuple[str, str, Optional[float]]]
    ranked_keywords: List[KeywordCandidate] = field(default_factory=list)

@dataclass
class AudienceRow:
    description: str
    audience_type: str  # "advertised_products" | "similar_products" | "category" | "unknown"
    bid: Optional[float]
    row_index: int

def classify_audience_type(description):
    # 描述是多行文本，需要全文匹配
    text = description.replace("\n", " ")
    if "Similar to advertised products" in text: return "similar_products"
    if "Advertised products" in text: return "advertised_products"
    if "Category:" in text: return "category"
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

def score_category_audience(category_text, title):
    cat_tokens = set(tokenize(category_text))
    title_tokens = set(tokenize(title))
    if not cat_tokens or not title_tokens: return 0.0
    return len(cat_tokens & title_tokens) / len(cat_tokens)

def rank_and_trim_categories(category_audiences, title, min_keep=3, max_keep=5):
    scored = [(a, score_category_audience(a.description, title)) for a in category_audiences]
    scored.sort(key=lambda x: -x[1])
    n = len(scored)
    keep_count = min(n, max_keep) if n >= min_keep else n
    return [a for a, _ in scored[:keep_count]], [a for a, _ in scored[keep_count:]]


# ==================== DataProcessor ====================
class DataProcessor:
    def __init__(self, media_plan_path, asin_info_path, format_path):
        self.media_plan_path = media_plan_path
        self.asin_info_path = asin_info_path
        self.format_path = format_path
    def process(self):
        for p in [self.media_plan_path, self.asin_info_path, self.format_path]:
            if not os.path.exists(p):
                raise FileNotFoundError(f"找不到文件: {p}")
        df_asin = pd.read_excel(self.asin_info_path, sheet_name="ASIN information")
        df_asin.columns = [str(c).strip().replace("\n","").replace("\r","") for c in df_asin.columns]
        col_map = {c.replace(" ","").lower(): c for c in df_asin.columns}
        brand_col = next((col_map[k] for k in col_map if "brandname" in k), "Brand name")
        url_col = next((col_map[k] for k in col_map if "urlsite" in k), "url site")
        pl_col = next((col_map[k] for k in col_map if "productline" in k or "parentasin" in k), "Parent ASIN or Product line")
        country_col = next((col_map[k] for k in col_map if "country" in k), "Country")
        df_asin = df_asin.rename(columns={brand_col:"Brand name", url_col:"url site", pl_col:"Parent ASIN or Product line", country_col:"Country"})
        for col in ["Parent ASIN or Product line","Country","Brand name","url site"]:
            if col in df_asin.columns:
                df_asin[col] = df_asin[col].replace(r"^\s*$", pd.NA, regex=True).ffill()
        df_asin["Parent ASIN or Product line"] = df_asin["Parent ASIN or Product line"].apply(lambda v: str(v).strip() if pd.notna(v) else "")
        df_asin["Country"] = df_asin["Country"].apply(clean_text)
        df_asin["Brand name"] = df_asin["Brand name"].apply(clean_text)
        df_asin["url site"] = df_asin["url site"].apply(lambda v: re.sub(r"\s+","",str(v)) if pd.notna(v) else "")
        if "Child ASIN" in df_asin.columns:
            df_asin["Child ASIN"] = df_asin["Child ASIN"].apply(lambda v: clean_text(v).upper())
        df_format = pd.read_excel(self.format_path)
        df_format.columns = [str(c).strip() for c in df_format.columns]
        wb = pd.ExcelFile(self.media_plan_path)
        rows_list = []
        unique_brands = df_asin["Brand name"].replace("", pd.NA).dropna().unique()
        for brand in unique_brands:
            sheet = next((s for s in wb.sheet_names if clean_text(s) == brand), None)
            if not sheet: continue
            df_bp = pd.read_excel(self.media_plan_path, sheet_name=sheet)
            for pl_col in df_bp.columns[7:]:
                pl = str(pl_col).strip()
                for _, row in df_bp.iterrows():
                    base = str(row.iloc[0]).strip()
                    if not base or base.lower() == "nan": continue
                    if str(row[pl_col]).strip().upper() == "Y":
                        rows_list.append({"Brand": brand, "Product_line": pl, "Base_campaign_name": base})
        df_mp = pd.DataFrame(rows_list)
        if not df_mp.empty:
            pl_to_country = df_asin.dropna(subset=["Country"]).set_index("Parent ASIN or Product line")["Country"].to_dict()
            df_mp["Country"] = df_mp["Product_line"].map(pl_to_country)
            df_mp = df_mp.dropna(subset=["Country"])
        return df_mp, df_asin, df_format

# ==================== CampaignNameParser ====================
class CampaignNameParser:
    def parse(self, name):
        parts = [p.strip() for p in name.split("_") if p.strip()]
        pl = parts[0] if parts else name
        ad_type = next((p for p in parts if p in {"SD","SP","SB","SBV"}), "Unknown")
        kw_label = next((p for p in parts if p in SOURCE_HEADINGS), None)
        match_type = next((p for p in parts if p in {"Broad","Phrase","Exact"}), None)
        cost_type = next((p for p in parts if p in {"CPC","CPM","vCPM"}), None)
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
                if token in name: opt_type = mapped; break
        return CampaignMetadata(name, pl, ad_type, opt_type, kw_label, match_type, cost_type, remarketing_subtype)


# ==================== KeywordAnalysisResolver ====================
class KeywordAnalysisResolver:
    def __init__(self, working_dir, df_asin):
        self.working_dir = Path(working_dir)
        self.df_asin = df_asin

    def discover_workbooks(self):
        return sorted([p for p in self.working_dir.glob("*.xlsx") if "keyword analysis" in p.name.lower() and not p.name.startswith("~$")])

    def find_brand_for_product_line(self, pl):
        match = self.df_asin[self.df_asin["Parent ASIN or Product line"].astype(str).str.strip() == pl]
        if match.empty: return None
        brands = match["Brand name"].dropna().astype(str).str.strip().tolist()
        return brands[0] if brands else None

    def select_workbook(self, pl):
        brand = self.find_brand_for_product_line(pl)
        if not brand: raise ValueError(f"无法为产品线找到品牌: {pl}")
        norm_brand = clean_text(brand).lower()
        scored = []
        for wb in self.discover_workbooks():
            score = 0
            norm_name = clean_text(wb.stem).lower()
            if norm_brand and norm_brand in norm_name: score += 2
            if wb.name.lower().startswith("internal_"): score += 1
            scored.append((score, wb))
        if not scored: raise FileNotFoundError("目录中未发现 Keyword analysis 文件")
        scored.sort(key=lambda x: (-x[0], x[1].name.lower()))
        if scored[0][0] <= 0: raise FileNotFoundError(f"未找到与品牌匹配的 Keyword analysis 文件: {brand}")
        return scored[0][1]

    def select_sheet(self, wb_path, pl):
        wb = pd.ExcelFile(wb_path)
        norm_pl = clean_text(pl).lower()
        for s in wb.sheet_names:
            if norm_pl and norm_pl in clean_text(s).lower(): return s
        raise ValueError(f"文件 {wb_path.name} 中找不到产品线 sheet: {pl}")

    def extract_keywords(self, wb_path, sheet_name, heading):
        df = pd.read_excel(wb_path, sheet_name=sheet_name, header=None)
        known = set(SOURCE_HEADINGS.values())
        positions = []
        for r in range(df.shape[0]):
            for c in range(df.shape[1]):
                v = df.iat[r, c]
                if pd.isna(v): continue
                if str(v).strip() == heading: positions.append((r, c))
        if not positions: raise ValueError(f"sheet {sheet_name} 中找不到标题: {heading}")
        r, c = positions[0]
        results, blank = [], 0
        scan = r + 1
        while scan < df.shape[0]:
            v = df.iat[scan, c]
            text = str(v).strip() if pd.notna(v) else ""
            if text in known: break
            if not text:
                blank += 1
                if blank >= 3 and results: break
                scan += 1; continue
            blank = 0; results.append(text); scan += 1
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

        # 按 ASIN Category 过滤（本品=Child ASIN, 竞品=Competitor ASIN）
        cat_col = next((c for c in raw.columns if normalize_text(c) in {"asin category", "asincategory"}), None)
        if cat_col and asin_category:
            raw = raw[raw[cat_col].astype(str).str.strip() == asin_category]
            if raw.empty:
                raise ValueError(f"过滤 ASIN Category='{asin_category}' 后无数据")
            print(f"  📊 过滤 ASIN Category='{asin_category}': {len(raw)} 行")

        kw_col = next((c for c in raw.columns if "keyword" in normalize_text(c) or "searchterm" in normalize_text(c)), None)
        ps_col = next((c for c in raw.columns if normalize_text(c) in {"purchase share","purchaseshare"}), None)
        atc_col = next((c for c in raw.columns if normalize_text(c) in {"atc share","atcshare"}), None)
        cpf_col = next((c for c in raw.columns if "clickpurchasefunnel" in normalize_text(c)), None)
        apf_col = next((c for c in raw.columns if "atcpurchasefunnel" in normalize_text(c)), None)
        ss_col = next((c for c in raw.columns if normalize_text(c) in {"search share","searchshare"}), None)
        if kw_col is None: raise ValueError(f"{wb_path.name} 的 Raw Data 没找到关键词列")
        metrics = {}
        for _, row in raw.iterrows():
            kw = str(row.get(kw_col, "")).strip()
            if not kw: continue
            nk = normalize_text(kw)
            metrics[nk] = RawMetric(kw,
                self._f(row.get(ps_col)) if ps_col else None,
                self._f(row.get(atc_col)) if atc_col else None,
                self._f(row.get(cpf_col)) if cpf_col else None,
                self._f(row.get(apf_col)) if apf_col else None,
                self._f(row.get(ss_col)) if ss_col else None)
        return metrics

    @staticmethod
    def _f(v):
        if pd.isna(v): return None
        try: return float(v)
        except: return None


# ==================== KeywordScoringEngine ====================
class KeywordScoringEngine:
    def __init__(self, metadata, title):
        self.metadata = metadata
        self.title = title or ""

    def build_candidates(self, excel_kws, current_kws, raw_metrics):
        candidates = {}
        for i, kw in enumerate(excel_kws):
            nk = normalize_text(kw)
            if not nk: continue
            m = raw_metrics.get(nk)
            labels = self._build_excel_labels()
            c = candidates.get(nk)
            if c is None:
                candidates[nk] = KeywordCandidate(kw, nk, "excel", labels, i,
                    m.purchase_share if m else None, m.atc_share if m else None,
                    m.click_purchase_funnel if m else None, m.atc_purchase_funnel if m else None,
                    m.search_share if m else None,
                    excel_tier=self._excel_tier(labels))
                continue
            c.labels.update(labels)
            if m:
                c.purchase_share = m.purchase_share; c.atc_share = m.atc_share
                c.click_purchase_funnel = m.click_purchase_funnel; c.atc_purchase_funnel = m.atc_purchase_funnel
                c.search_share = m.search_share
        for i, (kw, _mt, _bid) in enumerate(current_kws):
            nk = normalize_text(kw)
            if not nk: continue
            m = raw_metrics.get(nk)
            c = candidates.get(nk)
            if c is None:
                candidates[nk] = KeywordCandidate(kw, nk, "system", {"system"}, i,
                    m.purchase_share if m else None, m.atc_share if m else None,
                    m.click_purchase_funnel if m else None, m.atc_purchase_funnel if m else None,
                    m.search_share if m else None,
                    is_system_keyword=True)
                continue
            if c.excel_tier == 1: continue
            c.source = "system"; c.is_system_keyword = True; c.labels.add("system")
            if c.excel_tier == 3: c.labels.discard("high_potential")
        for c in candidates.values():
            if "own" in c.labels and "competitor" in c.labels: c.labels.discard("competitor")
        return list(candidates.values())

    def rank(self, candidates):
        import math
        hb = [self._core(c) for c in candidates if not c.is_system_keyword and self._core(c) is not None and self._core(c) > 0]
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
            if exc == 0.0: c.exclusion_reason = "competitor_brand_exclusion"
        ranked = [c for c in candidates if c.final_score > 0]
        ranked.sort(key=lambda c: (-c.final_score, c.source_order, c.keyword.lower()))
        return ranked[:TOP_KEYWORD_LIMIT]

    def _build_excel_labels(self):
        labels = {"excel"}
        kl = self.metadata.keyword_label or ""
        if "本品" in kl: labels.add("own")
        if "竞品" in kl: labels.add("competitor")
        if "高转化" in kl: labels.add("high_conversion")
        if "高加购" in kl: labels.add("high_atc")
        if "高潜力" in kl: labels.add("high_potential")
        return labels

    @staticmethod
    def _excel_tier(labels):
        if "high_conversion" in labels or "high_atc" in labels: return 1
        if "high_potential" in labels: return 3
        return None

    @staticmethod
    def _core(c):
        if "high_atc" in c.labels: return c.atc_share * 100 if c.atc_share is not None else None
        return c.purchase_share * 100 if c.purchase_share is not None else None

    @staticmethod
    def _funnel(c):
        if "high_atc" in c.labels: return c.atc_purchase_funnel
        return c.click_purchase_funnel

    def _relevance(self, c):
        tt = set(tokenize(self.title)); kt = set(tokenize(c.keyword))
        if not tt or not kt:
            # title 为空时：无法判断相关性，给中性分数（不杀词）
            return 0.5
        overlap = kt & tt
        if overlap == kt: return 3.0
        if overlap: return 1.5
        return 0.5 if not c.is_system_keyword else 0.0

    def _detect_competitor_heads(self, candidates):
        """
        竞品品牌词动态排异：
        步骤1: title 第一个词 = Target_Brand，其余 = Safe_Attribute_Set
        步骤2: 遍历竞品词组中每个单词，找出不在白名单中的 Suspect_Word
        步骤3: 统计 Suspect_Word 出现在词首的频率，>=2 次判定为竞品品牌词
        """
        tt = tokenize(self.title)
        if not tt: return set()
        brand_token = tt[0]
        safe = set(tt) | STOPWORDS  # Target_Brand + Safe_Attribute_Set + 停用词

        # 步骤2: 扫描所有竞品词，收集 suspect words
        suspect_words = set()
        for c in candidates:
            if "competitor" not in c.labels: continue
            for token in tokenize(c.keyword):
                if token not in safe:
                    suspect_words.add(token)

        if not suspect_words:
            return set()

        # 步骤3: 统计 suspect words 出现在词首的频率
        head_freq = {}
        for c in sorted(candidates, key=lambda x: x.source_order):
            if "competitor" not in c.labels: continue
            ct = tokenize(c.keyword)
            if not ct: continue
            h = ct[0]
            if h in suspect_words:
                head_freq[h] = head_freq.get(h, 0) + 1

        # 频繁出现在词首 (>=2次) 的 suspect word = 竞品品牌词
        excluded = {t for t, n in head_freq.items() if n >= 2}
        if excluded:
            print(f"  🚫 竞品品牌词排异: {excluded}")
        return excluded

    @staticmethod
    def _exclusion(c, exc_heads):
        if "competitor" not in c.labels: return 1.0
        t = tokenize(c.keyword)
        if t and t[0] in exc_heads: return 0.0
        return 1.0


# ==================== 弹窗操作 ====================
async def open_popup_and_read_info(page, row, campaign_name=""):
    """
    点蓝色数字按钮 → 读 ASIN + title
    - SP: 直接在 Product 页面，不需要切 tab
    - SB/SBV 普通: Landing page tab → 读 ASIN → 读 title
    - SB_HP: Creative tab → 直接从 <a> 链接读 ASIN 和 title
    返回: (asins, combined_title)
    """
    blue_btn = row.locator("[col-id='numberOfAsinGroups'] button").first
    if await blue_btn.count() == 0:
        print("  ❌ 没找到蓝色数字按钮 [col-id='numberOfAsinGroups'] button")
        return [], None
    await blue_btn.click(force=True)
    await page.wait_for_timeout(1500)

    # 判断广告类型
    parts = [p.strip() for p in campaign_name.split("_") if p.strip()]
    ad_type = next((p for p in parts if p in {"SD", "SP", "SB", "SBV"}), "")
    is_sp = ad_type == "SP"
    is_hp = "SB_HP" in campaign_name.upper() or "_HP_" in campaign_name.upper()

    if is_sp:
        # SP: 打开后默认在 Product 页面，等待 ASIN 信息渲染
        print("  📋 SP 模式: 直接从 Product 页面读取...")
        try:
            await page.wait_for_selector("p:has-text('ASIN:')", state="visible", timeout=3000)
        except PlaywrightTimeoutError:
            # ASIN 文本没出现，可能需要点击 Product tab
            products_tab = page.locator("button#PRODUCTS").first
            if await products_tab.count() > 0:
                await products_tab.click()
                await page.wait_for_timeout(1000)
    elif is_hp:
        # SB_HP: 去 Creative tab
        print("  📋 SB_HP 模式: 从 Creative tab 读取...")
        creative_tab = page.locator("button#CREATIVE").first
        try:
            await creative_tab.wait_for(state="visible", timeout=5000)
            await creative_tab.click()
            await page.wait_for_timeout(1000)
        except PlaywrightTimeoutError:
            print("  ❌ 超时: Creative tab (button#CREATIVE) 5秒内未出现")
            return [], None
    else:
        # SB/SBV 普通: 去 Landing page tab
        lp_tab = page.locator('button#LANDING_PAGE').first
        try:
            await lp_tab.wait_for(state="visible", timeout=5000)
            await lp_tab.click()
            await page.wait_for_timeout(1000)
        except PlaywrightTimeoutError:
            print("  ❌ 超时: Landing page tab (button#LANDING_PAGE) 5秒内未出现")
            return [], None

    # 读取 ASIN 文本（SP 和普通 SB 都有 p:has-text('ASIN:')）
    asins = []
    if not is_hp:
        for attempt in range(3):
            try:
                await page.wait_for_selector("p:has-text('ASIN:')", state="visible", timeout=10000)
                asins_text = await page.locator("p:has-text('ASIN:')").all_inner_texts()
                asins = [t.replace("ASIN:", "").strip() for t in asins_text if "ASIN:" in t]
                if asins:
                    break
            except PlaywrightTimeoutError:
                print(f"  ⚠️ 第{attempt+1}次读取 ASIN 超时")
                if attempt < 2:
                    await page.wait_for_timeout(500)
                continue

        if not asins:
            print("  ❌ 3次重试后仍未读取到 ASIN")
            return [], None

    # 从弹窗里的 <a href*="/dp/"> 链接读取所有 title 文本 + 提取 ASIN
    titles = []
    asin_links = page.locator("a[href*='/dp/']")
    link_count = await asin_links.count()
    for j in range(link_count):
        try:
            text = (await asin_links.nth(j).inner_text()).strip()
            href = await asin_links.nth(j).get_attribute("href") or ""
            if text and len(text) > 5:
                titles.append(text)
            asin_match = re.search(r'/dp/([A-Z0-9]{10})', href)
            if asin_match and asin_match.group(1) not in asins:
                asins.append(asin_match.group(1))
        except:
            continue

    combined_title = " ".join(titles) if titles else None
    if titles:
        print(f"  📝 读取到 {len(titles)} 个 ASIN title")
        for t in titles:
            print(f"    - {t[:80]}...")
    else:
        print("  ⚠️ 弹窗内未找到 ASIN title 链接")

    return asins, combined_title


async def read_keywords_from_popup(page):
    """
    切到 Keyword tab，读取所有现有关键词
    """
    print("  🔄 切换到 Keyword tab...")

    # 尝试 button#KEYWORDS（SB/SBV 弹窗常用）
    kw_btn = page.locator("button#KEYWORDS").first
    if await kw_btn.count() > 0 and await kw_btn.is_visible():
        print("    找到 button#KEYWORDS")
        await kw_btn.click()
        await page.wait_for_timeout(800)
    else:
        # 备用: role=tab 的 Keyword
        kw_tab = page.get_by_role("tab", name=re.compile(r"^Keyword$", re.IGNORECASE)).first
        if await kw_tab.count() > 0 and await kw_tab.is_visible():
            print("    找到 role=tab 'Keyword'")
            await kw_tab.click()
            await page.wait_for_timeout(800)
        else:
            print("  ❌ 没找到 Keyword tab (button#KEYWORDS 和 role=tab 'Keyword' 都不存在)")
            return []

    # 等待关键词行出现（用弹窗内独有的 keywordText 列定位）
    print("  🔄 等待关键词表格行...")
    kw_cell_selector = "[col-id='keywordText'] .cell-renderer-content-text"
    try:
        await page.wait_for_selector(kw_cell_selector, state="visible", timeout=10000)
    except PlaywrightTimeoutError:
        print(f"  ❌ 超时: 关键词单元格 ({kw_cell_selector}) 10秒内未出现")
        return []

    # 通过 keywordText 列的所有单元格来读取关键词
    kw_cells = page.locator(kw_cell_selector)
    row_count = await kw_cells.count()
    print(f"    找到 {row_count} 个关键词单元格")
    results = []
    for j in range(row_count):
        try:
            kw = (await kw_cells.nth(j).inner_text()).strip()
            if not kw:
                continue
            # 找到同一行的 matchType 和 bid
            # 从 keywordText 单元格向上找到 .ag-row 父级
            row_el = kw_cells.nth(j).locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
            mt = ""
            bid = None
            try:
                mt_el = row_el.locator("[col-id='matchType'] .cell-renderer-content-text").first
                if await mt_el.count() > 0:
                    mt = (await mt_el.inner_text()).strip()
            except: pass
            try:
                bid_el = row_el.locator("[col-id='bid'] .cell-renderer-content-text").first
                if await bid_el.count() > 0:
                    bid_text = (await bid_el.inner_text()).strip()
                    bid_match = re.search(r"([0-9]+(?:\.[0-9]+)?)", bid_text)
                    bid = float(bid_match.group(1)) if bid_match else None
            except: pass
            results.append((kw, mt, bid))
        except Exception:
            continue
    return results


async def retry_click_until_visible(page, click_locator, wait_selector, max_retries=3, click_wait=1000, timeout=5000):
    """通用重试：点击 → 等待目标出现，失败则重试"""
    for attempt in range(max_retries):
        try:
            await click_locator.click(force=True)
            await page.wait_for_timeout(click_wait)
            target = page.locator(wait_selector).first
            await target.wait_for(state="visible", timeout=timeout)
            return True
        except (PlaywrightTimeoutError, Exception):
            if attempt < max_retries - 1:
                print(f"    ⏳ 重试 {attempt+2}/{max_retries}...")
                await page.wait_for_timeout(1500)
            continue
    return False


async def release_browser_memory(page):
    """定期释放浏览器内存，减少页面卡顿"""
    try:
        await page.evaluate("""() => {
            // 清理已分离的 DOM 节点引用
            if (window.gc) window.gc();
            // 清理 ag-grid 缓存的行数据
            var containers = document.querySelectorAll('.ag-body-viewport');
            containers.forEach(function(c) {
                // 触发 ag-grid 回收不可见行
                c.dispatchEvent(new Event('scroll'));
            });
        }""")
    except:
        pass


async def apply_keyword_optimization(page, ranked, match_type, is_sp=False):
    """
    优化流程（带重试验收）：
    1. 删除除第一行外的所有词（系统要求至少保留一个）
    2. 把第一行关键词改名为 "1"（占位）
    3. 添加新的排名词
    4. 删掉 "1" 占位词
    """
    # 用弹窗内独有的 keywordText 列定位行
    kw_cells = page.locator("[col-id='keywordText'] .cell-renderer-content-text")
    row_count = await kw_cells.count()
    print(f"  📋 当前弹窗内关键词: {row_count} 个")

    # ---- 步骤1: 选中除第一行外的所有行并删除 ----
    if row_count > 1:
        print(f"  🗑️ 删除第2~{row_count}行...")
        # 先点全选
        select_all = page.locator(
            "#tactical-recommendations-table\\:tactical-recommendations-table\\:bulkActions\\:selectAllCheckbox"
        ).first
        await select_all.click(force=True)
        await page.wait_for_timeout(800)

        # 再点第一行的 checkbox 取消勾选（保留第一行）
        # 兼容 SB (col-id="selectRow") 和 SP (col-id="select") 两种结构
        first_row_el = kw_cells.nth(0).locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
        first_cb = first_row_el.locator(
            "[col-id='selectRow'] input[type='checkbox'], "
            "[col-id='select'] input[type='checkbox']"
        ).first
        if await first_cb.count() > 0:
            await first_cb.click(force=True)
            await page.wait_for_timeout(800)

        # 等 Remove keywords 按钮可用再点
        rm_btn = page.get_by_role("button", name=re.compile(r"Remove keywords", re.IGNORECASE)).first
        for _ in range(10):
            if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                break
            await page.wait_for_timeout(500)
        await rm_btn.click(force=True)
        await page.wait_for_timeout(2000)
        print("  ✅ 已删除第2行及之后的关键词")

    # ---- 步骤2: 把第一行关键词改名为 "1"（带重试）----
    print("  ✏️ 第一行关键词改名为 '1'...")
    edit_trigger = page.locator(
        "[data-e2e-id='tactical-recommendations-table:tactical-recommendations-table:cell-keywordText:edit']"
    ).first
    kw_input_sel = "[data-e2e-id='tactical-recommendations-table:tactical-recommendations-table:cell-keywordText:input']"
    rename_ok = False
    for attempt in range(3):
        if await edit_trigger.count() > 0:
            await edit_trigger.click(force=True)
            await page.wait_for_timeout(800 + attempt * 500)

            kw_input = page.locator(kw_input_sel).first
            try:
                await kw_input.wait_for(state="visible", timeout=5000)
                await kw_input.click()
                await page.keyboard.press("Control+A")
                await kw_input.fill("1")
                await page.wait_for_timeout(300)

                save_edit_btn = page.locator(
                    "[data-e2e-id='tactical-recommendations-table:tactical-recommendations-table:cell-keywordText:save']"
                ).first
                await save_edit_btn.click(force=True)
                await page.wait_for_timeout(800)

                # 验收：检查第一行是否已变为 "1"
                first_kw = page.locator("[col-id='keywordText'] .cell-renderer-content-text").first
                first_text = (await first_kw.inner_text()).strip() if await first_kw.count() > 0 else ""
                if first_text == "1":
                    print("  ✅ 第一行已改名为 '1'（已验收）")
                    rename_ok = True
                    break
                else:
                    print(f"    ⚠️ 验收失败，当前值='{first_text}'，重试...")
            except PlaywrightTimeoutError:
                print(f"    ⏳ 第{attempt+1}次输入框未出现，重试...")
                await page.wait_for_timeout(1000)
        else:
            print("  ❌ 没找到 keywordText 编辑触发器")
            break

    if not rename_ok:
        print("  ⚠️ 改名为 '1' 未成功，继续尝试添加词...")

    # ---- 步骤3: 添加新关键词（带重试）----
    await page.wait_for_timeout(2000)
    print(f"  ➕ 添加 {len(ranked)} 个新关键词...")
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
            print(f"    ⏳ 第{attempt+1}次 Add keywords 未响应，重试...")
            await page.wait_for_timeout(2000)

    if not add_ok:
        print("  ❌ Add keywords 3次重试均失败")
        return

    await page.wait_for_timeout(500)

    # 设置 match type
    mt_map = {
        "Broad": "#add_keywords_checkbox_broad",
        "Phrase": "#add_keywords_checkbox_phrase",
        "Exact": "#add_keywords_checkbox_exact",
    }
    target_sel = mt_map.get(match_type or "Broad", "#add_keywords_checkbox_broad")
    for sel in mt_map.values():
        cb = page.locator(sel).first
        if await cb.count() == 0: continue
        checked = await cb.is_checked()
        should = (sel == target_sel)
        if checked != should: await cb.click(force=True)
        await page.wait_for_timeout(200)

    await page.locator("#add_keywords_bid").fill(str(DEFAULT_BID))
    await page.wait_for_timeout(300)
    await page.locator("#add_keywords_input").fill("\n".join(c.keyword for c in ranked))
    await page.wait_for_timeout(500)
    save_btn = page.get_by_role("button", name=re.compile(r"Save", re.IGNORECASE)).last
    await save_btn.click(force=True)
    await page.wait_for_timeout(2000)

    # ---- 步骤4: 删掉 "1" 占位词 ----
    print("  🗑️ 删除占位词 '1'...")
    await page.wait_for_timeout(1000)

    if is_sp:
        # SP 模式："1" 在最后一行
        await page.evaluate("""() => {
            const vp = document.querySelector('.ag-body-viewport');
            if (vp) vp.scrollTop = vp.scrollHeight;
        }""")
        await page.wait_for_timeout(1000)

        # 找最后一行
        last_row_center = page.locator(".ag-center-cols-container .ag-row-last").first
        if await last_row_center.count() > 0:
            await last_row_center.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)
            row_index = await last_row_center.get_attribute("row-index")
            print(f"    最后一行 row-index: {row_index}")

            # 直接找页面上所有 row-index 匹配的行里的 checkbox wrapper
            # 不限定 pinned-left，因为虚拟滚动可能导致容器不一致
            all_matching = page.locator(f".ag-row[row-index='{row_index}'] .ag-checkbox-input-wrapper")
            match_count = await all_matching.count()
            print(f"    找到 {match_count} 个匹配的 checkbox wrapper (row-index={row_index})")
            clicked_ok = False
            for k in range(match_count):
                try:
                    el = all_matching.nth(k)
                    if await el.is_visible():
                        await el.click(force=True)
                        await page.wait_for_timeout(500)
                        clicked_ok = True
                        print(f"    ✅ 已点击 checkbox wrapper")
                        break
                except:
                    continue

            if not clicked_ok:
                # 备用：用键盘 Space 选中当前聚焦的行
                await last_row_center.click()
                await page.wait_for_timeout(300)
                await page.keyboard.press("Space")
                await page.wait_for_timeout(500)
                print(f"    ✅ 已用 Space 键选中行")

            # 勾选后点 Remove
            rm_btn = page.get_by_role("button", name=re.compile(r"Remove keywords", re.IGNORECASE)).first
            for _ in range(10):
                if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                    break
                await page.wait_for_timeout(500)
            await rm_btn.click(force=True)
            await page.wait_for_timeout(1500)
            print("  ✅ SP 最后一行已删除")
        else:
            print("  ⚠️ 没找到最后一行")
    else:
        # SB/SBV 模式：遍历查找 "1"
        found_placeholder = False
        for scroll_attempt in range(5):
            kw_cells_new = page.locator("[col-id='keywordText'] .cell-renderer-content-text")
            new_count = await kw_cells_new.count()
            for j in range(new_count):
                try:
                    kw_text = (await kw_cells_new.nth(j).inner_text()).strip()
                    if kw_text == "1":
                        row_el = kw_cells_new.nth(j).locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
                        await row_el.scroll_into_view_if_needed()
                        await page.wait_for_timeout(500)
                        cb = row_el.locator("input[type='checkbox']").first
                        if await cb.count() > 0:
                            await cb.click(force=True)
                            await page.wait_for_timeout(500)
                            found_placeholder = True
                        break
                except:
                    continue
            if found_placeholder:
                break
            await page.evaluate("""() => {
                const vp = document.querySelector('.ag-body-viewport');
                if (vp) vp.scrollTop = vp.scrollHeight;
            }""")
            await page.wait_for_timeout(800)

        if found_placeholder:
            await page.wait_for_timeout(500)
            rm_btn = page.get_by_role("button", name=re.compile(r"Remove keywords", re.IGNORECASE)).first
            for _ in range(10):
                if await rm_btn.count() > 0 and await rm_btn.is_enabled():
                    break
                await page.wait_for_timeout(500)
            await rm_btn.click(force=True)
            await page.wait_for_timeout(1500)
            print("  ✅ 占位词 '1' 已删除")
        else:
            print("  ⚠️ 没找到占位词 '1'")

    print("  ✅ 关键词替换完成")


# AG Grid audience 表格操作
# 关键：AG Grid 有 center + pinned-left 两个容器，每行在两个容器中各有一份
# 必须用 row-index 属性精确定位，只在 center container 内读取内容，
# 但删除按钮在 center container 的 [col-id='delete'] 列内


async def navigate_to_audience_tab(page):
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
            print(f"  ⏳ 第{attempt+1}次 Audiences tab 加载超时，重试...")
            await page.wait_for_timeout(1500)
    print("  ❌ Audiences tab 3次重试均失败")
    return False


async def read_audiences_from_popup(page):
    """读取 audience 列表，通过 center container 的 row-index 逐行读取"""
    container = page.locator(".ag-center-cols-container")
    rows = container.locator("> .ag-row")
    try:
        await rows.first.wait_for(state="visible", timeout=10000)
    except PlaywrightTimeoutError:
        return []
    count = await rows.count()
    audiences = []
    seen_indices = set()
    for i in range(count):
        try:
            row = rows.nth(i)
            ri = await row.get_attribute("row-index")
            if ri in seen_indices:
                continue
            seen_indices.add(ri)
            desc_cell = row.locator("[col-id='audienceExpression']").first
            if await desc_cell.count() == 0:
                continue
            desc = (await desc_cell.inner_text()).strip()
            if not desc or len(desc) < 5:
                continue
            bid = None
            bid_el = row.locator("[col-id='bid'] .cell-renderer-content-text").first
            if await bid_el.count() > 0:
                bid_text = (await bid_el.inner_text()).strip()
                m = re.search(r"([0-9]+(?:\.[0-9]+)?)", bid_text)
                bid = float(m.group(1)) if m else None
            audiences.append(AudienceRow(desc, classify_audience_type(desc), bid, int(ri)))
        except Exception as e:
            print(f"  ⚠️ 读取第{i}行audience失败: {e}")
    print(f"  📋 读取到 {len(audiences)} 个 audience")
    for a in audiences:
        print(f"    - [{a.audience_type}] {a.description[:60]}... bid={a.bid}")
    return audiences


async def _delete_row_by_index(page, row_index):
    """通过 row-index 精确定位 center container 内的行并点击删除"""
    for attempt in range(3):
        try:
            target_row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{row_index}']").first
            if await target_row.count() == 0:
                # 可能需要滚动
                await page.evaluate("""(idx) => {
                    const vp = document.querySelector('.ag-body-viewport');
                    if (vp) vp.scrollTop = idx * 60;
                }""", row_index)
                await page.wait_for_timeout(800)
                if await target_row.count() == 0:
                    return False
            del_btn = target_row.locator("[col-id='delete'] button").first
            if await del_btn.count() == 0:
                return False
            await del_btn.click(force=True)
            await page.wait_for_timeout(1200)
            return True
        except Exception as e:
            print(f"    ⏳ 删除row-index={row_index}失败(第{attempt+1}次): {e}")
            await page.wait_for_timeout(1000)
    return False


async def delete_audiences_except(page, keep_type):
    """删除所有不匹配 keep_type 的 audience 行"""
    for _ in range(30):
        audiences = await read_audiences_from_popup(page)
        target = None
        for a in audiences:
            if a.audience_type != keep_type:
                target = a
                break
        if target is None:
            return
        print(f"  🗑️ 删除 [{target.audience_type}] row-index={target.row_index}: {target.description.replace(chr(10), ' ')[:50]}...")
        ok = await _delete_row_by_index(page, target.row_index)
        if not ok:
            print(f"    ⚠️ 删除失败，跳过")
            return


async def delete_audiences_by_types(page, types_to_delete):
    """删除指定类型集合的 audience 行"""
    for _ in range(30):
        audiences = await read_audiences_from_popup(page)
        target = None
        for a in audiences:
            if a.audience_type in types_to_delete:
                target = a
                break
        if target is None:
            return
        print(f"  🗑️ 删除 [{target.audience_type}] row-index={target.row_index}: {target.description.replace(chr(10), ' ')[:50]}...")
        ok = await _delete_row_by_index(page, target.row_index)
        if not ok:
            print(f"    ⚠️ 删除失败，跳过")
            return


async def modify_audience_bid(page, row_index, new_bid):
    """通过 row-index 精确定位行并修改 bid"""
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
                    print(f"    ⚠️ row-index={row_index} 不存在，跳过")
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
            print(f"    ✅ row-index={row_index} bid 已修改为 {new_bid}")
            return True
        except Exception as e:
            print(f"    ⏳ 修改bid失败(第{attempt+1}次): {e}")
            try: await page.keyboard.press("Escape"); await page.wait_for_timeout(500)
            except: pass
            await page.wait_for_timeout(1500)
    print(f"    ⚠️ row-index={row_index} bid修改3次均失败，继续")
    return False


async def modify_all_audience_bids(page, new_bid):
    """修改当前所有 audience 行的 bid"""
    audiences = await read_audiences_from_popup(page)
    for a in audiences:
        await modify_audience_bid(page, a.row_index, new_bid)


async def optimize_sd_remarketing(page, row, metadata, bid):
    subtype = metadata.remarketing_subtype
    print(f"  🎯 SD再营销子类型: {subtype}")
    blue_btn = row.locator("[col-id='numberOfAsinGroups'] button").first
    if await blue_btn.count() == 0:
        print("  ❌ 没找到蓝色数字按钮")
        return False
    await blue_btn.click(force=True)
    await page.wait_for_timeout(1500)

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
                    if text and len(text) > 5: titles.append(text)
                except: continue
            title = " ".join(titles) if titles else None
            print(f"  📝 ASIN title: {(title or 'N/A')[:120]}")

    if not await navigate_to_audience_tab(page): return False
    audiences = await read_audiences_from_popup(page)
    if not audiences:
        print("  ⚠️ 没有audience行，跳过")
        return False

    if subtype == "本品浏览再营销":
        # 删除所有非 advertised_products 的行
        await delete_audiences_except(page, "advertised_products")
        await modify_all_audience_bids(page, bid)

    elif subtype == "相似品浏览再营销":
        # 删除所有非 similar_products 的行
        await delete_audiences_except(page, "similar_products")
        await modify_all_audience_bids(page, bid)

    elif subtype == "类目浏览再营销":
        # 先删除 advertised_products 和 similar_products
        await delete_audiences_by_types(page, {"advertised_products", "similar_products"})
        # 对剩余 category 打分排名
        remaining = await read_audiences_from_popup(page)
        category_rows = [a for a in remaining if a.audience_type == "category"]
        if category_rows and title:
            to_keep, to_remove = rank_and_trim_categories(category_rows, title)
            print(f"  📊 类目打分: 保留{len(to_keep)}个，删除{len(to_remove)}个")
            for a in to_keep:
                print(f"    ✅ 保留: {a.description[:50]}... (score={score_category_audience(a.description, title):.2f})")
            # 删除低分类目：逐轮扫描，用 row_index 精确删除
            keep_descs = {a.description for a in to_keep}
            for _ in range(30):
                current = await read_audiences_from_popup(page)
                target = None
                for a in current:
                    if a.audience_type == "category" and a.description not in keep_descs:
                        target = a
                        break
                if target is None:
                    break
                print(f"    🗑️ 删除 row-index={target.row_index}: {target.description.replace(chr(10), ' ')[:50]}...")
                await _delete_row_by_index(page, target.row_index)
        await modify_all_audience_bids(page, bid)

    print("  ✅ SD再营销 audience 优化完成")
    return True


async def close_popup(page):
    """点 X 按钮关闭弹窗，回到列表页面"""
    try:
        # 优先找弹窗顶部的返回/关闭按钮（不在 ag-grid 表格内的 fa-times）
        # SD弹窗的关闭X在 side-panel 外层，不在 .ag-row 内
        close_btns = page.locator("svg.fa-times, svg[class*='fa-times']")
        btn_count = await close_btns.count()
        for i in range(btn_count):
            btn = close_btns.nth(i)
            # 排除在 ag-row 内的删除按钮（那些是 audience 行的删除）
            in_grid = btn.locator("xpath=ancestor::div[contains(@class,'ag-row')]").first
            if await in_grid.count() > 0:
                continue
            # 找到不在 ag-row 内的 fa-times，就是弹窗关闭按钮
            if await btn.is_visible():
                await btn.click(force=True)
                await page.wait_for_timeout(2000)
                return
        # 备用：用 path d 值精确匹配
        x_btn = page.locator("svg path[d*='M20.44 367.51']").first
        if await x_btn.count() > 0:
            await x_btn.click(force=True)
            await page.wait_for_timeout(2000)
            return
        # 最后备用：Escape
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(1500)
    except Exception:
        try:
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(1500)
        except:
            pass


# ==================== 主测试流程 ====================
async def main():
    print("[test] 加载 Media Plan 数据...")
    processor = DataProcessor(MEDIA_PLAN_PATH, ASIN_INFO_PATH, FORMAT_PATH)
    df_mp, df_asin, _df_fmt = processor.process()
    parser = CampaignNameParser()
    resolver = KeywordAnalysisResolver(KEYWORD_ANALYSIS_DIR, df_asin)

    print("[test] 探测 Chrome 调试端口...")
    from chrome_port_finder import get_cdp_url
    cdp_url = get_cdp_url()
    pw = await async_playwright().start()
    browser = await pw.chromium.connect_over_cdp(cdp_url)
    context = browser.contexts[0]
    pages = context.pages
    print(f"[test] 共 {len(pages)} 个页面标签")
    # 找到广告页面（包含 advertising.amazon 的那个）
    page = None
    for p in pages:
        url = p.url or ""
        print(f"  - {url[:80]}")
        if "advertising.amazon" in url or "global-action-center" in url:
            page = p
    if not page:
        # 没找到就用最后一个非 chrome:// 页面
        for p in reversed(pages):
            if not (p.url or "").startswith("chrome://"):
                page = p
                break
    if not page:
        page = pages[-1]
    print(f"[test] 选中页面: {page.url}")

    # 排名缓存：同一 campaign 只是 match type 不同时，复用排名结果
    ranking_cache = {}  # key: campaign_name去掉match_type → value: ranked list

    print("[test] 等待表格加载...")
    try:
        await page.wait_for_selector(".ag-center-cols-container > .ag-row", timeout=15000)
    except PlaywrightTimeoutError:
        print(f"  ❌ 页面上没找到表格行，当前URL: {page.url}")
        return

    # 从分页状态读取总行数
    total_campaigns = 0
    try:
        status_el = page.locator("#tactical-recommendations-table\\:pagination-page-status").first
        if await status_el.count() > 0:
            status_text = (await status_el.inner_text()).strip()
            m = re.search(r'of\s+(\d+)\s+results', status_text)
            if m:
                total_campaigns = int(m.group(1))
                print(f"[test] 总共 {total_campaigns} 个 campaign ({status_text})")
    except:
        pass
    if total_campaigns == 0:
        rows = page.locator(".ag-center-cols-container > .ag-row")
        total_campaigns = await rows.count()
        print(f"[test] 可见 {total_campaigns} 行 campaign")

    print()
    processed_count = 0
    last_memory_release = 0
    idx = 0

    while idx < total_campaigns:
        # 每处理15个释放内存（同一值只触发一次）
        if processed_count > 0 and processed_count % 15 == 0 and processed_count != last_memory_release:
            last_memory_release = processed_count
            print(f"\n  🧹 已处理 {processed_count} 个，释放浏览器内存...")
            await release_browser_memory(page)
            await page.wait_for_timeout(500)

        # 用 row-index 定位当前行，从当前位置向下滚动查找
        row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{idx}']").first
        if await row.count() == 0 or not await row.is_visible():
            # 向下滚动尝试，ag-grid 虚拟滚动需要逐步加载
            found_row = False
            for scroll_try in range(10):
                # 向下滚动一段距离
                await page.mouse.wheel(0, 600)
                await page.wait_for_timeout(800)
                if await row.count() > 0 and await row.is_visible():
                    found_row = True
                    break
            if not found_row:
                print(f"\n[test] row-index={idx} 向下滚动10次后仍不可见，已到底。共优化 {processed_count} 个")
                break

        await row.scroll_into_view_if_needed()
        await page.wait_for_timeout(300)

        # 检查预算，跳过已优化的
        # 注意：SP 列表的预算列可能需要横向滚动才可见，读不到就当作未优化
        skip_row = False
        budget_cell = row.locator("[col-id='campaignBudget'] .cell-renderer-content-text").first
        if await budget_cell.count() > 0:
            try:
                budget_text = (await budget_cell.inner_text(timeout=2000)).strip()
                budget_match_val = re.search(r"([0-9]+(?:\.[0-9]+)?)", budget_text)
                if budget_match_val:
                    current_budget = float(budget_match_val.group(1))
                    if current_budget in {10.0, 16.0}:
                        skip_row = True
            except:
                pass  # 读不到预算，不跳过
        if skip_row:
            idx += 1
            continue

        # ---- 读取 campaign name（纯文本，不点击）----
        campaign_name = (await row.locator("[col-id='campaignName'] .cell-renderer-content-text").inner_text()).strip()
        metadata = parser.parse(campaign_name)

        if metadata.ad_type not in TARGET_AD_TYPES:
            print(f"[skip] {campaign_name} -> ad_type={metadata.ad_type}")
            idx += 1
            continue

        # ==================== 路由：按 optimization_type 分派 ====================
        if metadata.optimization_type == "Remarketing":
            print(f"\n[row {idx}] ▶ {campaign_name}")
            print(f"  解析: pl={metadata.product_line} | ad={metadata.ad_type} | subtype={metadata.remarketing_subtype}")
            if not DEFAULT_DRY_RUN:
                try:
                    bid = calculate_bid(metadata)
                    print(f"  💰 计算出价: {bid}")
                    success = await optimize_sd_remarketing(page, row, metadata, bid)
                    await close_popup(page)
                    if success:
                        budget = BUDGET_RULES.get(metadata.ad_type, 10)
                        budget_trigger = row.locator(
                            "[data-e2e-id='tactical-recommendations-table:cell-campaignBudget:edit'], "
                            "[data-takt-id='tactical-recommendations-table:cell-campaignBudget:edit']"
                        ).first
                        budget_ok = False
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
                                budget_ok = True
                                break
                            except PlaywrightTimeoutError:
                                print(f"    ⏳ 预算修改第{attempt+1}次超时，重试...")
                                await page.wait_for_timeout(1500)
                        if budget_ok:
                            print(f"[row {idx}] ✅ SD再营销优化完成 (budget={budget})")
                            await page.wait_for_timeout(2000)
                            try:
                                await page.wait_for_selector(f".ag-center-cols-container > .ag-row[row-index='{idx}']", state="attached", timeout=10000)
                            except:
                                await page.wait_for_timeout(3000)
                        else:
                            print(f"[row {idx}] ⚠️ 预算修改超时，audience已优化成功")
                    else:
                        print(f"[row {idx}] ⚠️ SD再营销优化未成功")
                except Exception as e:
                    print(f"[row {idx}] ❌ SD再营销执行失败: {e}")
                    await close_popup(page)
            else:
                print(f"[row {idx}] 🔍 DRY RUN - 不执行修改")
            processed_count += 1
            await page.wait_for_timeout(2000)
            try:
                await page.wait_for_selector(".ag-center-cols-container > .ag-row", state="visible", timeout=15000)
            except:
                await page.wait_for_timeout(3000)
            idx += 1
            continue

        if not metadata.is_keyword_campaign:
            print(f"[skip] {campaign_name} -> {metadata.optimization_type}")
            idx += 1
            continue

        # ---- 以下为现有 Keyword 优化流程（SB/SP）----

        print(f"\n[row {idx}] ▶ {campaign_name}")
        print(f"  解析: pl={metadata.product_line} | ad={metadata.ad_type} | label={metadata.keyword_label} | match={metadata.match_type}")

        # ---- 步骤1: 点蓝色数字 → Landing page/Creative → 读 ASIN + title ----
        asins, title = await open_popup_and_read_info(page, row, campaign_name)
        if not asins:
            print(f"[row {idx}] ⚠️ 没读取到 ASIN，跳过")
            await close_popup(page)
            idx += 1
            continue
        print(f"  📦 ASIN: {asins}")
        print(f"  📝 title: {(title or 'N/A')[:120]}")

        # ---- 步骤2: 回到 Keyword tab，读取现有关键词 ----
        current_keywords = await read_keywords_from_popup(page)
        if not current_keywords:
            print(f"[row {idx}] ⚠️ 没读取到关键词，跳过")
            await close_popup(page)
            idx += 1
            continue
        print(f"  🔑 当前关键词: {len(current_keywords)} 个")
        for kw, mt, bid in current_keywords[:3]:
            print(f"    - {kw} | {mt} | bid={bid}")

        # ---- 步骤3: 构建优化计划（评分排名）----
        # 生成 cache key：去掉 match type 部分，其余相同则复用排名
        match_types = {"Broad", "Phrase", "Exact"}
        cache_parts = [p for p in campaign_name.split("_") if p.strip() not in match_types]
        cache_key = "_".join(cache_parts)

        if cache_key in ranking_cache:
            ranked = ranking_cache[cache_key]
            print(f"  ⚡ 命中缓存！复用已计算的 {len(ranked)} 个关键词（仅 match type 不同）")
        else:
            try:
                print(f"  🔄 查找 workbook...")
                wb_path = resolver.select_workbook(metadata.product_line)
                print(f"    workbook: {wb_path.name}")

                print(f"  🔄 查找 sheet...")
                sheet = resolver.select_sheet(wb_path, metadata.product_line)
                print(f"    sheet: {sheet}")

                heading = SOURCE_HEADINGS[metadata.keyword_label]
                print(f"  🔄 提取 Excel 关键词 (heading={heading})...")
                excel_kws = resolver.extract_keywords(wb_path, sheet, heading)
                print(f"    Excel 候选词: {len(excel_kws)} 个")

                print(f"  🔄 加载 Raw Data 指标 (产品线={metadata.product_line})...")
                kw_label = metadata.keyword_label or ""
                if "本品" in kw_label:
                    asin_cat = "Child ASIN"
                elif "竞品" in kw_label:
                    asin_cat = "Competitor ASIN"
                else:
                    asin_cat = None
                raw_metrics = resolver.load_raw_metrics(wb_path, product_line=metadata.product_line, asin_category=asin_cat)
                print(f"    Raw Data 指标: {len(raw_metrics)} 个词有数据")

                print(f"  🔄 评分排名...")
                scoring = KeywordScoringEngine(metadata, title)
                candidates = scoring.build_candidates(excel_kws, current_keywords, raw_metrics)
                print(f"    候选池: {len(candidates)} 个 (Excel + 系统词合并去重后)")
                ranked = scoring.rank(candidates)
                print(f"    排名结果: {len(ranked)} 个")

                # 存入缓存
                ranking_cache[cache_key] = ranked
            except FileNotFoundError as e:
                print(f"[row {idx}] ❌ 文件未找到: {e}")
                await close_popup(page)
                idx += 1
                continue
            except ValueError as e:
                print(f"[row {idx}] ❌ 数据错误: {e}")
                await close_popup(page)
                idx += 1
                continue
            except Exception as e:
                print(f"[row {idx}] ❌ 构建计划未知错误: {type(e).__name__}: {e}")
                await close_popup(page)
                idx += 1
                continue

        print(f"  📊 排名前 {len(ranked)} 词:")
        for rank_i, c in enumerate(ranked, 1):
            print(f"    {rank_i:02d}. {c.keyword} | score={c.final_score:.2f} | src={c.source}")

        # ---- 步骤4: 执行优化 ----
        if not DEFAULT_DRY_RUN:
            try:
                await apply_keyword_optimization(page, ranked, metadata.match_type, is_sp=(metadata.ad_type == "SP"))

                # 点 X 按钮关闭弹窗回到列表
                await close_popup(page)

                # 改预算（回到列表后操作，带重试）
                budget = BUDGET_RULES.get(metadata.ad_type, 10)
                budget_trigger = row.locator(
                    "[data-e2e-id='tactical-recommendations-table:cell-campaignBudget:edit'], "
                    "[data-takt-id='tactical-recommendations-table:cell-campaignBudget:edit']"
                ).first
                budget_ok = False
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
                        budget_ok = True
                        break
                    except PlaywrightTimeoutError:
                        print(f"    ⏳ 预算修改第{attempt+1}次超时，重试...")
                        await page.wait_for_timeout(1500)
                if budget_ok:
                    print(f"[row {idx}] ✅ 优化完成 (budget={budget})")
                    # 改预算后页面会刷新，等表格重新渲染稳定
                    await page.wait_for_timeout(2000)
                    try:
                        await page.wait_for_selector(
                            f".ag-center-cols-container > .ag-row[row-index='{idx}']",
                            state="attached", timeout=10000
                        )
                    except:
                        # 表格刷新后等一下
                        await page.wait_for_timeout(3000)
                else:
                    print(f"[row {idx}] ⚠️ 预算修改3次均超时，关键词已替换成功")
            except Exception as e:
                print(f"[row {idx}] ❌ 执行优化失败: {e}")
                await close_popup(page)
        else:
            print(f"[row {idx}] 🔍 DRY RUN - 不执行修改")
            await close_popup(page)

        processed_count += 1

        # 改预算后页面可能刷新，等表格重新稳定再继续下一行
        await page.wait_for_timeout(2000)
        try:
            await page.wait_for_selector(".ag-center-cols-container > .ag-row", state="visible", timeout=15000)
        except:
            await page.wait_for_timeout(3000)

        # 从当前位置继续往下一行，不滚回顶部
        idx += 1
        next_row = page.locator(f".ag-center-cols-container > .ag-row[row-index='{idx}']").first
        if await next_row.count() > 0 and await next_row.is_visible():
            await next_row.scroll_into_view_if_needed()
            await page.wait_for_timeout(500)
        # 如果下一行不可见，交给循环开头的滚动逻辑处理

    print(f"\n[test] ✅ 全部完成！共 {total_campaigns} 个 campaign，优化了 {processed_count} 个，页面保持打开")
    # 不调用 pw.stop()，保持浏览器和页面打开


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[test] 手动中断，页面保持打开")
    except Exception as e:
        print(f"\n[test] ❌ 未捕获异常: {e}")
        print("[test] 页面保持打开，可手动检查")
