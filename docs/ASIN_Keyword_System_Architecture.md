# ASIN 关键词系统架构文档

本文档描述 `ASIN Keyword Data Scraping.py` 和 `keywords_analysis.py` 两个脚本的结构逻辑。

---

## 系统总览

```
┌─────────────────────────┐     ┌─────────────────────────┐
│  ASIN Keyword Data      │     │  Keywords Analysis      │
│  Scraping.py            │────▶│  keywords_analysis.py   │
│  (数据采集)              │     │  (数据分析 + 报表生成)    │
└─────────────────────────┘     └─────────────────────────┘
         │                                │
         ▼                                ▼
  Reporting-keyword/              Reporting-keyword/
  ├── CA.csv                      ├── Internal_Brand_CA Keyword analysis.xlsx
  ├── US.csv                      ├── Client_Brand_CA Keyword analysis.xlsx
  └── ...                         └── (源 CSV 自动删除)
```

两个脚本串联执行：Scraping 采集完数据后自动调用 Analysis 生成报表。

---

## 脚本 1：ASIN Keyword Data Scraping.py

### 功能
从 Amazon 内部 Superset 数据平台自动抓取各国家/站点的 ASIN 关键词趋势数据，按国家导出为 CSV 文件。

### 数据流

```
Excel Template/                          Amazon Superset
ASIN_Input_Template    ──读取──▶  脚本  ──浏览器操作──▶  数据平台
For Keywords.xlsx                   │                    │
                                    │                    ▼
                                    │              自动下载 CSV
                                    │                    │
                                    ▼                    ▼
                              Reporting-keyword/CA.csv
                              Reporting-keyword/US.csv
                              ...
```

### 执行流程

```
启动
 │
 ├── 1. 读取 Excel 配置
 │    ├── 从 ASIN_Input_Template For Keywords.xlsx 读取
 │    ├── 提取时间维度 (generate data for week 列，默认 4 周)
 │    ├── 提取筛选参数 (min_purchase, min_atc, max_keywords)
 │    └── 按国家分组，合并 Child ASIN + Competitor ASIN 并去重
 │
 ├── 2. 启动 Chrome 浏览器
 │    ├── 有头模式启动 (端口 9223)
 │    └── 用户可见浏览器窗口
 │
 ├── 3. 连接浏览器 & 登录检测
 │    ├── Playwright 连接 localhost:9223
 │    ├── 导航到 Superset 数据平台
 │    ├── 检测登录状态 (8秒超时)
 │    └── 未登录 → 等待用户手动登录
 │
 ├── 4. 逐国家循环处理
 │    │
 │    │  ┌─────────── 每个国家 ───────────┐
 │    │  │                                │
 │    │  │  [1] 设置 Region (NA/EU/FE)    │
 │    │  │  [2] 设置 Marketplace          │
 │    │  │      (Amazon.ca 等)            │
 │    │  │  [3] 粘贴 ASIN 列表           │
 │    │  │      (剪贴板批量粘贴)          │
 │    │  │  [4] 首次设置全局参数          │
 │    │  │      ├── 切换到数据表 Tab      │
 │    │  │      ├── 设置时间范围          │
 │    │  │      └── 清空语言筛选          │
 │    │  │  [5] 点击 Apply Filters        │
 │    │  │  [6] 等待数据加载 (3秒)        │
 │    │  │  [7] 导出 CSV                  │
 │    │  │      ├── 点击三点菜单          │
 │    │  │      ├── 选择 Export to CSV     │
 │    │  │      └── 保存到 Reporting-      │
 │    │  │          keyword/{国家}.csv     │
 │    │  │                                │
 │    │  └────────────────────────────────┘
 │    │
 │    └── 会话过期检测：如果 URL 跳转到 login 页面，自动重新加载
 │
 ├── 5. 关闭浏览器
 │
 └── 6. 自动调用 keywords_analysis.py
      └── 采集完成后立即启动分析模块
```

### 关键配置

| 配置项 | 来源 | 说明 |
|--------|------|------|
| ASIN 列表 | Excel Template/ASIN_Input_Template For Keywords.xlsx | Child ASIN + Competitor ASIN |
| 时间维度 | Excel 表头 "generate data for week" 列 | 默认 4 周 |
| 输出目录 | Reporting-keyword/ | 按国家代码命名 CSV |
| 数据平台 | superset.sds.advertising.amazon.dev | Amazon 内部 Superset |

---

## 脚本 2：keywords_analysis.py

### 功能
读取 Scraping 脚本下载的 CSV 原始数据，结合 ASIN 模板中的产品线/品牌信息，通过 SQL 聚合计算关键词指标，按"高转化/高加购/高潜力"三个维度分类，生成格式化的 Excel 分析报表（Internal 版 + Client 版）。

### 数据流

```
ASIN_Input_Template          Reporting-keyword/
For Keywords.xlsx             ├── CA.csv (原始数据)
      │                       ├── US.csv
      │                       │
      ▼                       ▼
   ┌──────────────────────────────┐
   │     keywords_analysis.py     │
   │                              │
   │  1. 读取 Excel 配置          │
   │  2. 读取各国 CSV 原始数据     │
   │  3. SQL 聚合计算指标          │
   │  4. 三维度分类筛选            │
   │  5. 生成格式化 Excel 报表     │
   │  6. 删除已处理的源 CSV        │
   └──────────────────────────────┘
              │
              ▼
   Reporting-keyword/
   ├── Internal_Brand_CA Keyword analysis_时间戳.xlsx
   ├── Client_Brand_CA Keyword analysis_时间戳.xlsx
   └── (CA.csv 已删除)
```

### 执行流程

```
启动
 │
 ├── 1. 读取 Excel 配置
 │    ├── 从 ASIN_Input_Template For Keywords.xlsx 读取
 │    ├── 提取动态参数：
 │    │    ├── min_purchase (最低购买数筛选)
 │    │    ├── min_atc (最低加购数筛选)
 │    │    ├── max_keywords (每类最大关键词数，最低 10)
 │    │    └── weeks_to_generate (数据周数)
 │    └── 数据清洗：列名标准化、空值填充、ASIN 大写去符号
 │
 ├── 2. 识别待分析的国家
 │    └── 从 Excel 的 Country 列提取所有唯一国家
 │
 ├── 3. 逐国家循环处理
 │    │
 │    │  ┌─────────── 每个国家 ───────────┐
 │    │  │                                │
 │    │  │  [1] 查找源数据文件            │
 │    │  │      Reporting-keyword/        │
 │    │  │      优先 {国家}.csv           │
 │    │  │      其次 {国家}.xlsx          │
 │    │  │                                │
 │    │  │  [2] 加载到 SQLite 内存数据库   │
 │    │  │                                │
 │    │  │  [3] 逐品牌生成报表            │
 │    │  │      │                         │
 │    │  │      ▼                         │
 │    │  │  ┌── SQL 核心计算 ──┐          │
 │    │  │  │                  │          │
 │    │  │  │ ASIN 分类        │          │
 │    │  │  │ (本品 vs 竞品)   │          │
 │    │  │  │      ▼           │          │
 │    │  │  │ 关键词聚合       │          │
 │    │  │  │ (按产品线+类型)  │          │
 │    │  │  │      ▼           │          │
 │    │  │  │ 计算指标         │          │
 │    │  │  │ CTR/CVR/ATC%     │          │
 │    │  │  │      ▼           │          │
 │    │  │  │ 产品线平均值     │          │
 │    │  │  │      ▼           │          │
 │    │  │  │ 三维度标记       │          │
 │    │  │  │ 高转化/高加购/   │          │
 │    │  │  │ 高潜力           │          │
 │    │  │  └──────────────────┘          │
 │    │  │                                │
 │    │  │  [4] 生成 Excel 报表           │
 │    │  │      ├── Internal 版 (完整)    │
 │    │  │      └── Client 版 (精简)      │
 │    │  │                                │
 │    │  │  [5] 删除源 CSV 文件           │
 │    │  │                                │
 │    │  └────────────────────────────────┘
 │
 └── 完成
```

### 三维度分类逻辑

```
                    CTR > 平均值?
                   /            \
                 是              否
                /                  \
         CVR > 平均值?          任一指标 > 平均值?
        /           \           /            \
      是             否       是              否
      │              │        │               │
  ✅ 高转化      ATC% > 平均值?  ✅ 高潜力     ❌ 不标记
  (T1)          /        \      (T3)
              是          否
              │           │
          ✅ 高加购    检查其他指标
          (T2)         是否 > 平均值
                       → 高潜力(T3)
```

互斥规则：T1 > T2 > T3（已标记高转化的不再标记高加购或高潜力）

### Excel 报表结构

```
生成的 Excel 文件
 │
 ├── 📗 Home (导航页)
 │    ├── 各产品线的跳转链接
 │    ├── 数据日期范围
 │    └── 指标说明表 (仅 Internal 版)
 │
 ├── 📊 Raw Data (仅 Internal 版)
 │    ├── 所有关键词的完整指标数据
 │    └── 条件格式高亮 (T1=深绿, T2=浅绿, T3=最浅绿)
 │
 └── 📄 各产品线 Sheet (每个产品线一个)
      ├── 6 列关键词分类展示：
      │    ├── 本品-高转化 | 竞品-高转化
      │    ├── 本品-高加购 | 竞品-高加购
      │    └── 本品-高潜力 | 竞品-高潜力
      ├── 重叠词高亮 (本品和竞品共有的词用颜色标记)
      └── 按 Purchases > AddToCart > Clicks 降序排列
```

### 关键指标计算

| 指标 | 计算公式 | 说明 |
|------|----------|------|
| CTR | Clicks / Searches | 点击率 |
| CVR | Purchases / Clicks | 转化率 |
| ATC Rate | AddToCart / Clicks | 加购率 |
| Search Share | 关键词搜索量 / 产品线总搜索量 | 搜索份额 |
| Click Share | 关键词点击量 / 产品线总点击量 | 点击份额 |
| Purchase Share | 关键词购买量 / 产品线总购买量 | 购买份额 |
| Click Purchase Funnel | Purchase Share / Click Share | 点击转化漏斗 |
| ATC Purchase Funnel | Purchase Share / ATC Share | 加购转化漏斗 |

---

## 两个脚本的协作关系

```
时间线
──────────────────────────────────────────────────────▶

  ASIN Keyword Data Scraping.py          keywords_analysis.py
  ┌────────────────────────┐             ┌────────────────────┐
  │ 读取 Excel 配置         │             │                    │
  │ 启动 Chrome             │             │                    │
  │ 登录检测                │             │                    │
  │ 逐国家抓取数据          │             │                    │
  │ 导出 CSV 到             │             │                    │
  │ Reporting-keyword/      │─── 自动 ──▶│ 读取 Excel 配置     │
  │ 关闭浏览器              │   调用      │ 读取 CSV 源数据     │
  └────────────────────────┘             │ SQL 聚合计算        │
                                         │ 三维度分类          │
                                         │ 生成 Excel 报表     │
                                         │ 删除源 CSV          │
                                         └────────────────────┘

  输入文件:                               输入文件:
  - ASIN_Input_Template                   - ASIN_Input_Template
    For Keywords.xlsx                       For Keywords.xlsx
                                          - Reporting-keyword/*.csv

  输出文件:                               输出文件:
  - Reporting-keyword/                    - Reporting-keyword/
    {国家}.csv                              Internal_*_Keyword analysis.xlsx
                                            Client_*_Keyword analysis.xlsx
```
