import pandas as pd
import sqlite3
import os
import sys
import time
import numpy as np
from datetime import datetime, timedelta
import traceback

# ================= Configuration =================
PERCENT_COLS_INDICES = [8, 9, 10, 11, 12, 13] 
MASTER_FILENAME = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Excel Template", "ASIN_Input_Template For Keywords.xlsx"))
REPORTING_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Reporting-keyword"))

def main():
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    
    os.chdir(application_path)
    print(f"[Analysis Module] Current Working Directory: {application_path}")

    if not os.path.exists(MASTER_FILENAME):
        print(f"[Error] Master file '{MASTER_FILENAME}' not found.")
        return

    print(f"[Info] Reading master file: {MASTER_FILENAME} ...")
    try:
        df_master = pd.read_excel(MASTER_FILENAME)
    except Exception as e:
        print(f"[Error] Failed to read master file: {e}")
        return

    # ================= 读取动态配置参数 =================
    weeks_to_generate = None
    min_purchase = None
    min_atc = None
    max_keywords = None

    if not df_master.empty:
        for col in df_master.columns:
            col_str = str(col).lower()
            val = str(df_master[col].iloc[0]).replace('nan', '').replace('None', '').strip()
            if not val: continue
            
            # 防止 8.0 变成 80 的小数点截断术
            val = val.split('.')[0] 
            val_num = ''.join(filter(str.isdigit, val)) 
            
            if "filtered by purchase" in col_str and val_num:
                min_purchase = int(val_num)
            elif "filtered by add to cart" in col_str and val_num:
                min_atc = int(val_num)
            elif "maximum filtered keywords" in col_str and val_num:
                max_keywords = int(val_num)
                if max_keywords < 10: max_keywords = 10
            elif "generate data for week" in col_str and val_num:
                weeks_to_generate = int(val_num)

# ================= [新增] 强制默认时间范围 =================
    if not weeks_to_generate:
        weeks_to_generate = 4  # 如果表格里没填或者没抓到，默认设为 4 周

    end_dt = datetime.now() - timedelta(days=1)
    start_dt = end_dt - timedelta(weeks=weeks_to_generate)
    date_range_str = f"Data Date Range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')} ({weeks_to_generate} Weeks)"

    print(f"[Info] Config loaded -> Min Purchase: {min_purchase}, Min ATC: {min_atc}, Max KW: {max_keywords}, Weeks: {weeks_to_generate}")
    # =========================================================
    # ================= [终极防弹清洗] 数据预处理 =================
    # 1. 强制清理所有列名（去除前后空格，防止 "Child ASIN " 这种低级错误）
    df_master.columns = [str(c).strip() for c in df_master.columns]

    # 2. 识别关键列
    target_parent_col = "Product Line"
    for col in df_master.columns:
        if "Parent" in str(col) and "Product Line" not in str(col):
            df_master.rename(columns={col: target_parent_col}, inplace=True)
            break
    if target_parent_col not in df_master.columns:
        df_master.rename(columns={df_master.columns[0]: target_parent_col}, inplace=True)

    country_col_name = "Country"
    for col in df_master.columns:
        if "country" in str(col).lower() or "marketplace" in str(col).lower() or "site" in str(col).lower():
            df_master.rename(columns={col: country_col_name}, inplace=True)
            break
    if country_col_name not in df_master.columns:
        country_col_name = df_master.columns[3]

    target_brand_col = "Brand"
    found_brand = False
    for col in df_master.columns:
        if "brand" in str(col).lower():
            df_master.rename(columns={col: target_brand_col}, inplace=True)
            found_brand = True
            break
    if not found_brand:
        df_master[target_brand_col] = "Default_Brand"

    # 3. 彻底将所有空缺值转为 np.nan，为向下填充铺平道路
    df_master.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df_master.replace(['nan', 'None', 'NAN'], np.nan, inplace=True)

    # 4. 关键列向下填充 (ffill)
    df_master[country_col_name] = df_master[country_col_name].ffill()
    df_master[target_parent_col] = df_master[target_parent_col].ffill()
    df_master[target_brand_col] = df_master[target_brand_col].ffill()

    # 5. 极限清洗数据内容
    # Country 和 Product Line: 填补空白、去空格
    for col in [country_col_name, target_parent_col]:
        df_master[col] = df_master[col].fillna('Unknown').astype(str).str.strip()

    # Brand: 只保留字母和数字，去除特殊符号（支持原样大小写）
    df_master[target_brand_col] = df_master[target_brand_col].fillna('Default_Brand').astype(str)
    df_master[target_brand_col] = df_master[target_brand_col].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip()
    # 如果剔除符号后变空（比如纯中文品牌或者什么都没填），则赋予默认名
    df_master.loc[df_master[target_brand_col] == '', target_brand_col] = 'Default_Brand'

    # ASIN 列: 强制全大写、去符号、转为干净列表防止报错
    for col in ['Child ASIN', 'Competitor ASIN']:
        if col in df_master.columns:
            clean_asins = df_master[col].fillna('').astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper().replace('NAN', '').tolist()
            df_master[col] = clean_asins
    # ===============================================================

    countries = df_master[country_col_name].dropna().unique()
    countries = [c for c in countries if str(c).strip() and str(c) != 'Unknown']
    print(f"[Info] Sites to analyze: {len(countries)}")

    # ================= 嵌套大循环: 国家 -> 品牌 =================
    for country in countries:
        country = str(country).strip()
        if not country: continue

        print(f"\n[Info] ================= Processing Site: {country} =================")
        
        conn = None
        try:
            conn = sqlite3.connect(':memory:') 
            
            # 找到并加载当前国家的源数据
            source_table_name = f"{country}_source"
            found_source = False
            current_files = os.listdir(REPORTING_DIR)
            target_data_file = None
            
            if f"{country}.csv" in current_files:
                target_data_file = f"{country}.csv"
            elif f"{country}.xlsx" in current_files:
                target_data_file = f"{country}.xlsx"
            else:
                for filename in current_files:
                    if (country in filename and filename != MASTER_FILENAME and 
                        not filename.endswith('.db') and "Keyword analysis" not in filename and 
                        not filename.endswith('.exe') and "chrome_user_data" not in filename):
                        target_data_file = filename
                        break
            
            if target_data_file:
                target_full_path = os.path.join(REPORTING_DIR, target_data_file)
                print(f"[Info] Using data source file: {target_full_path}")
                try:
                    if target_data_file.endswith('.csv'): df_source = pd.read_csv(target_full_path)
                    else: df_source = pd.read_excel(target_full_path)
                    
                    # 动态捕捉 ASIN 列并深度清洗
                    asin_col = None
                    for c in df_source.columns:
                        if str(c).strip().lower() == 'asin':
                            asin_col = c
                            break
                    
                    if asin_col:
                        clean_source_asins = df_source[asin_col].fillna('').astype(str).str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.upper().replace('NAN', '').tolist()
                        df_source[asin_col] = clean_source_asins
                        df_source.rename(columns={asin_col: 'asin'}, inplace=True)

                    df_source.to_sql(source_table_name, conn, if_exists='replace', index=False)
                    found_source = True
                except Exception as e:
                    print(f"[Warning] Failed to read data file: {e}")

            if not found_source:
                print(f"[Warning] Data source file for {country} not found, skipping.")
                continue

            # 筛选出当前国家的所有数据
            df_country = df_master[df_master[country_col_name] == country].copy()
            
            # 找出当前国家包含的所有品牌
            brands_in_country = df_country[target_brand_col].dropna().unique()

            # --------- 按品牌分割生成报表 ---------
            for brand in brands_in_country:
                brand_str = str(brand).strip()
                if not brand_str: continue

                print(f"\n[Info] ---> Generating Report for Brand: {brand_str} (Site: {country})")
                
                # 提取当前 国家 + 品牌 的核心模板数据，存入SQL临时表
                df_country_brand = df_country[df_country[target_brand_col] == brand_str].copy()
                current_master_table = "current_master"
                df_country_brand.to_sql(current_master_table, conn, if_exists='replace', index=False)

                # --- SQL Core Logic (核心表变成了当前品牌专用的 current_master) ---
                sql_query = f"""
                WITH asin_classification AS (
                    SELECT `{target_parent_col}`, TRIM(UPPER(`Child ASIN`)) AS asin, 'Child ASIN' AS asin_type
                    FROM `{current_master_table}` WHERE `Child ASIN` IS NOT NULL AND `Child ASIN` <> ''
                    UNION ALL
                    SELECT `{target_parent_col}`, TRIM(UPPER(`Competitor ASIN`)) AS asin, 'Competitor ASIN' AS asin_type
                    FROM `{current_master_table}` WHERE `Competitor ASIN` IS NOT NULL AND `Competitor ASIN` <> ''
                ),
                keyword_stats AS (
                    SELECT TRIM(UPPER(`asin`)) AS asin, `keywords` AS keyword,
                    CAST(`SUM(Num of Searches)` AS INTEGER) AS `Num Searches`,
                    CAST(`SUM(Num of Clicks)` AS INTEGER) AS `Num Clicks`,
                    CAST(`SUM(Num of Add to Cart)` AS INTEGER) AS `Num AddToCart`,
                    CAST(`SUM(Num of Purchases)` AS INTEGER) AS `Num Purchases`
                    FROM `{source_table_name}`
                    WHERE `asin` NOT IN ('26108', 'keyword', '筛选行(F)', 'Num Seanches')
                ),
                keyword_aggregated AS (
                    SELECT COALESCE(ac.`{target_parent_col}`, 'Uncategorized') AS `{target_parent_col}`,
                    COALESCE(ac.asin_type, 'Uncategorized') AS asin_type,
                    ks.keyword,
                    SUM(ks.`Num Searches`) AS `Num Searches`,
                    SUM(ks.`Num Clicks`) AS `Num Clicks`,
                    SUM(ks.`Num AddToCart`) AS `Num AddToCart`,
                    SUM(ks.`Num Purchases`) AS `Num Purchases`,
                    ROUND(1.0 * SUM(ks.`Num Clicks`) / NULLIF(SUM(ks.`Num Searches`), 0), 4) AS ctr,
                    ROUND(1.0 * SUM(ks.`Num Purchases`) / NULLIF(SUM(ks.`Num Clicks`), 0), 4) AS cvr,
                    ROUND(1.0 * SUM(ks.`Num AddToCart`) / NULLIF(SUM(ks.`Num Clicks`), 0), 4) AS addtocart_rate
                    FROM keyword_stats ks
                    LEFT JOIN asin_classification ac ON ks.asin = ac.asin
                    GROUP BY ac.`{target_parent_col}`, ac.asin_type, ks.keyword
                ),
                parent_asin_stats AS (
                    SELECT `{target_parent_col}`, asin_type,
                    SUM(`Num Searches`) AS total_searches, SUM(`Num Clicks`) AS total_clicks,
                    SUM(`Num AddToCart`) AS total_addtocart, SUM(`Num Purchases`) AS total_purchases,
                    ROUND(1.0 * SUM(`Num Clicks`) / NULLIF(SUM(`Num Searches`), 0), 4) AS avg_ctr,
                    ROUND(1.0 * SUM(`Num Purchases`) / NULLIF(SUM(`Num Clicks`), 0), 4) AS avg_cvr,
                    ROUND(1.0 * SUM(`Num AddToCart`) / NULLIF(SUM(`Num Clicks`), 0), 4) AS avg_addtocart_rate
                    FROM keyword_aggregated GROUP BY `{target_parent_col}`, asin_type
                ),
                final_metrics AS (
                    SELECT ka.*, pas.total_searches, pas.total_clicks, pas.total_addtocart, pas.total_purchases,
                    pas.avg_ctr, pas.avg_cvr, pas.avg_addtocart_rate,
                    
                    CASE WHEN ka.ctr > COALESCE(pas.avg_ctr, 0) AND ka.cvr > COALESCE(pas.avg_cvr, 0) THEN 'Y' ELSE 'N' END AS `High conversion rate`,
                    CASE WHEN (ka.ctr > COALESCE(pas.avg_ctr, 0) AND ka.cvr > COALESCE(pas.avg_cvr, 0)) THEN 'N' 
                         WHEN ka.ctr > COALESCE(pas.avg_ctr, 0) AND ka.addtocart_rate > COALESCE(pas.avg_addtocart_rate, 0) THEN 'Y' ELSE 'N' END AS `High add-to-cart rate`,
                    CASE WHEN (ka.ctr > COALESCE(pas.avg_ctr, 0) AND ka.cvr > COALESCE(pas.avg_cvr, 0)) THEN 'N' 
                         WHEN (ka.ctr > COALESCE(pas.avg_ctr, 0) AND ka.addtocart_rate > COALESCE(pas.avg_addtocart_rate, 0)) THEN 'N' 
                         WHEN ka.ctr > COALESCE(pas.avg_ctr, 0) OR ka.cvr > COALESCE(pas.avg_cvr, 0) OR ka.addtocart_rate > COALESCE(pas.avg_addtocart_rate, 0) THEN 'Y' ELSE 'N' END AS `High potential`
                    FROM keyword_aggregated ka
                    LEFT JOIN parent_asin_stats pas ON ka.`{target_parent_col}` = pas.`{target_parent_col}` AND ka.asin_type = pas.asin_type
                )
                
                SELECT 
                    `{target_parent_col}`, asin_type AS `ASIN Category`, keyword,
                    `Num Searches`, `Num Clicks`, `Num AddToCart`, `Num Purchases`,
                    COALESCE(avg_ctr, 0) AS `AV.CTR`, COALESCE(avg_cvr, 0) AS `AV.CVR`, COALESCE(avg_addtocart_rate, 0) AS `AV.ADD TO CART`,
                    ctr AS `CTR`, cvr AS `CVR`, addtocart_rate AS `ADD TO CART Rate`,
                    `High conversion rate`, `High add-to-cart rate`, `High potential`,
                    CASE WHEN `Num Clicks` > 0 THEN 1.0 * `Num Clicks` / `Num Purchases` ELSE 0 END AS `CPO`,
                    CASE WHEN total_searches > 0 THEN 1.0 * `Num Searches` / total_searches ELSE 0 END AS `SEARCH SHARE`,
                    CASE WHEN total_clicks > 0 THEN 1.0 * `Num Clicks` / total_clicks ELSE 0 END AS `CLICK SHARE`,
                    CASE WHEN total_addtocart > 0 THEN 1.0 * `Num AddToCart` / total_addtocart ELSE 0 END AS `ATC SHARE`,
                    CASE WHEN total_purchases > 0 THEN 1.0 * `Num Purchases` / total_purchases ELSE 0 END AS `PURCHASE SHARE`
                FROM final_metrics
                ORDER BY `{target_parent_col}` DESC, asin_type DESC, `Num Searches` DESC;
                """

                raw_df = pd.read_sql(sql_query, conn)
                
                # ================= 彻底剔除所有未匹配到产品线的数据 =================
                raw_df = raw_df[(raw_df[target_parent_col] != 'Uncategorized') & (raw_df['ASIN Category'] != 'Uncategorized')].copy()
                
                if raw_df.empty:
                    print(f"[Warning] No valid data found for {brand_str} in {country}, skipping file generation.")
                    continue

                raw_df['Impression Purchase Funnel'] = raw_df.apply(lambda x: x['PURCHASE SHARE'] / x['SEARCH SHARE'] if x['SEARCH SHARE'] > 0 else 0, axis=1)
                raw_df['Click Purchase Funnel'] = raw_df.apply(lambda x: x['PURCHASE SHARE'] / x['CLICK SHARE'] if x['CLICK SHARE'] > 0 else 0, axis=1)
                raw_df['ATC Purchase Funnel'] = raw_df.apply(lambda x: x['PURCHASE SHARE'] / x['ATC SHARE'] if x['ATC SHARE'] > 0 else 0, axis=1)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # ================= 生成特定品牌、特定国家的两版报表 =================
                for report_type in ['Internal', 'Client']:
                    
                    base_filename = f"{report_type}_{brand_str}_{country} Keyword analysis_{timestamp}.xlsx"
                    output_filename = os.path.join(REPORTING_DIR, base_filename)
                    print(f"    - Generating {report_type} report: {output_filename} ...")

                    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
                        workbook = writer.book
                        font_name = 'DengXian' 

                        link_format = workbook.add_format({'font_name': font_name, 'font_color': 'blue', 'underline': True})
                        raw_data_fmt = workbook.add_format({'font_name': font_name, 'border': 1})
                        percent_fmt = workbook.add_format({'font_name': font_name, 'border': 1, 'num_format': '0.00%'})
                        decimal_fmt = workbook.add_format({'font_name': font_name, 'border': 1, 'num_format': '0.00'})
                        header_own_format = workbook.add_format({'font_name': font_name, 'bold': True, 'font_color': 'white', 'bg_color': '#4472C4', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
                        header_comp_format = workbook.add_format({'font_name': font_name, 'bold': True, 'font_color': 'white', 'bg_color': '#ED7D31', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
                        row_odd_format = workbook.add_format({'border': 1, 'font_name': font_name, 'valign': 'vcenter', 'text_wrap': True, 'bg_color': '#FFFFFF'})
                        row_even_format = workbook.add_format({'border': 1, 'font_name': font_name, 'valign': 'vcenter', 'text_wrap': True, 'bg_color': '#F2F2F2'})
                        warning_format = workbook.add_format({'font_name': font_name, 'bold': True, 'font_color': '#C00000', 'bg_color': '#FFF2CC', 'align': 'left', 'valign': 'vcenter', 'border': 1})
                        logic_format = workbook.add_format({'font_name': font_name, 'size': 10, 'text_wrap': True, 'align': 'left', 'valign': 'top', 'bg_color': '#F2F2F2', 'font_color': '#595959', 'border': 1})
                        score_format = workbook.add_format({'font_name': font_name, 'bold': True, 'size': 11, 'bg_color': '#E7E6E6', 'border': 1, 'align': 'left'})

                        green_fmt = workbook.add_format({'bg_color': '#92D050', 'font_color': 'black'})  
                        light_green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': 'black'}) 
                        lighter_green_fmt = workbook.add_format({'bg_color': '#E2EFDA', 'font_color': 'black'}) 

                        guide_header_fmt = workbook.add_format({'font_name': font_name, 'bold': True, 'bg_color': '#404040', 'font_color': 'white', 'border': 1})
                        guide_cell_fmt = workbook.add_format({'font_name': font_name, 'border': 1, 'text_wrap': True, 'valign': 'vcenter'})
                        guide_red_fmt = workbook.add_format({'font_name': font_name, 'border': 1, 'text_wrap': True, 'valign': 'vcenter', 'font_color': 'red', 'bold': True})

                        overlap_fmt_1 = workbook.add_format({'bg_color': '#E0CFE9', 'font_name': font_name, 'border': 1})
                        overlap_fmt_2 = workbook.add_format({'bg_color': '#FCE4D6', 'font_name': font_name, 'border': 1})
                        overlap_fmt_3 = workbook.add_format({'bg_color': '#D9E1F2', 'font_name': font_name, 'border': 1})

                        # ================= [完美防错版] 智能工作表命名系统 =================
                        parent_asins = raw_df[target_parent_col].unique()
                        sheet_name_map = {}
                        used_sheet_names = set(['Home', 'Raw Data']) # 预留系统表名，防止冲突

                        for parent in parent_asins:
                            if pd.isna(parent) or str(parent).strip() == '': continue
                            
                            # 1. 基础清理（去除非法字符：/ \ ? * : [ ]）
                            clean_name = str(parent).replace('/', '_').replace('\\', '_').replace(':', '').replace('*', '').replace('?', '').replace('[', '').replace(']', '').strip()
                            
                            # 2. 截取前30个字符 (Excel 最大支持 31，留出余量)
                            base_name = clean_name[:30]
                            
                            # 3. 查重逻辑：如果名字撞车了才加上后缀 _1, _2
                            if base_name not in used_sheet_names:
                                final_safe_name = base_name
                            else:
                                counter = 1
                                while True:
                                    suffix = f"_{counter}"
                                    # 重新截取，为后缀腾出空间
                                    test_name = f"{clean_name[:30-len(suffix)]}{suffix}"
                                    if test_name not in used_sheet_names:
                                        final_safe_name = test_name
                                        break
                                    counter += 1
                                    
                            used_sheet_names.add(final_safe_name)
                            sheet_name_map[parent] = final_safe_name
                        # ===================================================================

                        # [Sheet] Home
                        ws_home = workbook.add_worksheet("Home")
                        ws_home.set_tab_color('#70AD47') 
                        ws_home.write(0, 0, "Navigate to Analysis Sheet", header_own_format)
                        ws_home.set_column(0, 0, 50)
                        
                        if date_range_str:
                            date_format = workbook.add_format({'font_name': font_name, 'bold': True, 'size': 12, 'font_color': 'white', 'bg_color': '#70AD47', 'align': 'center', 'valign': 'vcenter'})
                            ws_home.merge_range('B1:H1', date_range_str, date_format)
                            ws_home.set_column('B:H', 15)
                            
                        row_cursor = 1
                        for parent in parent_asins: 
                            if pd.isna(parent) or str(parent).strip() == '': continue
                            
                            safe_sheet_name = sheet_name_map[parent]
                            # 【核心修复】：显示文字使用完整原名 parent，跳转链接使用安全短名 safe_sheet_name
                            ws_home.write_url(row_cursor, 0, f"internal:'{safe_sheet_name}'!A1", link_format, string=f"Go to Analysis: {parent}")
                            row_cursor += 1

                        if report_type == 'Internal':
                            row_cursor += 2
                            ws_home.write(row_cursor, 0, "Metric Guidelines / Definitions", guide_header_fmt)
                            ws_home.write(row_cursor, 1, "Calculation Logic", guide_header_fmt)
                            
                            guidelines = [
                                ("Calculation Level", "Metrics are calculated at the Product Line level (comprising either Children or Competitors)"),
                                ("High conversion rate (T1)", "CTR > Avg & CVR > Avg"),
                                ("High add-to-cart rate (T2)", "CTR > Avg & ATC% > Avg"),
                                ("High potential (T3)", "Any metric > Avg "),
                                ("AV.CTR", "Total Clicks / Total Impressions for the Product Line (Children or Competitors)"),
                                ("AV.CVR", "Total Purchases / Total Clicks for the Product Line (Children or Competitors)"),
                                ("AV.ADD TO CART", "Total Add-to-Carts / Total Clicks for the Product Line (Children or Competitors)"),
                                ("SEARCH SHARE", "Keyword Impressions / Total Impressions for the Product Line (Children or Competitors)"),
                                ("CLICK SHARE", "Keyword Clicks / Total Clicks for the Product Line (Children or Competitors)"),
                                ("ATC SHARE", "Keyword Add-to-Carts / Total Add-to-Carts for the Product Line (Children or Competitors)"),
                                ("PURCHASE SHARE", "Keyword Purchases / Total Purchases for the Product Line (Children or Competitors)"),
                                ("Impression Purchase Funnel", "PURCHASE SHARE / SEARCH SHARE"),
                                ("Click Purchase Funnel", "PURCHASE SHARE / CLICK SHARE"),
                                ("ATC Purchase Funnel", "PURCHASE SHARE / ATC SHARE")
                            ]       
                            
                            for idx, (metric, logic) in enumerate(guidelines):
                                ws_home.write(row_cursor + 1 + idx, 0, metric, guide_cell_fmt)
                                if metric == "Calculation Level":
                                    ws_home.write(row_cursor + 1 + idx, 1, logic, guide_red_fmt)
                                else:
                                    ws_home.write(row_cursor + 1 + idx, 1, logic, guide_cell_fmt)
                            
                            ws_home.set_column(1, 1, 60)

                        # [Sheet] Raw Data
                        if report_type == 'Internal':
                            raw_sheet_name = 'Raw Data'
                            ws_raw = workbook.add_worksheet(raw_sheet_name)
                            ws_raw.set_tab_color('#7F7F7F')
                            ws_raw.set_zoom(80)
                            ws_raw.freeze_panes(1, 0)
                            
                            for col_num, value in enumerate(raw_df.columns.values):
                                ws_raw.write(0, col_num, value, header_own_format)
                                is_percent = col_num in PERCENT_COLS_INDICES or "Share" in str(value).title() or "CTR" in str(value) or "CVR" in str(value)
                                is_decimal = col_num in [16, 21, 22, 23]

                                if is_decimal: ws_raw.set_column(col_num, col_num, 12, decimal_fmt)
                                elif is_percent: ws_raw.set_column(col_num, col_num, 12, percent_fmt)
                                else: ws_raw.set_column(col_num, col_num, 12, raw_data_fmt)
                            
                            raw_df_safe = raw_df.fillna('') 
                            data_rows = raw_df_safe.values.tolist()
                            for r_idx, row in enumerate(data_rows):
                                ws_raw.write_row(r_idx + 1, 0, row)
                            
                            cols_list = list(raw_df.columns)
                            rows_count = len(data_rows)
                            
                            if 'High conversion rate' in cols_list:
                                t1_idx = cols_list.index('High conversion rate')
                                ws_raw.conditional_format(1, t1_idx, rows_count, t1_idx, {'type': 'cell', 'criteria': 'equal to', 'value': '"Y"', 'format': green_fmt})
                            if 'High add-to-cart rate' in cols_list:
                                t2_idx = cols_list.index('High add-to-cart rate')
                                ws_raw.conditional_format(1, t2_idx, rows_count, t2_idx, {'type': 'cell', 'criteria': 'equal to', 'value': '"Y"', 'format': light_green_fmt})
                            if 'High potential' in cols_list:
                                t3_idx = cols_list.index('High potential')
                                ws_raw.conditional_format(1, t3_idx, rows_count, t3_idx, {'type': 'cell', 'criteria': 'equal to', 'value': '"Y"', 'format': lighter_green_fmt})

                        # [Sheet] Analysis per Parent ASIN
                        for p_idx, parent in enumerate(parent_asins):
                            if pd.isna(parent) or str(parent).strip() == '': continue
                            
                            # 获取安全短名用于创建 Sheet 页
                            safe_sheet_name = sheet_name_map[parent]
                            
                            sub_df = raw_df[raw_df[target_parent_col] == parent].copy()
                            own_df = sub_df[sub_df['ASIN Category'] == 'Child ASIN']
                            comp_df = sub_df[sub_df['ASIN Category'] == 'Competitor ASIN']
                            
                            def get_filtered_data(df, type_col):
                                if type_col not in df.columns: return pd.DataFrame()
                                cond = (df[type_col] == 'Y')
                                if min_purchase is None and min_atc is None:
                                    cond = cond & (df['Num Purchases'] >= 0)
                                else:
                                    if min_purchase is not None: cond = cond & (df['Num Purchases'] >= min_purchase)
                                    if min_atc is not None: cond = cond & (df['Num AddToCart'] >= min_atc)
                                        
                                result_df = df[cond].sort_values(
                                    ['Num Purchases', 'Num AddToCart', 'Num Clicks'], 
                                    ascending=[False, False, False]
                                )[['keyword', 'Num Purchases', 'Num AddToCart', 'Num Clicks', 'SEARCH SHARE']].reset_index(drop=True)
                                
                                if max_keywords is not None: result_df = result_df.head(max_keywords)
                                return result_df

                            d1 = get_filtered_data(own_df, 'High conversion rate')
                            d2 = get_filtered_data(comp_df, 'High conversion rate')
                            d3 = get_filtered_data(own_df, 'High add-to-cart rate')
                            d4 = get_filtered_data(comp_df, 'High add-to-cart rate')
                            d5 = get_filtered_data(own_df, 'High potential')
                            d6 = get_filtered_data(comp_df, 'High potential')
                            
                            comp_prefix = "Competitor" if report_type == 'Internal' else "Category"
                            comp_short = "Comp" if report_type == 'Internal' else "Category"
                            comp_score_name = "Comp ASIN" if report_type == 'Internal' else "Category"
                            
                            display_df = pd.concat([
                                d1['keyword'].rename("Own - High conversion rate"),
                                d2['keyword'].rename(f"{comp_prefix} - High conversion rate"),
                                d3['keyword'].rename("Own - High add-to-cart rate"),
                                d4['keyword'].rename(f"{comp_prefix} - High add-to-cart rate"),
                                d5['keyword'].rename("Own - High potential"),
                                d6['keyword'].rename(f"{comp_prefix} - High potential")
                            ], axis=1)

                            # >>> 核心：在这里使用安全的短名真正创建底部标签页 <<<
                            ws = writer.book.add_worksheet(safe_sheet_name)
                            ws.write_url('A1', "internal:'Home'!A1", link_format, string="Return to Home")
                            
                            if report_type == 'Internal':
                                header_row = 6  # 内部版表头在第7行 (0-indexed 为 6)
                                data_row = 7    # 内部版数据在第8行 (0-indexed 为 7)
                                
                                note_parts = []
                                if min_purchase is not None: note_parts.append(f"Purchase >= {min_purchase}")
                                if min_atc is not None: note_parts.append(f"Add To Cart >= {min_atc}")
                                    
                                note_text = "Note: Categories Filtered by " + " AND ".join(note_parts) + "." if note_parts else "Note: All Categories Filtered by Purchase > 1."
                                if max_keywords is not None: note_text += f" (Max displayed per category: {max_keywords})"
                                ws.merge_range('A2:F2', note_text, warning_format)
                                
                                logic_def = (
                                    "See Home Sheet for detailed Guidelines.\n"
                                    "1. HCR (T1): CTR > Avg & CVR > Avg\n"
                                    "2. High ATC (T2): CTR > Avg & ATC% > Avg (Excl. HCR)\n"
                                    "3. Pot (T3): CTR > Avg OR CVR > Avg OR ATC% > Avg"
                                )
                                ws.merge_range('A3:F6', logic_def, logic_format)
                                ws.write('H4', "当数据量过小时，不会出现报表 (Note: Charts omitted if valid data points < 2)", warning_format)
                            else:
                                # Client 版本：直接顶上去
                                header_row = 1
                                data_row = 2

                            ws.set_row(header_row, 40)
                            for idx, val in enumerate(display_df.columns):
                                fmt = header_own_format if idx % 2 == 0 else header_comp_format
                                ws.write(header_row, idx, val, fmt)
                                
                            ws.set_column('A:F', 18) 
                            display_df_safe = display_df.fillna('')
                            
                            for r_idx, row in display_df_safe.iterrows():
                                row_fmt = row_even_format if r_idx % 2 == 0 else row_odd_format
                                for c_idx, value in enumerate(row):
                                    ws.write(r_idx + data_row, c_idx, value, row_fmt)
                            
                            if len(display_df) > 0:
                                last_row_idx = data_row + len(display_df) 
                                xl_start = data_row + 1       
                                xl_end = last_row_idx + 1     
                                
                                ws.conditional_format(data_row, 0, last_row_idx, 0, {'type': 'formula', 'criteria': f'=COUNTIF($B${xl_start}:$B${xl_end}, A{xl_start})>0', 'format': overlap_fmt_1})
                                ws.conditional_format(data_row, 1, last_row_idx, 1, {'type': 'formula', 'criteria': f'=COUNTIF($A${xl_start}:$A${xl_end}, B{xl_start})>0', 'format': overlap_fmt_1})
                                ws.conditional_format(data_row, 2, last_row_idx, 2, {'type': 'formula', 'criteria': f'=COUNTIF($D${xl_start}:$D${xl_end}, C{xl_start})>0', 'format': overlap_fmt_2})
                                ws.conditional_format(data_row, 3, last_row_idx, 3, {'type': 'formula', 'criteria': f'=COUNTIF($C${xl_start}:$C${xl_end}, D{xl_start})>0', 'format': overlap_fmt_2})
                                ws.conditional_format(data_row, 4, last_row_idx, 4, {'type': 'formula', 'criteria': f'=COUNTIF($F${xl_start}:$F${xl_end}, E{xl_start})>0', 'format': overlap_fmt_3})
                                ws.conditional_format(data_row, 5, last_row_idx, 5, {'type': 'formula', 'criteria': f'=COUNTIF($E${xl_start}:$E${xl_end}, F{xl_start})>0', 'format': overlap_fmt_3})

                            score_own = f"  Own ASIN   |   High Conv: {len(d1)}    |    High ATC: {len(d3)}    |    High Pot: {len(d5)}  "
                            score_comp_display = f"  {comp_score_name}  |   High Conv: {len(d2)}    |    High ATC: {len(d4)}    |    High Pot: {len(d6)}  "
                            
                            if report_type == 'Internal':
                                ws.merge_range('H2:N2', score_own, score_format)
                                ws.merge_range('H3:N3', score_comp_display, score_format)
                                
                                hidden_sheet_name = f"Z_ChartData_{safe_sheet_name[:15]}_{p_idx}"
                                ws_hidden = writer.book.add_worksheet(hidden_sheet_name)
                                ws_hidden.hide()
                                
                                chart_layout = [
                                    (d1, "Own - HCR", "H6"),  (d2, f"{comp_short} - HCR", "P6"),
                                    (d3, "Own - ATC", "H23"), (d4, f"{comp_short} - ATC", "P23"),
                                    (d5, "Own - Pot", "H40"), (d6, f"{comp_short} - Pot", "P40")
                                ]
                                
                                data_write_col = 0 
                                
                                for data_df, base_title, insert_pos in chart_layout:
                                    if len(data_df) < 2: 
                                        continue 
                                    
                                    df_purch = data_df[data_df['Num Purchases'] > 0]
                                    df_atc = data_df[data_df['Num AddToCart'] > 0]
                                    
                                    if len(df_purch) >= 2:
                                        top10 = df_purch.head(10)
                                        metric_col = 'Num Purchases'
                                        chart_title = f"{base_title} (Purchases vs Share)"
                                    elif len(df_atc) >= 2:
                                        top10 = df_atc.head(10)
                                        metric_col = 'Num AddToCart'
                                        chart_title = f"{base_title} (Add To Cart vs Share)"
                                    else:
                                        continue
                                    
                                    c_base = data_write_col
                                    ws_hidden.write(0, c_base, "Keyword")
                                    ws_hidden.write(0, c_base+1, metric_col)
                                    ws_hidden.write(0, c_base+2, "Share %")
                                    
                                    for i, row in top10.iterrows():
                                        k = row['keyword'] if pd.notna(row['keyword']) else ""
                                        p = row[metric_col] if pd.notna(row[metric_col]) else 0
                                        s = row['SEARCH SHARE'] if pd.notna(row['SEARCH SHARE']) else 0
                                        ws_hidden.write(i+1, c_base, k)
                                        ws_hidden.write(i+1, c_base+1, p)
                                        ws_hidden.write(i+1, c_base+2, s)
                                    
                                    chart_col = workbook.add_chart({'type': 'column'})
                                    chart_col.add_series({
                                        'name': metric_col.replace('Num ', ''),
                                        'categories': [hidden_sheet_name, 1, c_base, len(top10), c_base],
                                        'values':     [hidden_sheet_name, 1, c_base+1, len(top10), c_base+1],
                                        'fill':       {'color': '#4472C4'}
                                    })
                                    
                                    chart_line = workbook.add_chart({'type': 'line'})
                                    chart_line.add_series({
                                        'name': 'Search Share',
                                        'categories': [hidden_sheet_name, 1, c_base, len(top10), c_base],
                                        'values':     [hidden_sheet_name, 1, c_base+2, len(top10), c_base+2],
                                        'y2_axis':    True,
                                        'line':       {'color': '#ED7D31', 'width': 2.5},
                                        'marker':     {'type': 'circle', 'size': 5}
                                    })
                                    
                                    chart_col.combine(chart_line)
                                    chart_col.set_title({'name': chart_title, 'name_font': {'name': font_name, 'size': 10, 'bold': True}})
                                    chart_col.set_legend({'position': 'top'})
                                    chart_col.set_x_axis({'name': 'Keywords'})
                                    chart_col.set_y_axis({'name': 'Volume'})
                                    chart_line.set_y2_axis({'name': 'Share (%)', 'num_format': '0.00%'})
                                    chart_col.set_size({'width': 500, 'height': 280}) 
                                    ws.insert_chart(insert_pos, chart_col)
                                    data_write_col += 4
                    print(f"    - Report generated: {output_filename}")

            # [New] Delete Source File after processing the country
            if target_data_file:
                try:
                    os.remove(target_full_path)
                    print(f"[Cleanup] Deleted source file: {target_full_path}")
                except Exception as clean_e:
                    print(f"[Warning] Failed to delete {target_full_path}: {clean_e}")
                            
        except Exception as e:
            print(f"[Error] Failed to process {country}: {e}")
            traceback.print_exc()
        
        finally:
            if conn:
                conn.close()

    print("\n[Analysis Module] All tasks completed.")

if __name__ == "__main__":
    main()
    time.sleep(0.5)