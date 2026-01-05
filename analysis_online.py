import os
import re
import json
from openai import OpenAI
import pandas as pd
from datetime import datetime
import concurrent.futures

# 配置
VAULT_PATH = "2025" # 日记根目录
MODEL_NAME = "deepseek-chat"
MODEL_NAME_2 = "deepseek-reasoner"
# 建议通过环境变量设置：export DEEPSEEK_API_KEY="您的key"
API_KEY = os.environ.get("DEEPSEEK_API_KEY", "your-api-key-here")
BASE_URL = "https://api.deepseek.com"

client = OpenAI(api_key=API_KEY, base_url=BASE_URL)

# 1. 数据清洗函数
def clean_markdown(text):
    text = re.sub(r'^---[\s\S]*?---', '', text)
    text = re.sub(r'```dataview[\s\S]*?```', '', text)
    return text.strip()

# 辅助：从文件名提取标准化日期
def extract_date(filename):
    clean_name = filename.replace(" ", "")
    match = re.search(r'(\d{4}-\d{2}-\d{2})', clean_name)
    if match:
        return match.group(1)
    return None

# 2. 核心：LLM 提取器 (Map Phase)
def analyze_chunk(date_range, content_chunk):
    prompt = f"""
    你是一个极其敏锐的心理咨询师和传记作家。这是用户在 {date_range} 期间的日记片段。
    
    请分析并以纯 JSON 格式输出以下信息：
    1. "emotion_score": 情绪评分 (-5 到 +5，-5极度痛苦，0平静，+5极度狂喜)。
    2. "key_events": 发生的关键事件列表（请非常具体，包含项目名称、创作的作品名如"制作毕业歌"、具体地名等）。
    3. "main_focus": 用户主要花费精力的事务（如"学习Rust"、"准备马拉松"）。
    4. "highlights": 任何值得记录的人生高光或低谷时刻（包括情感波动、重要反思）。
    5. "weekly_summary": 一段100-200字的本周生活摘要，串联关键事件，捕捉生活细节和氛围。
    6. "travel_experiences": 具体的旅游经历（包括地点、特色体验、具体感受，请保留丰富细节，少概括）。
    7. "artistic_works": 接触的文艺作品（书籍、电影、游戏、音乐等，请列出具体名称和简要评价/感受，少概括多细节）。

    日记内容：
    {content_chunk}
    
    请只输出 JSON，不要包含 Markdown 格式标记。
    """
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{'role': 'user', 'content': prompt}],
            response_format={'type': 'json_object'}
        )
        content = response.choices[0].message.content
        print(f"[{date_range}] LLM Response Length: {len(content)}")
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error for {date_range}: {e}")
            return None
    except Exception as e:
        print(f"Error processing {date_range}: {e}")
        return None

# 3. 中间层：月度总结器 (Compress Phase)
def generate_monthly_summary(month_str, weekly_data_list):
    month_context = ""
    for item in weekly_data_list:
        month_context += f"""
        【时间段: {item['date_range']}】
        摘要: {item.get('weekly_summary', '')}
        关键事件: {item.get('key_events_str', '')}
        重心: {item.get('main_focus_str', '')}
        旅游: {item.get('travel_experiences_str', '')}
        文艺: {item.get('artistic_works_str', '')}
        -----------------------------------
        """
        
    prompt = f"""
    你是用户的生活传记作者。这是用户在 {month_str} 月份的几周日记分析片段。
    请将这些碎片信息整合成一份连贯的【月度总结】。
    
    请以纯 JSON 格式输出：
    1. "month_narrative": 本月叙事主线（150-300字），概括本月的生活状态、核心变化和心路历程。
    2. "key_achievements": 本月完成的关键成就或里程碑（列表）。
    3. "challenges": 本月遇到的主要挑战或低谷（列表）。
    4. "month_vibe": 本月的整体氛围/关键词（如“兵荒马乱”、“宁静致远”）。
    5. "travel_art_summary": 本月在旅游和文艺方面的亮点汇总。

    输入数据：
    {month_context}
    
    请只输出 JSON。
    """
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME_2,
            messages=[{'role': 'user', 'content': prompt}],
            response_format={'type': 'json_object'}
        )
        content = response.choices[0].message.content
        result = json.loads(content)
        result['month'] = month_str
        
        def safe_join(data):
            if isinstance(data, list):
                return "; ".join([str(item) for item in data])
            return str(data) if data else ""
            
        result['key_achievements_str'] = safe_join(result.get('key_achievements', []))
        result['challenges_str'] = safe_join(result.get('challenges', []))
        
        return result
    except Exception as e:
        print(f"Error generating monthly summary for {month_str}: {e}")
        return None

# 4. 归档板块生成器 (Archive Phase)
def generate_archive_section(section_name, raw_data_list, prompt_instruction):
    print(f"正在整理归档板块: {section_name} ...")
    
    # 拼接原始数据，为了避免过长，可以简单加个换行
    full_context = "\n".join([str(item) for item in raw_data_list if item])
    
    # 如果数据量过大，可能需要截断或分批（这里暂假设deepseek 128k能hold住全年纯文本列表）
    # 但为了保险，我们只取前100k字符（约）
    if len(full_context) > 100000:
        full_context = full_context[:100000] + "\n...(部分内容因过长截断)..."

    prompt = f"""
    你是一个专业的个人史料整理员。请基于以下提供的【全年原始记录流】，整理出一份结构清晰、细节丰富的“{section_name}”清单。
    
    **整理要求：**
    1. **去重与合并**：对于重复提到的事件或项目，合并为一条，保留最详细的描述。
    2. **结构化分类**：{prompt_instruction}
    3. **保留细节**：不要只列大纲，要保留具体的地名、书名、人名、情感评价和独特体验。
    4. **时间感**：如果可能，按时间顺序或逻辑顺序排列。
    
    请直接输出整理好的 Markdown 内容，不需要开场白。
    
    【原始记录流】
    {full_context}
    """
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME, # 使用 chat 模型处理长文本整理
            messages=[{'role': 'user', 'content': prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error generating archive section {section_name}: {e}")
        return f"整理 {section_name} 失败"

# 5. 年度总结生成器 (Reduce Phase - Dual Track)
def generate_final_summary(monthly_df, output_dir, weekly_df):
    
    # --- Track 1: 叙事篇 (基于月报) ---
    print("正在生成叙事篇...")
    narrative_context = ""
    for _, row in monthly_df.iterrows():
        narrative_context += f"""
        【{row['month']}】叙事:{row.get('month_narrative','')}| 氛围:{row.get('month_vibe','')}| 成就:{row.get('key_achievements_str','')}| 挑战:{row.get('challenges_str','')}
        -------------------
        """
    
    narrative_prompt = f"""
    你是一位敏锐的人生叙事者。请基于以下【月度叙事流】，撰写一份年度总结的 **第一部分：叙事篇**。
    
    标题：《我的2025：[请提炼年度主题词]》
    
    **写作要求：**
    1. **年度叙事弧光**：用“开篇-发展-高潮-沉淀”的结构，讲述这一年我如何从起点出发，经历波折，最终获得成长。
    2. **深度洞察**：
       - **年度面孔**：为我画一幅自画像（几个关键身份）。
       - **隐秘的旋律**：指出一个贯穿全年的深层行为或思维模式。
       - **致2026年的我**：一句有力的话。
    
    请只输出“第一部分：叙事篇”的内容。
    
    【月度叙事流】
    {narrative_context}
    """
    
    try:
        narrative_response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{'role': 'user', 'content': narrative_prompt}]
        )
        narrative_content = narrative_response.choices[0].message.content
    except Exception as e:
        print(f"生成叙事篇失败: {e}")
        narrative_content = "# 叙事篇生成失败"

    # --- Track 2: 归档篇 (基于周报原始数据，分块并行) ---
    print("正在并行生成归档板块...")
    
    # 准备数据源
    travel_data = weekly_df['travel_experiences_str'].tolist()
    arts_data = weekly_df['artistic_works_str'].tolist()
    
    # 技术与创造：合并 key_events 和 main_focus
    tech_data = weekly_df['key_events_str'].tolist() + weekly_df['main_focus_str'].tolist()
    
    # 个人成长：合并 summary 和 highlights
    growth_data = weekly_df['weekly_summary'].tolist() + weekly_df['highlights'].tolist()
    
    archive_tasks = {
        "行旅与足迹": (travel_data, "请按【城市/地区】分类。列出具体的景点、餐厅、独特体验和当时的感受。"),
        "书影音游": (arts_data, "请按【书籍、电影、动画、游戏、音乐】分类。列出作品名、简评和带来的触动。"),
        "技术与创造": (tech_data, "请按【硬核技术研究】（如内核、AI）和【创造性产出】（如毕业歌、视频）分类。列出具体项目、攻克的技术难点和成果。"),
        "个人成长与生活": (growth_data, "请按【生活里程碑】（如升学、搬家）、【情感与反思】（人际、内耗、和解）、【技能树】（运动、乐器）分类。捕捉内心的变化轨迹。")
    }
    
    archive_contents = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_section = {
            executor.submit(generate_archive_section, name, data, instr): name 
            for name, (data, instr) in archive_tasks.items()
        }
        
        for future in concurrent.futures.as_completed(future_to_section):
            section_name = future_to_section[future]
            try:
                archive_contents[section_name] = future.result()
                print(f"完成归档板块: {section_name}")
            except Exception as e:
                print(f"归档板块 {section_name} 异常: {e}")
                archive_contents[section_name] = "生成失败"

    # --- 拼接最终文档 ---
    final_markdown = f"""
{narrative_content}

## 第二部分：归档篇 —— 岁月留痕的清单

### 1. 👣 行旅与足迹
{archive_contents.get("行旅与足迹", "")}

### 2. 📚 书影音游
{archive_contents.get("书影音游", "")}

### 3. 💻 技术与创造
{archive_contents.get("技术与创造", "")}

### 4. 🌱 个人成长与生活
{archive_contents.get("个人成长与生活", "")}
"""

    print("\n========== 年度总结 ==========\n")
    # print(final_markdown) # 内容太长，不全部打印到控制台
    
    file_path = os.path.join(output_dir, '2025_年度总结_online.md')
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(final_markdown)
    print(f"\n已保存至 {file_path}")
    
    # 评估
    evaluate_summary_framework(final_markdown, "双轨制生成：月度叙事 + 全量周报细节归档", output_dir)

def evaluate_summary_framework(summary_content, old_prompt, output_dir):
    print("正在评估年度总结并生成改进建议...")
    eval_prompt = f"""
    你是一个专业的写作顾问。请评估这篇年度总结（特别是归档部分的细节丰富度）。
    请给出简短的评价和改进建议，并保存为 Markdown。
    """
    try:
        # 截取部分内容进行评估，避免token溢出
        preview_content = summary_content[:10000] + "\n...(后略)"
        response = client.chat.completions.create(
            model=MODEL_NAME_2,
            messages=[{'role': 'user', 'content': eval_prompt + f"\n\n内容预览：\n{preview_content}"}]
        )
        new_prompt_content = response.choices[0].message.content
        file_path = os.path.join(output_dir, 'New_prompt.md')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(new_prompt_content)
        print(f"已生成改进建议，保存至 {file_path}")
    except Exception as e:
        print(f"评估分析时出错: {e}")

# 6. 主流程
def main():
    # 创建输出目录
    report_base = "Diary_report"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(report_base, timestamp)
    os.makedirs(output_dir, exist_ok=True)
    print(f"本次运行输出目录: {output_dir}")

    results = []
    unique_files = {}

    print("正在扫描并去重日记文件...")
    if not os.path.exists(VAULT_PATH):
        print(f"Directory {VAULT_PATH} not found.")
        return

    for month_folder in sorted(os.listdir(VAULT_PATH)):
        month_path = os.path.join(VAULT_PATH, month_folder)
        if os.path.isdir(month_path):
            for file_name in os.listdir(month_path):
                if file_name.endswith('.md'):
                    date_str = extract_date(file_name)
                    if date_str:
                        full_path = os.path.join(month_path, file_name)
                        if date_str not in unique_files:
                            unique_files[date_str] = full_path
                        else:
                            if " " not in file_name and " " in os.path.basename(unique_files[date_str]):
                                unique_files[date_str] = full_path
    
    sorted_dates = sorted(unique_files.keys())
    all_files = [{'date': d, 'path': unique_files[d]} for d in sorted_dates]

    print(f"共找到 {len(all_files)} 篇有效日记（已去重）。开始处理...")

    chunk_size = 7
    tasks = []

    for i in range(0, len(all_files), chunk_size):
        batch_files = all_files[i:i+chunk_size]
        batch_text = ""
        start_date = batch_files[0]['date']
        end_date = batch_files[-1]['date']
        batch_date_str = f"{start_date} 到 {end_date}"
        
        for file_info in batch_files:
            with open(file_info['path'], 'r', encoding='utf-8') as f:
                content = clean_markdown(f.read())
                batch_text += f"【日期: {file_info['date']}】\n{content}\n\n"
        
        tasks.append((batch_date_str, batch_text))

    print(f"共生成 {len(tasks)} 个周分析任务，准备并行处理 (Max Workers: 10)...")

    # 并行执行周分析
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_date = {executor.submit(analyze_chunk, date_str, text): date_str for date_str, text in tasks}
        
        for future in concurrent.futures.as_completed(future_to_date):
            batch_date_str = future_to_date[future]
            try:
                analysis = future.result()
                print(f"完成周分析: {batch_date_str}")
                
                if analysis:
                    analysis['date_range'] = batch_date_str
                    
                    # 辅助函数：安全地将 list 或 string 转换为带分隔符的字符串
                    def safe_join(data):
                        if isinstance(data, list):
                            return "; ".join([str(item) for item in data])
                        return str(data) if data else ""

                    # 保存为 _str 供后续处理和 CSV 导出
                    analysis['key_events_str'] = safe_join(analysis.get('key_events', []))
                    analysis['main_focus_str'] = safe_join(analysis.get('main_focus', []))
                    analysis['travel_experiences_str'] = safe_join(analysis.get('travel_experiences', []))
                    analysis['artistic_works_str'] = safe_join(analysis.get('artistic_works', []))
                    
                    # 同时保留原始 list/obj 数据在内存中，供 generate_monthly_summary 使用（如果需要）
                    # 但为了 CSV 干净，我们在生成 DF 前清理一下或在 DF 生成后筛选列
                        
                    results.append(analysis)
            except Exception as exc:
                print(f"任务 {batch_date_str} 抛出异常: {exc}")

    results.sort(key=lambda x: x['date_range'])

    if not results:
        print("未生成任何分析结果。")
        return

    # 保存周报 CSV (仅保留清洗后的列)
    df_weekly = pd.DataFrame(results)
    # 定义期望的列顺序和名称
    cols_to_keep = [
        'date_range', 'weekly_summary', 'emotion_score', 
        'key_events_str', 'main_focus_str', 'highlights', 
        'travel_experiences_str', 'artistic_works_str'
    ]
    # 确保列存在
    final_cols = [c for c in cols_to_keep if c in df_weekly.columns]
    df_weekly_clean = df_weekly[final_cols]
    
    weekly_csv_path = os.path.join(output_dir, 'diary_analysis_2025_weekly.csv')
    df_weekly_clean.to_csv(weekly_csv_path, index=False, encoding='utf-8-sig')
    print(f"周报数据已保存至 {weekly_csv_path}")
    
    # 月度总结逻辑
    print("正在生成月度总结...")
    monthly_groups = {}
    for item in results:
        start_date = item['date_range'].split(' ')[0]
        month_key = start_date[:7] 
        if month_key not in monthly_groups:
            monthly_groups[month_key] = []
        monthly_groups[month_key].append(item)
    
    monthly_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_month = {
            executor.submit(generate_monthly_summary, month, data): month 
            for month, data in monthly_groups.items()
        }
        for future in concurrent.futures.as_completed(future_to_month):
            month = future_to_month[future]
            try:
                m_summary = future.result()
                if m_summary:
                    print(f"完成月度总结: {month}")
                    monthly_results.append(m_summary)
            except Exception as e:
                print(f"月度总结 {month} 失败: {e}")

    monthly_results.sort(key=lambda x: x['month'])
    
    if monthly_results:
        df_monthly = pd.DataFrame(monthly_results)
        monthly_csv_path = os.path.join(output_dir, 'diary_analysis_2025_monthly.csv')
        # 同样只保留 str 列（generate_monthly_summary 已处理）
        df_monthly.to_csv(monthly_csv_path, index=False, encoding='utf-8-sig')
        print(f"月报数据已保存至 {monthly_csv_path}")
        
        # 生成年度总结 (双轨制：传入 monthly_df 和 weekly_df)
        generate_final_summary(df_monthly, output_dir, df_weekly) # 传入原始 weekly_df 以获取列表数据
    else:
        print("未生成月度数据，无法生成年度总结。")

if __name__ == "__main__":
    main()