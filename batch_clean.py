"""
批量清洗脚本 - 从 Excel 读取账户数据，调用 Gemini API 清洗，输出到新 Excel
"""
import sys
import io

# 修复 Windows 终端编码问题（支持 emoji 和中文）
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import asyncio
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm.asyncio import tqdm_asyncio

import config
from gemini_service import get_gemini_service
from preprocessor import normalize_separator


# ============ 配置区域 ============
INPUT_FILE = r"D:\dev\test-doc\account-cleaner\测试集.xlsx"  # 输入文件路径
INPUT_COLUMN = "A"  # 原始账户所在列（A列 = 第0列）
MAX_ROWS = None  # 最多处理行数（设为 None 处理全部）
# =================================


async def process_batch(gemini_service, batch: list, batch_index: int) -> tuple:
    """处理单个批次，返回 (batch_index, batch, results)"""
    try:
        results = await gemini_service.clean_batch(batch)
        return (batch_index, batch, results, None)
    except Exception as e:
        return (batch_index, batch, None, str(e))


async def main():
    # 记录开始时间
    start_time = time.time()
    
    print("=" * 50)
    print("账户数据批量清洗工具")
    print("=" * 50)
    print(f"\n⚙️  配置: 并发数={config.MAX_CONCURRENT_REQUESTS}, 批次大小={config.BATCH_SIZE}, 模型={config.GEMINI_MODEL}")
    
    # 1. 读取 Excel 文件
    print(f"\n📂 读取文件: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE, header=None)
    
    # 获取 A 列数据（第 0 列）
    col_index = ord(INPUT_COLUMN.upper()) - ord('A')
    accounts = df.iloc[:, col_index].dropna().astype(str).tolist()
    
    # 跳过表头行（如果第一行看起来是表头）
    if accounts and accounts[0] in ["账户", "账户名", "原始账户", "account"]:
        print(f"⚠️  检测到表头行 '{accounts[0]}'，已跳过")
        accounts = accounts[1:]
    
    # 过滤掉明显不是账户数据的行（太短或不包含分隔符）
    original_count = len(accounts)
    accounts = [acc for acc in accounts if len(acc) > 5 and ("-" in acc or "_" in acc)]
    if len(accounts) < original_count:
        print(f"⚠️  过滤掉 {original_count - len(accounts)} 条无效数据")
    
    # 限制行数
    if MAX_ROWS:
        accounts = accounts[:MAX_ROWS]
    
    print(f"📊 共读取 {len(accounts)} 条账户数据")
    
    # 2. 预处理
    print("\n🔧 预处理中...")
    accounts = [normalize_separator(acc) for acc in accounts]
    
    # 3. 调用 Gemini API 批量清洗（asyncio.gather 并发模式）
    print(f"\n🚀 开始调用 Gemini API（批次大小: {config.BATCH_SIZE}，并发数: {config.MAX_CONCURRENT_REQUESTS}）")
    gemini_service = get_gemini_service()
    
    # 分批处理
    batches = [accounts[i:i+config.BATCH_SIZE] for i in range(0, len(accounts), config.BATCH_SIZE)]
    print(f"📦 共 {len(batches)} 个批次")
    
    # 创建所有并发任务
    tasks = [
        process_batch(gemini_service, batch, i)
        for i, batch in enumerate(batches)
    ]
    
    # 使用 asyncio.gather 并发执行，信号量在 gemini_service 中控制并发数
    batch_results = await tqdm_asyncio.gather(*tasks, desc="处理进度")
    
    # 按原始顺序排序结果
    batch_results = sorted(batch_results, key=lambda x: x[0])
    
    # 整理结果
    all_results = []
    failed_accounts = []
    
    for batch_index, batch, results, error in batch_results:
        if error:
            print(f"\n❌ 批次 {batch_index + 1} 处理失败: {error}")
            for acc in batch:
                failed_accounts.append(acc)
                all_results.append({"原始账户名": acc})
        else:
            for acc, result in zip(batch, results):
                if result:
                    result["原始账户名"] = acc
                    all_results.append(result)
                else:
                    failed_accounts.append(acc)
                    all_results.append({"原始账户名": acc})
    
    # 4. 输出到 Excel
    print(f"\n✅ 处理完成！成功: {len(all_results) - len(failed_accounts)}, 失败: {len(failed_accounts)}")
    
    # 创建 DataFrame
    output_df = pd.DataFrame(all_results)
    
    # 调整列顺序
    columns_order = ["原始账户名", "分销自产", "上剧日期", "名称", "盈利方式", "投流人", "类型", "主体"]
    for col in columns_order:
        if col not in output_df.columns:
            output_df[col] = None
    output_df = output_df[columns_order]
    
    # 生成输出文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = Path(INPUT_FILE).parent / f"清洗结果_{timestamp}.xlsx"
    
    output_df.to_excel(output_file, index=False)
    print(f"\n📁 结果已保存到: {output_file}")
    
    # 输出失败列表
    if failed_accounts:
        failed_file = Path(INPUT_FILE).parent / f"失败记录_{timestamp}.txt"
        with open(failed_file, "w", encoding="utf-8") as f:
            f.write("\n".join(failed_accounts))
        print(f"⚠️ 失败记录已保存到: {failed_file}")
    
    # 输出总耗时
    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = total_time % 60
    print(f"\n⏱️  总耗时: {minutes}分{seconds:.1f}秒")
    print(f"📈 处理速度: {len(accounts) / total_time:.1f} 条/秒")


if __name__ == "__main__":
    asyncio.run(main())
