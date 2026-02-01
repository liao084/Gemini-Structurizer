"""
数据预处理模块
"""
import re
from typing import List


def normalize_separator(text: str) -> str:
    """
    标准化分隔符：将各种横线变体统一替换为短横线 '-'
    
    Args:
        text: 原始账户字符串
        
    Returns:
        标准化后的字符串
    """
    # 各种横线变体：一字线、破折号、全角减号等
    separator_variants = [
        '—',   # 一字线 (U+2014)
        '–',   # 半角破折号 (U+2013)
        '―',   # 水平线 (U+2015)
        '－',  # 全角减号 (U+FF0D)
        '‐',   # 连字符 (U+2010)
        '‑',   # 不间断连字符 (U+2011)
        '⁃',   # 项目符号 (U+2043)
    ]
    
    result = text
    for variant in separator_variants:
        result = result.replace(variant, '-')
    
    # 清理多余空格
    result = re.sub(r'\s+', ' ', result).strip()
    
    return result


def preprocess_accounts(accounts: List[str]) -> List[str]:
    """
    批量预处理账户字符串
    
    Args:
        accounts: 原始账户字符串列表
        
    Returns:
        预处理后的字符串列表
    """
    return [normalize_separator(acc) for acc in accounts]
