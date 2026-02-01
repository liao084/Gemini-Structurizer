"""
Gemini API 服务模块 - 异步调用 Gemini API 进行账户数据清洗
"""
import json
import asyncio
from typing import List, Optional
from google import genai
from google.genai import types

import config  # 直接导入根目录的 config


# 少样本学习 Prompt 模板
SYSTEM_PROMPT = """你是一个专业的数据清洗助手，专门处理短剧投流账户字段的结构化清洗。

账户字段的标准结构包含以下部分（顺序可能不固定，部分字段可能缺失）：
- 分销自产：分销公司名称或缩写（如 百川、星漫、灵境、漫谭、稀谷、剧点、中文在线）
- 上剧日期：日期格式如 0115、1226、0113 等（4位数字 MMDD）
- 名称：短剧名称
- 盈利方式：只有 iaa 或 iap 两种
- 投流人：姓名或拼音缩写（如 郑菲雨、ztt、mxy、ls、Ai）
- 类型：如 动态漫、沙雕漫、推文小说、漫剧、Ai、动态2D 等
- 主体：推流公司主体，可选字段，常见的有 推啊、兑吧 等，后面可能跟数字编号（如 推啊02、兑吧01、roi4）

特别注意：
1. 价格数字（如 3.9、9.9）不是有效字段，应忽略
2. 【重要】剧名末尾紧跟的单个数字（如"诺亚之影9"中的9）通常是 IAP 付费剧集价格，应从剧名中去除
3. 【重要】类型字段末尾的纯数字序号应去除，如"沙雕漫1"→"沙雕漫"，"动态漫01"→"动态漫"，"沙雕2"→"沙雕"，"AI3"→"AI"
4. 但如果数字是类型名称的一部分则保留，如"动态2D"应保持原样
5. 字段顺序可能混乱，需要根据语义识别
6. 如果某字段无法识别，返回空字符串 "" 而不是 null
7. 盈利方式可能写成 IAA、IAP、iaa、iap 等形式，统一转换为小写
8. 类型字段可能是大小写混合的（如 Ai），保持原样

请将输入的账户字符串解析为 JSON 格式。"""

FEW_SHOT_EXAMPLES = """
示例输入：
["iaa-1226-收徒-剧点-郑菲雨-动态漫-1"]

示例输出：
[
  {"分销自产": "剧点", "上剧日期": "1226", "名称": "收徒", "盈利方式": "iaa", "投流人": "郑菲雨", "类型": "动态漫", "主体": "1"}
]

示例输入：
["漫谭-1230-iaa-诸神之战-林孝翔-动态漫-3", "稀谷-0103-iap-诺亚之影9-ls-Ai-兑吧01-1", "剧点-0101-iap-灵气复苏，从斩杀魔龙开始-3.9-mxy-漫剧-roi4"]

示例输出：
[
  {"分销自产": "漫谭", "上剧日期": "1230", "名称": "诸神之战", "盈利方式": "iaa", "投流人": "林孝翔", "类型": "动态漫", "主体": "3"},
  {"分销自产": "稀谷", "上剧日期": "0103", "名称": "诺亚之影", "盈利方式": "iap", "投流人": "ls", "类型": "Ai", "主体": "兑吧01"},
  {"分销自产": "剧点", "上剧日期": "0101", "名称": "灵气复苏，从斩杀魔龙开始", "盈利方式": "iap", "投流人": "mxy", "类型": "漫剧", "主体": "roi4"}
]

示例输入：
["百川-0113零物资求生：末日极限挑战-iaa-mxy-沙雕漫5", "灵境-日0121-不速之客-iaa-ls-动态漫-推啊04-5"]

示例输出：
[
  {"分销自产": "百川", "上剧日期": "0113", "名称": "零物资求生：末日极限挑战", "盈利方式": "iaa", "投流人": "mxy", "类型": "沙雕漫", "主体": "5"},
  {"分销自产": "灵境", "上剧日期": "0121", "名称": "不速之客", "盈利方式": "iaa", "投流人": "ls", "类型": "动态漫", "主体": "推啊04-5"}
]

示例输入：
["iap-1229-反派系统-9.9-风行-zfy-真人AI", "风行-1229-iaa-重生七零-系数1-ztt-沙雕漫-1"]

示例输出：
[
  {"分销自产": "风行", "上剧日期": "1229", "名称": "反派系统", "盈利方式": "iap", "投流人": "zfy", "类型": "真人AI", "主体": ""},
  {"分销自产": "风行", "上剧日期": "1229", "名称": "重生七零", "盈利方式": "iaa", "投流人": "ztt", "类型": "沙雕漫", "主体": "1"}
]

示例输入：
["风行-0103-iaa-错付三年寿宴重生-ztt-推啊02-10"]

示例输出：
[
  {"分销自产": "风行", "上剧日期": "0103", "名称": "错付三年寿宴重生", "盈利方式": "iaa", "投流人": "ztt", "类型": "", "主体": "推啊02"}
]
"""


class GeminiService:
    """Gemini API 异步服务"""
    
    def __init__(self):
        self.client = genai.Client(api_key=config.GEMINI_API_KEY)
        self.model = config.GEMINI_MODEL
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        self.max_retries = config.MAX_RETRIES
        self.retry_delay = config.RETRY_DELAY
    
    async def clean_batch(self, accounts: List[str]) -> List[Optional[dict]]:
        """
        清洗一批账户数据
        
        Args:
            accounts: 账户字符串列表（已预处理）
            
        Returns:
            清洗结果列表，失败的返回 None
        """
        async with self.semaphore:
            return await self._call_api_with_retry(accounts)
    
    async def _call_api_with_retry(self, accounts: List[str]) -> List[Optional[dict]]:
        """带重试的 API 调用"""
        for attempt in range(self.max_retries):
            try:
                return await self._call_api(accounts)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))
                else:
                    print(f"API 调用失败: {e}")
                    return [None] * len(accounts)
        return [None] * len(accounts)
    
    async def _call_api(self, accounts: List[str]) -> List[Optional[dict]]:
        """调用 Gemini API"""
        prompt = f"""{SYSTEM_PROMPT}

{FEW_SHOT_EXAMPLES}

现在请处理以下账户数据，只返回 JSON 数组，不要有其他内容：
{json.dumps(accounts, ensure_ascii=False)}
"""
        
        # 使用异步生成内容
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,  # 低温度提高一致性
                response_mime_type="application/json",
            )
        )
        
        # 解析 JSON 响应
        result_text = response.text.strip()
        
        # 尝试解析 JSON
        try:
            results = json.loads(result_text)
            if isinstance(results, list) and len(results) == len(accounts):
                return results
            else:
                return [None] * len(accounts)
        except json.JSONDecodeError:
            return [None] * len(accounts)


# 全局服务实例
_gemini_service: Optional[GeminiService] = None


def get_gemini_service() -> GeminiService:
    """获取 Gemini 服务单例"""
    global _gemini_service
    if _gemini_service is None:
        _gemini_service = GeminiService()
    return _gemini_service
