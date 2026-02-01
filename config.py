# ============ 配置文件 ============
# 直接编辑此文件来修改配置

import os
from dotenv import load_dotenv

# 加载 .env 文件中的 API Key
load_dotenv()

# Gemini API 配置
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-flash-lite-latest"

# 并发配置
MAX_CONCURRENT_REQUESTS = 20  # 最大并发请求数
BATCH_SIZE = 20               # 每批发送给 API 的数量

# 重试配置
MAX_RETRIES = 5               # 最大重试次数
RETRY_DELAY = 2.0             # 重试间隔（秒）
