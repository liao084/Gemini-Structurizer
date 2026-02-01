# 账户数据批量清洗工具

基于 Gemini AI 的短剧投流账户数据清洗工具，将非结构化的账户字符串解析为结构化数据。

## 功能

- 读取 Excel 文件中的账户数据
- 使用 Gemini API 智能解析账户字段
- 输出结构化的 Excel 结果文件
- 支持并发处理，提高效率

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 API Key

编辑 `.env` 文件，填入你的 Gemini API Key：

```env
GEMINI_API_KEY=你的API密钥
```

### 3. 调整配置（可选）

编辑 `config.py` 修改配置：

```python
GEMINI_MODEL = "gemini-flash-lite-latest"  # 模型
MAX_CONCURRENT_REQUESTS = 20               # 并发数
BATCH_SIZE = 20                            # 每批数量
```

### 4. 运行

```bash
python batch_clean.py
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `batch_clean.py` | 主脚本，运行入口 |
| `config.py` | 配置文件 |
| `gemini_service.py` | Gemini API 服务 |
| `preprocessor.py` | 数据预处理 |
| `.env` | API Key（敏感信息） |

## 输出格式

清洗后的数据包含以下字段：

- 原始账户名
- 分销自产
- 上剧日期
- 名称
- 盈利方式
- 投流人
- 类型
- 主体
