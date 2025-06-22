# 列名映射器 (Column Mapping Generator)

## 📋 概述

列名映射器是一个智能化的数据库列名与业务含义映射配置生成工具，能够自动分析数据库表结构，利用大语言模型生成准确的列名映射关系，并支持自动创建和增量更新功能。

## 🚀 核心特性

### ✨ 自动化管理
- **自动创建**: 首次运行时自动创建映射注册表和配置目录
- **增量更新**: 启动时自动检测新增表并生成映射配置
- **智能跳过**: 已存在配置时自动跳过，避免重复处理
- **批量控制**: 支持配置批次大小，避免一次处理过多表

### 🎯 智能映射
- **语义分析**: 基于大语言模型的智能列名语义理解
- **上下文感知**: 结合表名、列名、数据类型和样本数据进行分析
- **业务导向**: 生成面向业务用户的可读性强的列名映射

### 🔧 灵活配置
- **配置文件驱动**: 支持通过JSON配置文件控制行为
- **排除列表**: 可配置需要排除的系统表
- **自定义目录**: 支持自定义映射配置保存目录

## 📁 文件结构

```
NL2DB/
├── column_mapping_generator.py      # 核心映射生成器
├── column_mapping_config.json       # 配置文件
└── column_mapping_docs/             # 映射配置目录
    ├── mapping_registry.json        # 映射注册表
    └── table_*_column_mapping.json  # 各表的映射配置
```

## ⚙️ 配置文件说明

### `column_mapping_config.json`

```json
{
  "auto_generate_on_startup": true,     // 启动时自动生成新表映射
  "max_tables_per_batch": 50,            // 每批次最大处理表数
  "enable_incremental_updates": true,   // 启用增量更新检查
  "log_level": "info",                  // 日志级别
  "mapping_directory": "column_mapping_docs",  // 映射配置目录
  "excluded_tables": [                  // 排除的表名列表
    "sqlite_sequence",
    "file_versions",
    "table_mappings",
    "enhanced_table_mappings"
  ],
  "llm_settings": {                     // 大模型设置
    "max_retries": 3,
    "timeout_seconds": 30
  }
}
```

## 🚀 快速开始

### 1. 首次使用（自动创建）

```bash
# 系统会自动检查并创建必要的配置文件和目录
python -c "from column_mapping_generator import get_column_mapping_generator; get_column_mapping_generator()"
```

### 2. 查看系统状态

```python
# 查看映射配置状态和覆盖率
from column_mapping_generator import get_column_mapping_generator
generator = get_column_mapping_generator()
status = generator.get_mapping_status()
print(f"映射覆盖率: {status['mapping_coverage']:.1f}%")
```

### 3. 执行增量更新

```python
# 检查并为新增表生成映射
from column_mapping_generator import get_column_mapping_generator
generator = get_column_mapping_generator()
generator._check_incremental_updates()
```

### 4. 为所有表生成映射

```python
# 为所有未映射的表生成配置
import asyncio
from column_mapping_generator import get_column_mapping_generator
generator = get_column_mapping_generator()
asyncio.run(generator.generate_mappings_for_all_tables())
```

### 5. 为指定表生成映射

```python
# 为特定表生成映射配置
import asyncio
from column_mapping_generator import get_column_mapping_generator
generator = get_column_mapping_generator()
asyncio.run(generator.generate_mapping_for_table("表名"))
```



## 💻 编程接口

### 基础使用

```python
from column_mapping_generator import get_column_mapping_generator

# 获取映射生成器实例（单例模式）
generator = get_column_mapping_generator()

# 查看系统状态
status = generator.get_mapping_status()
print(f"映射覆盖率: {status['mapping_coverage']:.1f}%")

# 为指定表生成映射
import asyncio
asyncio.run(generator.generate_mapping_for_table("表名"))

# 获取映射配置
mapping = generator.get_mapping("表名")
if mapping:
    print("列名映射:", mapping)
```

### 高级使用

```python
# 自定义配置
generator = ColumnMappingGenerator(
    mapping_dir="custom_mappings",
    config_file="custom_config.json"
)

# 批量生成映射
import asyncio
asyncio.run(generator.generate_mappings_for_all_tables())

# 列出所有映射
mappings = generator.list_mappings()
for table_name, info in mappings.items():
    print(f"表: {table_name}, 生成时间: {info['generated_at']}")
```

## 📄 配置文件格式

### 映射注册表 (`mapping_registry.json`)

```json
{
  "表名": {
    "config_file": "配置文件名.json",
    "config_path": "完整文件路径",
    "generated_at": "2025-06-22T12:00:00.000000"
  }
}
```

### 列名映射配置 (`table_*_column_mapping.json`)

```json
{
  "table_name": "表名",
  "generated_at": "2025-06-22T12:00:00.000000",
  "column_mappings": {
    "原列名1": "业务含义1",
    "原列名2": "业务含义2",
    "Unnamed: 0": "序号",
    "复杂列名": "简化的业务描述"
  }
}
```

## 🔄 工作流程

### 启动时自动检查流程

1. **初始化检查**
   - 检查映射目录是否存在，不存在则创建
   - 检查注册表文件是否存在，不存在则创建空文件
   - 加载配置文件，不存在则使用默认配置

2. **增量更新检查**
   - 获取数据库中所有用户表
   - 对比注册表，找出未映射的新表
   - 根据配置决定是否自动生成映射

3. **自动生成**
   - 按批次大小限制处理表数量
   - 为每个新表生成列名映射配置
   - 更新注册表记录

### 映射生成流程

1. **数据收集**
   - 获取表结构信息（列名、数据类型）
   - 提取样本数据（前10行）
   - 分析表名和列名特征

2. **智能分析**
   - 构建大模型提示词
   - 调用大语言模型进行语义分析
   - 解析返回的映射关系

3. **配置保存**
   - 生成标准化的配置文件
   - 更新映射注册表
   - 验证配置文件完整性

## 🧪 测试验证

### 测试覆盖范围

- ✅ 自动创建功能
- ✅ 增量更新检查
- ✅ 配置文件加载
- ✅ 映射生成和保存
- ✅ 状态查询功能
- ✅ 错误处理机制

## 📦 依赖要求

```
pandas>=1.3.0
langchain-core>=0.1.0
sqlite3 (Python内置)
json (Python内置)
os (Python内置)
re (Python内置)
hashlib (Python内置)
```

## ⚠️ 注意事项

### 性能考虑
- 大模型调用有网络延迟，建议合理设置批次大小
- 大表的样本数据提取可能较慢，已限制为前10行
- 配置文件采用JSON格式，便于人工查看和修改

### 安全考虑
- 样本数据可能包含敏感信息，注意配置文件的访问权限
- 大模型API调用需要网络连接，确保网络安全
- 配置文件建议纳入版本控制管理

### 兼容性
- 支持Windows、Linux、macOS操作系统
- 兼容Python 3.7+版本
- 支持SQLite数据库格式

## 🔧 故障排除

### 常见问题

**Q: 启动时提示"无法在当前事件循环中运行生成任务"**
A: 这是异步调用冲突，可以通过以下方式解决：
```python
# 方法1: 在配置文件中禁用自动生成
{"auto_generate_on_startup": false}

# 方法2: 手动运行增量更新
from column_mapping_generator import get_column_mapping_generator
generator = get_column_mapping_generator()
generator._check_incremental_updates()
```

**Q: 映射生成失败或质量不佳**
A: 检查以下几点：
- 确保大模型API可正常访问
- 检查表中是否有足够的样本数据
- 尝试手动为单个表生成映射进行调试

**Q: 配置文件损坏或格式错误**
A: 删除对应配置文件，系统会自动重新生成：
```bash
# 删除注册表，重新初始化
rm column_mapping_docs/mapping_registry.json

# 删除特定表的配置
rm column_mapping_docs/table_*_column_mapping.json
```

### 调试模式

```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 查看详细状态
generator = get_column_mapping_generator()
status = generator.get_mapping_status()
print(json.dumps(status, indent=2, ensure_ascii=False))
```

## 🔮 未来规划

- [ ] 支持多种数据库类型（MySQL、PostgreSQL等）
- [ ] 添加映射质量评估和优化建议
- [ ] 支持自定义映射模板和规则
- [ ] 集成Web界面进行可视化管理
- [ ] 支持映射配置的版本控制和回滚
- [ ] 添加映射配置的导入导出功能

---

## 📞 技术支持

如有问题或建议，请通过以下方式联系：
- 查看日志文件获取详细错误信息
- 运行测试脚本验证系统状态
- 检查配置文件格式和内容

**版本**: v2.0.0  
**更新时间**: 2025-06-22  
**主要特性**: 自动创建、增量更新、配置驱动