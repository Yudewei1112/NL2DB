# 数据库文件处理说明

## 📋 概述

本文档详细说明了NL2DB项目中数据库文件的处理机制，特别是当`database.db`不存在时系统的行为，以及如何安全地将项目上传到GitHub。

## 🗄️ 数据库文件说明

### 主要文件

| 文件名 | 用途 | 是否上传Git | 说明 |
|--------|------|-------------|------|
| `database.db` | 统一数据库文件 | ❌ 否 | 包含处理后的Excel数据 |
| `file_registry.json` | 文件注册表 | ❌ 否 | 记录文件哈希和状态信息 |
| `uploads/*.xlsx` | 用户Excel文件 | ❌ 否 | 用户的原始数据文件 |
| `.env` | 环境变量配置 | ❌ 否 | 包含API密钥等敏感信息 |

### 自动生成文件

以下文件会在系统运行时自动生成，**不需要手动创建**：

- `database.db` - 统一数据库文件
- `file_registry.json` - 文件状态注册表
- `cache/` 目录下的缓存文件
- `Faiss/` 目录下的向量索引文件

## 🚀 首次运行流程

### 当`database.db`不存在时

系统会按以下步骤自动处理：

1. **数据库初始化**
   ```python
   # DatabaseManager.__init__() 会调用
   self._init_database()
   ```
   - 创建新的SQLite数据库文件
   - 创建必要的元数据表（`file_versions`, `table_mappings`）

2. **目录扫描**
   ```python
   # 扫描uploads目录
   db_manager.check_all_files("uploads")
   ```
   - 自动发现所有`.xlsx`和`.xls`文件
   - 计算每个文件的MD5哈希值

3. **数据导入**
   - 逐个处理Excel文件的每个工作表
   - 应用智能列名映射和清理
   - 将数据导入到对应的数据库表中
   - 记录文件状态到注册表

4. **索引建立**
   - 创建必要的数据库索引
   - 生成向量索引（如果启用）

### 示例输出

```
🗄️ 初始化数据库...
✅ 数据库初始化成功
📊 发现 3 个Excel文件，开始处理...
📄 处理文件: 价格表.xlsx
  📋 工作表: 产品价格 -> table_价格表_产品价格
  📋 工作表: 服务价格 -> table_价格表_服务价格
📄 处理文件: 库存表.xlsx
  📋 工作表: 当前库存 -> table_库存表_当前库存
✅ Excel文件处理完成
```

## 🔄 增量更新机制

### 文件变化检测

系统使用MD5哈希来检测文件变化：

```python
def get_file_hash(self, file_path: str) -> str:
    """计算文件的MD5哈希值"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
```

### 更新策略

- **文件未变化**: 跳过处理，直接使用缓存
- **文件已变化**: 重新处理并更新数据库
- **新增文件**: 自动检测并导入
- **删除文件**: 清理相关的数据库表

## 🔒 GitHub上传准备

### 1. 检查.gitignore文件

确保以下文件已被忽略：

```gitignore
# 数据库文件
database.db
file_registry.json

# 用户数据
uploads/*.xlsx
uploads/*.xls

# 环境配置
.env

# 缓存文件
cache/
__pycache__/
Faiss/
```

### 2. 清理敏感数据

运行清理脚本：

```bash
python cleanup_old_cache.py
```

### 3. 验证文件状态

检查哪些文件会被上传：

```bash
git status
git add .
git status  # 再次检查
```

### 4. 安全检查清单

- [ ] `database.db` 不在Git跟踪中
- [ ] `file_registry.json` 不在Git跟踪中
- [ ] `.env` 文件不在Git跟踪中
- [ ] `uploads/` 中的Excel文件不在Git跟踪中
- [ ] 已创建 `.env.example` 作为配置模板
- [ ] README.md 包含完整的设置说明

## 🛠️ 故障排除

### 数据库损坏

如果数据库文件损坏：

```bash
# 删除损坏的数据库
rm database.db file_registry.json

# 重新初始化
python init_system.py
```

### 文件注册表不一致

如果文件注册表与实际文件不一致：

```bash
# 删除注册表，强制重新扫描
rm file_registry.json

# 重新运行系统
python NL2DB.py
```

### 权限问题

确保程序有权限读写以下目录：

- 当前工作目录（用于创建database.db）
- `uploads/` 目录
- `cache/` 目录

## 📝 最佳实践

### 开发环境

1. **本地开发**
   - 使用真实的Excel文件进行测试
   - 定期备份重要的数据库文件
   - 使用不同的API密钥进行开发和生产

2. **版本控制**
   - 只提交代码和配置模板
   - 使用分支进行功能开发
   - 在README中详细说明设置步骤

### 生产部署

1. **环境隔离**
   - 使用独立的数据库文件
   - 配置专用的API密钥
   - 设置适当的文件权限

2. **数据备份**
   - 定期备份database.db
   - 备份file_registry.json
   - 监控磁盘空间使用

## 🔗 相关文档

- [README.md](README.md) - 项目总体说明
# - [enhanced_column_mapping_usage.md](enhanced_column_mapping_usage.md) - 列名映射说明（已删除）
- [NL2DB流程.md](NL2DB流程.md) - 系统流程说明