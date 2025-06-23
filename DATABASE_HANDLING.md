# 数据库文件处理机制

本文档详细说明了 NL2DB 项目中数据库文件的处理机制，包括首次运行、增量更新、文件变化检测、表映射管理等核心功能。

## 概述

NL2DB 使用 SQLite 数据库存储从 Excel 文件中提取的数据。系统具备智能的文件变化检测和增量更新机制，支持普通表映射和增强型表映射，确保数据库始终与源文件保持同步。

## 核心组件

### DatabaseManager 类

`DatabaseManager` 是数据库操作的核心类，负责：

- 数据库初始化和连接管理
- 文件注册表的维护（`file_versions` 表）
- 文件变化检测（基于 SHA-256 哈希）
- 增量数据更新
- 表映射关系管理（`table_mappings` 和 `enhanced_table_mappings` 表）
- 孤立表清理
- 数据库信息统计

## 🗄️ 数据库文件说明

### 主要文件

| 文件名 | 用途 | 是否上传Git | 说明 |
|--------|------|-------------|------|
| `database.db` | 统一数据库文件 | ❌ 否 | 包含处理后的Excel数据 |
| `file_registry.json` | 文件注册表 | ❌ 否 | 记录文件哈希和状态信息 |
| `uploads/*.xlsx` | 用户Excel文件 | ❌ 否 | 用户的原始数据文件 |
| `.env` | 环境变量配置 | ❌ 否 | 包含API密钥等敏感信息 |

### 元数据表结构

系统维护三个核心元数据表：

#### 1. 文件版本表 (file_versions)

```sql
CREATE TABLE file_versions (
    file_path TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL,
    last_modified TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

字段说明：
- `file_path`: 文件的绝对路径
- `file_hash`: 文件内容的 SHA-256 哈希值
- `last_modified`: 文件最后修改时间
- `created_at`: 记录创建时间

#### 2. 表映射表 (table_mappings)

```sql
CREATE TABLE table_mappings (
    table_name TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    sheet_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

记录数据表与源文件的映射关系。

#### 3. 增强型表映射表 (enhanced_table_mappings)

```sql
CREATE TABLE enhanced_table_mappings (
    table_name TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    sheet_name TEXT,
    original_name TEXT,
    row_count INTEGER,
    column_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

提供更详细的表信息，包括原始名称、行列数等统计信息。

### 自动生成文件

以下文件会在系统运行时自动生成，**不需要手动创建**：

- `database.db` - 统一数据库文件
- `file_registry.json` - 文件状态注册表
- `cache/` 目录下的缓存文件
- `Faiss/` 目录下的向量索引文件

## 工作流程

### 1. 首次运行

当系统首次运行时：

1. **数据库初始化**：创建 SQLite 数据库文件
2. **创建元数据表**：建立 `file_versions`、`table_mappings` 和 `enhanced_table_mappings` 表
3. **扫描 Excel 文件**：遍历指定目录下的所有 `.xlsx` 和 `.xls` 文件
4. **数据导入**：将每个工作表的数据导入到对应的数据库表中
5. **记录文件信息**：在注册表中记录每个文件的哈希值和修改时间
6. **建立映射关系**：在映射表中记录表与源文件的对应关系

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

### 2. 增量更新机制

系统支持智能的增量更新：

#### 文件变化检测

- **哈希比较**：计算文件的 SHA-256 哈希值，与注册表中的记录比较
- **修改时间检查**：作为辅助验证手段
- **新文件识别**：检测新增的 Excel 文件
- **孤立表检测**：识别源文件已删除但数据表仍存在的情况

#### 更新策略

1. **文件未变化**：跳过处理，提高效率
2. **文件已修改**：
   - 删除旧的数据表
   - 重新导入数据
   - 更新文件注册表和映射表
3. **新增文件**：
   - 导入新文件数据
   - 添加到文件注册表和映射表
4. **文件删除**：
   - 清理孤立的数据表
   - 从注册表和映射表中移除记录

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

## 核心功能

### 文件哈希计算

```python
def _calculate_file_hash(self, file_path: str) -> str:
    """计算文件的 SHA-256 哈希值"""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()
```

### 文件变化检测

```python
def _has_file_changed(self, file_path: str) -> bool:
    """检查文件是否发生变化"""
    current_hash = self._calculate_file_hash(file_path)
    
    cursor = self.conn.cursor()
    cursor.execute(
        "SELECT file_hash FROM file_versions WHERE file_path = ?",
        (file_path,)
    )
    result = cursor.fetchone()
    
    if result is None:
        return True  # 新文件
    
    return current_hash != result[0]
```

### 表映射管理

系统提供两种类型的表映射：

#### 普通表映射

```python
def get_table_mappings(self) -> Dict[str, Dict[str, str]]:
    """获取所有表的映射关系"""
    cursor = self.conn.cursor()
    cursor.execute("SELECT table_name, file_path, sheet_name FROM table_mappings")
    
    mappings = {}
    for row in cursor.fetchall():
        table_name, file_path, sheet_name = row
        mappings[table_name] = {
            "file_path": file_path,
            "sheet_name": sheet_name
        }
    
    return mappings
```

#### 增强型表映射

```python
def get_enhanced_table_mappings(self) -> Dict[str, Dict[str, Any]]:
    """获取增强型表映射信息"""
    cursor = self.conn.cursor()
    cursor.execute("""
        SELECT table_name, file_path, sheet_name, original_name, 
               row_count, column_count, created_at, updated_at
        FROM enhanced_table_mappings
    """)
    
    mappings = {}
    for row in cursor.fetchall():
        table_name, file_path, sheet_name, original_name, row_count, column_count, created_at, updated_at = row
        mappings[table_name] = {
            "file_path": file_path,
            "sheet_name": sheet_name,
            "original_name": original_name,
            "row_count": row_count,
            "column_count": column_count,
            "created_at": created_at,
            "updated_at": updated_at
        }
    
    return mappings
```

### 孤立表清理

```python
def cleanup_orphaned_tables(self) -> int:
    """清理孤立的表（源文件已删除但表仍存在）"""
    orphaned_count = 0
    
    # 获取所有映射的文件路径
    cursor = self.conn.cursor()
    cursor.execute("SELECT DISTINCT file_path FROM table_mappings")
    mapped_files = [row[0] for row in cursor.fetchall()]
    
    # 检查文件是否仍然存在
    for file_path in mapped_files:
        if not os.path.exists(file_path):
            # 文件已删除，清理相关表
            orphaned_count += self._cleanup_tables_for_file(file_path)
    
    return orphaned_count
```

## 性能优化

### 1. 批量操作

- 使用事务处理批量插入操作
- 减少数据库连接开销
- 批量更新映射表信息

### 2. 智能跳过

- 文件未变化时跳过处理
- 避免重复的数据导入操作
- 增量检测机制减少不必要的扫描

### 3. 内存管理

- 分块读取大文件
- 及时释放数据库连接
- 优化哈希计算的内存使用

### 4. 数据库优化

- 为关键字段创建索引
- 定期清理孤立表
- 压缩数据库文件

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

## 故障排除

### 常见问题

1. **数据库锁定**
   - 确保没有其他程序占用数据库文件
   - 检查文件权限设置
   - 使用 `get_database_info()` 检查数据库状态

2. **文件读取失败**
   - 验证 Excel 文件格式是否正确
   - 检查文件是否被其他程序打开
   - 查看文件注册表中的错误记录

3. **哈希计算错误**
   - 确保文件完整性
   - 检查磁盘空间是否充足
   - 验证文件权限

4. **映射表不一致**
   - 运行 `cleanup_orphaned_tables()` 清理孤立表
   - 检查 `table_mappings` 和 `enhanced_table_mappings` 的一致性
   - 重新扫描所有文件以重建映射

### 调试技巧

- 启用详细日志输出
- 使用 `get_database_info()` 获取数据库统计信息
- 检查文件注册表和映射表的内容
- 手动验证文件哈希值
- 使用 `get_enhanced_table_mappings()` 查看详细的表信息

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

## API 参考

### 主要方法

#### 核心功能
- `__init__(db_path, excel_dir)`: 初始化数据库管理器
- `update_database()`: 更新数据库（增量）
- `check_and_update_all_files()`: 检查并更新所有 Excel 文件

#### 映射管理
- `get_table_mappings()`: 获取普通表映射关系
- `get_enhanced_table_mappings()`: 获取增强型表映射关系

#### 文件管理
- `_has_file_changed(file_path)`: 检查文件是否变化
- `_calculate_file_hash(file_path)`: 计算文件哈希
- `_update_file_version(file_path, file_hash)`: 更新文件版本信息

#### 数据库维护
- `get_database_info()`: 获取数据库统计信息
- `cleanup_orphaned_tables()`: 清理孤立表

#### 全局函数
- `get_database_manager(db_path, excel_dir)`: 获取数据库管理器单例

### 使用示例

```python
from database_manager import get_database_manager

# 获取数据库管理器单例
db_manager = get_database_manager("data.db", "excel_files")

# 更新数据库
db_manager.update_database()

# 获取数据库信息
info = db_manager.get_database_info()
print(f"总表数: {info['total_tables']}")
print(f"总记录数: {info['total_records']}")

# 获取增强型表映射
enhanced_mappings = db_manager.get_enhanced_table_mappings()
for table_name, mapping in enhanced_mappings.items():
    print(f"表 {table_name}: {mapping['row_count']} 行, {mapping['column_count']} 列")

# 清理孤立表
orphaned_count = db_manager.cleanup_orphaned_tables()
print(f"清理了 {orphaned_count} 个孤立表")
```

## 🔗 相关文档

- [README.md](README.md) - 项目总体说明
- [COLUMN_MAPPING_README.md] - 列名映射说明
- [NL2DB流程.md](NL2DB流程.md) - 系统流程说明