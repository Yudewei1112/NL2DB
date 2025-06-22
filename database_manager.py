import os
import sqlite3
import pandas as pd
import hashlib
import json
from typing import Dict, List, Tuple, Optional
from datetime import datetime

class DatabaseManager:
    """数据库管理器 - 基于增量更新策略"""
    
    def __init__(self, db_path: str = "database.db", registry_file: str = "file_registry.json"):
        """
        初始化数据库管理器
        
        Args:
            db_path: 数据库文件路径
            registry_file: 文件注册表路径，用于记录文件状态
        """
        self.db_path = db_path
        self.registry_file = registry_file
        self.file_registry = self._load_file_registry()
        self._init_database()
    
    def _load_file_registry(self) -> Dict[str, str]:
        """
        加载文件注册表
        
        Returns:
            文件注册表字典，键为文件名，值为文件哈希
        """
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载文件注册表失败: {e}")
        return {}
    
    def _save_file_registry(self):
        """
        保存文件注册表到文件
        """
        try:
            with open(self.registry_file, 'w', encoding='utf-8') as f:
                json.dump(self.file_registry, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 保存文件注册表失败: {e}")
    
    def _init_database(self):
        """
        初始化数据库，创建必要的元数据表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建文件版本表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_versions (
                file_name TEXT PRIMARY KEY,
                file_hash TEXT NOT NULL,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                table_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active'
            )
        """)
        
        # 创建表映射表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS table_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(file_name, sheet_name)
            )
        """)
        
        # 创建增强的表映射表（方案1：包含excel_name的映射结构）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS enhanced_table_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                excel_name TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(excel_name, sheet_name)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def get_file_hash(self, file_path: str) -> str:
        """
        计算文件的MD5哈希值
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件的MD5哈希值
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            print(f"⚠️ 计算文件哈希失败 {file_path}: {e}")
            return ""
    
    def update_if_changed(self, excel_path: str) -> Tuple[bool, Dict[str, str]]:
        """
        检查文件是否发生变化，如有变化则更新数据库
        
        Args:
            excel_path: Excel文件路径
            
        Returns:
            (是否更新了数据库, 表映射字典)
        """
        if not os.path.exists(excel_path):
            print(f"⚠️ 文件不存在: {excel_path}")
            return False, {}
        
        file_key = os.path.basename(excel_path)
        current_hash = self.get_file_hash(excel_path)
        
        if not current_hash:
            return False, {}
        
        # 检查是否需要更新
        stored_hash = self.file_registry.get(file_key)
        if stored_hash == current_hash:
            # 文件未变化，从数据库获取现有映射
            table_mapping = self._get_table_mapping(file_key)
            print(f"📋 文件未变化，使用现有映射: {file_key}")
            return False, table_mapping
        
        # 文件发生变化或首次处理，更新数据库
        print(f"🔄 检测到文件变化，更新数据库: {file_key}")
        table_mapping = self._update_database(excel_path)
        
        if table_mapping:
            # 更新文件注册表
            self.file_registry[file_key] = current_hash
            self._save_file_registry()
            
            # 更新数据库中的文件版本信息
            self._update_file_version(file_key, current_hash, len(table_mapping))
            
            print(f"✅ 数据库更新完成: {file_key}")
            return True, table_mapping
        
        return False, {}
    
    def _update_database(self, excel_path: str) -> Dict[str, str]:
        """
        更新数据库，将Excel文件转换为SQLite表
        
        Args:
            excel_path: Excel文件路径
            
        Returns:
            表映射字典 {工作表名: 数据库表名}
        """
        try:
            excel_file = pd.ExcelFile(excel_path)
            sheet_names = excel_file.sheet_names
            
            conn = sqlite3.connect(self.db_path)
            table_mapping = {}
            file_name = os.path.basename(excel_path)
            
            # 清理旧的表映射记录
            cursor = conn.cursor()
            cursor.execute("DELETE FROM table_mappings WHERE file_name = ?", (file_name,))
            
            for sheet_name in sheet_names:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    
                    # 生成表名
                    excel_base = os.path.splitext(file_name)[0]
                    table_name = f"table_{''.join(filter(str.isalnum, excel_base))}_{''.join(filter(str.isalnum, sheet_name))}"
                    
                    # 删除旧表（如果存在）
                    cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
                    
                    # 创建新表
                    df.to_sql(table_name, conn, if_exists='replace', index=False)
                    table_mapping[sheet_name] = table_name
                    
                    # 记录表映射（原有方式）
                    cursor.execute("""
                        INSERT OR REPLACE INTO table_mappings 
                        (file_name, sheet_name, table_name) 
                        VALUES (?, ?, ?)
                    """, (file_name, sheet_name, table_name))
                    
                    # 记录增强表映射（方案1：包含excel_name）
                    excel_name = os.path.basename(excel_path)
                    cursor.execute("""
                        INSERT OR REPLACE INTO enhanced_table_mappings 
                        (excel_name, sheet_name, table_name, file_path) 
                        VALUES (?, ?, ?, ?)
                    """, (excel_name, sheet_name, table_name, excel_path))
                    
                    print(f"📊 已处理工作表: {sheet_name} -> {table_name}")
                    
                except Exception as e:
                    print(f"⚠️ 处理工作表失败 {sheet_name}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            
            return table_mapping
            
        except Exception as e:
            print(f"❌ 更新数据库失败 {excel_path}: {e}")
            return {}
    
    def get_table_mapping(self, excel_path: str) -> Dict[str, str]:
        """
        获取文件的表映射（公开方法）
        
        Args:
            excel_path: Excel文件路径
            
        Returns:
            表映射字典 {工作表名: 数据库表名}
        """
        file_name = os.path.basename(excel_path)
        return self._get_table_mapping(file_name)
    
    def _get_table_mapping(self, file_name: str) -> Dict[str, str]:
        """
        从数据库获取文件的表映射
        
        Args:
            file_name: 文件名
            
        Returns:
            表映射字典
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT sheet_name, table_name 
                FROM table_mappings 
                WHERE file_name = ?
            """, (file_name,))
            
            results = cursor.fetchall()
            conn.close()
            
            return {sheet_name: table_name for sheet_name, table_name in results}
            
        except Exception as e:
            print(f"⚠️ 获取表映射失败 {file_name}: {e}")
            return {}
    
    def get_enhanced_table_mapping(self, excel_name: str = None) -> Dict[Tuple[str, str], str]:
        """
        获取增强的表映射（方案1：包含excel_name的映射结构）
        
        Args:
            excel_name: Excel文件名，如果为None则返回所有映射
            
        Returns:
            增强映射字典 {(excel_name, sheet_name): table_name}
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if excel_name:
                cursor.execute("""
                    SELECT excel_name, sheet_name, table_name 
                    FROM enhanced_table_mappings 
                    WHERE excel_name = ?
                """, (excel_name,))
            else:
                cursor.execute("""
                    SELECT excel_name, sheet_name, table_name 
                    FROM enhanced_table_mappings
                """)
            
            results = cursor.fetchall()
            conn.close()
            
            return {(excel_name, sheet_name): table_name for excel_name, sheet_name, table_name in results}
            
        except Exception as e:
            print(f"⚠️ 获取增强表映射失败: {e}")
            return {}
    
    def get_table_name_by_excel_sheet(self, excel_name: str, sheet_name: str) -> str:
        """
        根据Excel文件名和Sheet名获取对应的表名（方案1专用方法）
        
        Args:
            excel_name: Excel文件名
            sheet_name: Sheet名称
            
        Returns:
            对应的数据库表名，如果未找到返回None
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name 
                FROM enhanced_table_mappings 
                WHERE excel_name = ? AND sheet_name = ?
            """, (excel_name, sheet_name))
            
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else None
            
        except Exception as e:
            print(f"⚠️ 获取表名失败 {excel_name}-{sheet_name}: {e}")
            return None
    
    def _update_file_version(self, file_name: str, file_hash: str, table_count: int):
        """
        更新文件版本信息
        
        Args:
            file_name: 文件名
            file_hash: 文件哈希
            table_count: 表数量
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO file_versions 
                (file_name, file_hash, last_updated, table_count, status) 
                VALUES (?, ?, CURRENT_TIMESTAMP, ?, 'active')
            """, (file_name, file_hash, table_count))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"⚠️ 更新文件版本失败 {file_name}: {e}")
    
    def check_all_files(self, excel_dir: str) -> Dict[str, Dict[str, str]]:
        """
        启动时检查所有Excel文件并更新数据库
        
        Args:
            excel_dir: Excel文件目录
            
        Returns:
            所有文件的表映射字典 {文件名: {工作表名: 表名}}
        """
        print(f"🔍 开始检查目录中的所有Excel文件: {excel_dir}")
        
        if not os.path.exists(excel_dir):
            print(f"⚠️ 目录不存在: {excel_dir}")
            return {}
        
        all_mappings = {}
        excel_files = [f for f in os.listdir(excel_dir) if f.endswith(('.xlsx', '.xls'))]
        
        if not excel_files:
            print(f"📁 目录中未找到Excel文件: {excel_dir}")
            return {}
        
        print(f"📋 找到 {len(excel_files)} 个Excel文件")
        
        for excel_file in excel_files:
            excel_path = os.path.join(excel_dir, excel_file)
            updated, table_mapping = self.update_if_changed(excel_path)
            
            if table_mapping:
                all_mappings[excel_file] = table_mapping
                status = "更新" if updated else "已存在"
                print(f"✅ {status}: {excel_file} ({len(table_mapping)} 个工作表)")
            else:
                print(f"❌ 处理失败: {excel_file}")
        
        print(f"🎯 文件检查完成，共处理 {len(all_mappings)} 个文件")
        return all_mappings
    
    def get_database_info(self) -> Dict:
        """
        获取数据库信息
        
        Returns:
            数据库统计信息
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取文件统计
            cursor.execute("SELECT COUNT(*) FROM file_versions WHERE status = 'active'")
            active_files = cursor.fetchone()[0]
            
            # 获取表统计
            cursor.execute("SELECT COUNT(*) FROM table_mappings")
            total_tables = cursor.fetchone()[0]
            
            # 获取数据库大小
            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            
            conn.close()
            
            return {
                "database_path": self.db_path,
                "database_size_mb": round(db_size / (1024 * 1024), 2),
                "active_files": active_files,
                "total_tables": total_tables,
                "registry_entries": len(self.file_registry)
            }
            
        except Exception as e:
            print(f"⚠️ 获取数据库信息失败: {e}")
            return {}
    
    def cleanup_orphaned_tables(self):
        """
        清理孤立的表（没有对应文件的表）
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有用户表
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name LIKE 'table_%'
            """)
            all_tables = [row[0] for row in cursor.fetchall()]
            
            # 获取映射中的表
            cursor.execute("SELECT DISTINCT table_name FROM table_mappings")
            mapped_tables = [row[0] for row in cursor.fetchall()]
            
            # 找出孤立的表
            orphaned_tables = set(all_tables) - set(mapped_tables)
            
            if orphaned_tables:
                print(f"🧹 发现 {len(orphaned_tables)} 个孤立表，开始清理...")
                for table_name in orphaned_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
                    print(f"🗑️ 已删除孤立表: {table_name}")
                
                conn.commit()
                print(f"✅ 孤立表清理完成")
            else:
                print(f"✨ 未发现孤立表")
            
            conn.close()
            
        except Exception as e:
            print(f"⚠️ 清理孤立表失败: {e}")

# 全局数据库管理器实例
_db_manager = None

def get_database_manager(db_path: str = "database.db") -> DatabaseManager:
    """
    获取数据库管理器单例
    
    Args:
        db_path: 数据库路径
        
    Returns:
        数据库管理器实例
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(db_path)
    return _db_manager