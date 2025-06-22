import sqlite3
import pandas as pd
import json
import os
import re
import hashlib
from typing import Dict, List, Any, Optional, Tuple
from database_manager import get_database_manager
from NL2DB import ModelManager
from langchain_core.messages import HumanMessage

class ColumnMappingGenerator:
    """列名映射生成器 - 生成列名与业务含义的映射配置文件"""
    
    def __init__(self, mapping_dir: str = "column_mapping_docs", config_file: str = "column_mapping_config.json"):
        """
        初始化列名映射生成器
        
        Args:
            mapping_dir: 映射配置文件保存目录
            config_file: 配置文件路径
        """
        self.mapping_dir = mapping_dir
        self.config_file = config_file
        self.db_manager = get_database_manager()
        self.model_manager = ModelManager()
        
        # 加载配置
        self.config = self._load_config()
        
        # 创建映射配置目录
        os.makedirs(self.mapping_dir, exist_ok=True)
        
        # 初始化映射关系存储
        self.mapping_registry_file = os.path.join(self.mapping_dir, "mapping_registry.json")
        self.mapping_registry = self._load_mapping_registry()
        
        # 启动时检查并生成映射
        if self.config.get("enable_incremental_updates", True):
            self._check_and_initialize_mappings()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置文件
        
        Returns:
            配置字典
        """
        default_config = {
            "auto_generate_on_startup": True,
            "max_tables_per_batch": 5,
            "enable_incremental_updates": True,
            "log_level": "info",
            "excluded_tables": ["sqlite_sequence", "file_versions", "table_mappings"],
            "llm_settings": {"max_retries": 3, "timeout_seconds": 30}
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # 合并默认配置
                    default_config.update(config)
                    return default_config
            except Exception as e:
                print(f"⚠️ 加载配置文件失败，使用默认配置: {e}")
        
        return default_config
    
    def _load_mapping_registry(self) -> Dict[str, Dict[str, str]]:
        """
        加载映射关系注册表
        
        Returns:
            映射关系注册表 {表名: {配置文件路径: 文件名, 生成时间: 时间戳}}
        """
        if os.path.exists(self.mapping_registry_file):
            try:
                with open(self.mapping_registry_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 加载映射关系注册表失败: {e}")
        return {}
    
    def _save_mapping_registry(self):
        """
        保存映射关系注册表到文件
        """
        try:
            with open(self.mapping_registry_file, 'w', encoding='utf-8') as f:
                json.dump(self.mapping_registry, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"⚠️ 保存映射关系注册表失败: {e}")
    
    def _get_table_schema_and_samples(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        获取表结构和样本数据
        
        Args:
            table_name: 数据库表名
            
        Returns:
            包含列信息和样本数据的字典
        """
        try:
            conn = sqlite3.connect(self.db_manager.db_path)
            
            # 获取列信息
            columns_query = f"PRAGMA table_info([{table_name}])"
            columns_df = pd.read_sql_query(columns_query, conn)
            
            # 获取样本数据（前10行）
            sample_query = f"SELECT * FROM [{table_name}] LIMIT 10"
            sample_data = pd.read_sql_query(sample_query, conn)
            
            conn.close()
            
            return {
                'table_name': table_name,
                'columns': columns_df['name'].tolist(),
                'types': columns_df['type'].tolist(),
                'sample_data': sample_data
            }
            
        except Exception as e:
            print(f"⚠️ 获取表结构失败 {table_name}: {e}")
            return None
    
    def _generate_mapping_prompt(self, table_info: Dict[str, Any]) -> str:
        """
        生成用于大模型的列名映射提示词
        
        Args:
            table_info: 表信息字典
            
        Returns:
            大模型提示词
        """
        table_name = table_info['table_name']
        columns = table_info['columns']
        types = table_info['types']
        sample_data = table_info['sample_data']
        
        # 构建样本数据字符串
        sample_data_str = ""
        for i, row in sample_data.iterrows():
            if i >= 5:  # 只显示前5行
                break
            row_data = [str(val)[:50] + "..." if len(str(val)) > 50 else str(val) for val in row]
            sample_data_str += f"行{i+1}: {row_data}\n"
        
        prompt = f"""
# 角色定义
你是一位资深的数据库架构师和业务分析专家，拥有15年以上的数据建模和业务理解经验。你的专长是分析复杂的数据库表结构，理解业务语义，并建立准确的映射关系。

# 核心任务
请分析以下数据库表的结构和内容，建立列名到业务概念的精确映射关系。这个映射将用于自然语言查询系统，准确性至关重要。

表名: {table_name}

列信息:
{chr(10).join([f"{i+1}. {col} ({typ})" for i, (col, typ) in enumerate(zip(columns, types))])}

样本数据:
{sample_data_str}

请根据列名和数据内容，推断每个列的业务含义，并以JSON格式返回映射关系。

要求:
1. 分析每列的数据特征
2. 数据库表都是由excel文件经pandas自动转化而来，所以每个表的第一行就是列名
3. 基于列名、数据内容推断业务含义（如：产品名称、品牌、价格、规格等）
4. 主要为中文业务场景
5. 业务含义要简洁明确，便于自然语言查询理解

# 输出要求
严格按照json格式输出 {{"列名": "业务概念", ...}}

示例输出:
{{
    "Unnamed: 0": "序号",
    "Unnamed: 1": "产品名称", 
    "Unnamed: 2": "技术规格",
    "Unnamed: 3": "品牌信息",
    "Unnamed: 4": "单位"
}}

请开始分析:
        """
        
        return prompt.strip()
    
    async def _generate_column_mapping_with_llm(self, table_info: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        使用大模型生成列名映射
        
        Args:
            table_info: 表信息字典
            
        Returns:
            列名到业务含义的映射字典
        """
        try:
            llm = self.model_manager.get_llm()
            prompt = self._generate_mapping_prompt(table_info)
            
            messages = [HumanMessage(content=prompt)]
            response = await llm.ainvoke(messages)
            
            # 提取响应内容
            if hasattr(response, 'content'):
                response_text = response.content
            else:
                response_text = str(response)
            
            # 尝试解析JSON
            try:
                # 提取JSON部分（可能包含其他文本）
                json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response_text)
                if json_match:
                    json_str = json_match.group()
                    mapping = json.loads(json_str)
                    return mapping
                else:
                    print(f"⚠️ 无法从响应中提取JSON: {response_text[:200]}...")
                    return None
            except json.JSONDecodeError as e:
                print(f"⚠️ JSON解析失败: {e}")
                print(f"响应内容: {response_text[:200]}...")
                return None
                
        except Exception as e:
            print(f"⚠️ 大模型生成列名映射失败: {e}")
            return None
    
    def _save_column_mapping(self, table_name: str, mapping: Dict[str, str]) -> str:
        """
        保存列名映射配置文件
        
        Args:
            table_name: 表名
            mapping: 列名映射字典
            
        Returns:
            配置文件路径
        """
        try:
            # 生成配置文件名
            config_filename = f"{table_name}_column_mapping.json"
            config_path = os.path.join(self.mapping_dir, config_filename)
            
            # 构建完整的配置数据
            config_data = {
                "table_name": table_name,
                "generated_at": pd.Timestamp.now().isoformat(),
                "column_mappings": mapping,
                "description": f"表 {table_name} 的列名与业务含义映射配置"
            }
            
            # 保存配置文件
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ 列名映射配置已保存: {config_path}")
            return config_path
            
        except Exception as e:
            print(f"⚠️ 保存列名映射配置失败: {e}")
            return ""
    
    def _update_mapping_registry(self, table_name: str, config_path: str):
        """
        更新映射关系注册表
        
        Args:
            table_name: 表名
            config_path: 配置文件路径
        """
        self.mapping_registry[table_name] = {
            "config_file": os.path.basename(config_path),
            "config_path": config_path,
            "generated_at": pd.Timestamp.now().isoformat()
        }
        self._save_mapping_registry()
    
    async def generate_mapping_for_table(self, table_name: str) -> bool:
        """
        为指定表生成列名映射配置
        
        Args:
            table_name: 数据库表名
            
        Returns:
            是否生成成功
        """
        print(f"🔄 开始为表 {table_name} 生成列名映射...")
        
        # 获取表结构和样本数据
        table_info = self._get_table_schema_and_samples(table_name)
        if not table_info:
            print(f"❌ 无法获取表 {table_name} 的信息")
            return False
        
        # 使用大模型生成映射
        mapping = await self._generate_column_mapping_with_llm(table_info)
        if not mapping:
            print(f"❌ 无法为表 {table_name} 生成列名映射")
            return False
        
        # 保存配置文件
        config_path = self._save_column_mapping(table_name, mapping)
        if not config_path:
            return False
        
        # 更新注册表
        self._update_mapping_registry(table_name, config_path)
        
        print(f"✅ 表 {table_name} 的列名映射生成完成")
        print(f"📋 映射内容: {json.dumps(mapping, ensure_ascii=False, indent=2)}")
        return True
    
    async def generate_mappings_for_all_tables(self) -> Dict[str, bool]:
        """
        为数据库中的所有表生成列名映射配置
        
        Returns:
            生成结果字典 {表名: 是否成功}
        """
        print(f"🚀 开始为所有数据库表生成列名映射...")
        
        try:
            # 获取所有表名
            conn = sqlite3.connect(self.db_manager.db_path)
            cursor = conn.cursor()
            
            # 获取所有用户表（排除系统表）
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%' 
                AND name NOT IN ('file_versions', 'table_mappings')
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if not tables:
                print(f"📭 数据库中未找到用户表")
                return {}
            
            print(f"📋 找到 {len(tables)} 个表: {', '.join(tables)}")
            
            # 为每个表生成映射
            results = {}
            for table_name in tables:
                success = await self.generate_mapping_for_table(table_name)
                results[table_name] = success
                
                if success:
                    print(f"✅ {table_name}: 成功")
                else:
                    print(f"❌ {table_name}: 失败")
            
            # 统计结果
            success_count = sum(results.values())
            print(f"\n🎯 列名映射生成完成: {success_count}/{len(tables)} 个表成功")
            
            return results
            
        except Exception as e:
            print(f"❌ 生成所有表的列名映射失败: {e}")
            return {}
    
    def get_mapping_for_table(self, table_name: str) -> Optional[Dict[str, str]]:
        """
        获取指定表的列名映射
        
        Args:
            table_name: 表名
            
        Returns:
            列名映射字典，如果不存在则返回None
        """
        if table_name not in self.mapping_registry:
            return None
        
        try:
            config_path = self.mapping_registry[table_name]["config_path"]
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                return config_data.get("column_mappings", {})
        except Exception as e:
            print(f"⚠️ 读取表 {table_name} 的列名映射失败: {e}")
            return None
    
    def list_all_mappings(self) -> Dict[str, Dict[str, Any]]:
        """
        列出所有已生成的列名映射
        
        Returns:
            所有映射的信息字典
        """
        return self.mapping_registry.copy()
    
    def delete_mapping_for_table(self, table_name: str) -> bool:
        """
        删除指定表的列名映射配置
        
        Args:
            table_name: 表名
            
        Returns:
            是否删除成功
        """
        if table_name not in self.mapping_registry:
            print(f"⚠️ 表 {table_name} 的列名映射不存在")
            return False
        
        try:
            # 删除配置文件
            config_path = self.mapping_registry[table_name]["config_path"]
            if os.path.exists(config_path):
                os.remove(config_path)
                print(f"🗑️ 已删除配置文件: {config_path}")
            
            # 从注册表中移除
            del self.mapping_registry[table_name]
            self._save_mapping_registry()
            
            print(f"✅ 表 {table_name} 的列名映射已删除")
            return True
            
        except Exception as e:
            print(f"⚠️ 删除表 {table_name} 的列名映射失败: {e}")
            return False
    
    def _check_and_initialize_mappings(self):
        """
        启动时检查并初始化映射配置
        类似于database_manager的启动检查机制
        """
        print("🔍 检查列名映射配置状态...")
        
        # 如果映射注册表不存在，说明是首次运行
        if not os.path.exists(self.mapping_registry_file):
            print("📝 首次运行，映射注册表不存在，将创建初始配置")
            self._save_mapping_registry()  # 创建空的注册表文件
            return
        
        # 检查是否需要增量更新
        self._check_incremental_updates()
    
    def _check_incremental_updates(self):
        """
        检查并执行增量更新
        检查数据库中的表是否有新增，如有则生成对应的映射
        """
        try:
            # 获取数据库中所有表
            db_tables = self._get_all_database_tables()
            
            # 检查哪些表还没有映射配置
            missing_tables = []
            for table in db_tables:
                if table not in self.mapping_registry:
                    missing_tables.append(table)
            
            if missing_tables:
                print(f"🆕 发现 {len(missing_tables)} 个新表需要生成映射配置")
                print(f"   新表: {missing_tables[:3]}{'...' if len(missing_tables) > 3 else ''}")
                
                # 检查是否自动生成（从配置文件读取）
                auto_generate = self.config.get("auto_generate_on_startup", True)
                max_batch = self.config.get("max_tables_per_batch", 5)
                
                if auto_generate:
                    print("🚀 开始自动生成新表的映射配置...")
                    import asyncio
                    try:
                        # 为新表生成映射，限制批次大小
                        batch_tables = missing_tables[:max_batch]
                        for table in batch_tables:
                            try:
                                asyncio.run(self.generate_mapping_for_table(table))
                                print(f"✅ 表 {table} 映射生成完成")
                            except Exception as e:
                                print(f"⚠️ 表 {table} 映射生成失败: {e}")
                        
                        if len(missing_tables) > max_batch:
                            print(f"💡 还有 {len(missing_tables) - max_batch} 个表未处理，请运行 'python generate_column_mappings.py --check' 继续")
                            
                    except RuntimeError as e:
                        if "cannot run the event loop" in str(e):
                            print("⚠️ 无法在当前上下文中自动生成映射，请手动运行生成命令")
                        else:
                            raise
                else:
                    print("💡 提示: 使用 'python generate_column_mappings.py --all' 为所有新表生成映射")
                    print("   或者在配置文件中设置 'auto_generate_on_startup': true 启用自动生成")
            else:
                print("✅ 所有数据库表都已有映射配置")
                
        except Exception as e:
            print(f"⚠️ 增量更新检查失败: {e}")
    
    def _get_all_database_tables(self) -> List[str]:
        """
        获取数据库中所有用户表（排除系统表）
        
        Returns:
            表名列表
        """
        try:
            conn = sqlite3.connect(self.db_manager.db_path)
            cursor = conn.cursor()
            
            # 获取所有表名，排除系统表和配置中指定的表
            excluded_tables = self.config.get("excluded_tables", ["sqlite_sequence", "file_versions", "table_mappings"])
            excluded_placeholders = ','.join(['?' for _ in excluded_tables])
            
            query = f"""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                AND name NOT LIKE 'sqlite_%'
                AND name NOT IN ({excluded_placeholders})
            """
            
            cursor.execute(query, excluded_tables)
            
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            return tables
            
        except Exception as e:
            print(f"⚠️ 获取数据库表列表失败: {e}")
            return []
    
    def get_mapping_status(self) -> Dict[str, Any]:
        """
        获取映射配置状态信息
        
        Returns:
            状态信息字典
        """
        db_tables = self._get_all_database_tables()
        mapped_tables = list(self.mapping_registry.keys())
        unmapped_tables = [t for t in db_tables if t not in mapped_tables]
        
        return {
            "total_tables": len(db_tables),
            "mapped_tables": len(mapped_tables),
            "unmapped_tables": len(unmapped_tables),
            "mapping_coverage": len(mapped_tables) / len(db_tables) * 100 if db_tables else 0,
            "unmapped_table_list": unmapped_tables[:10],  # 只显示前10个
            "registry_file_exists": os.path.exists(self.mapping_registry_file),
            "mapping_dir_exists": os.path.exists(self.mapping_dir)
        }

# 全局列名映射生成器实例
_column_mapping_generator = None

def get_column_mapping_generator(mapping_dir: str = "column_mapping_docs") -> ColumnMappingGenerator:
    """
    获取列名映射生成器单例
    
    Args:
        mapping_dir: 映射配置目录
        
    Returns:
        列名映射生成器实例
    """
    global _column_mapping_generator
    if _column_mapping_generator is None:
        _column_mapping_generator = ColumnMappingGenerator(mapping_dir)
    return _column_mapping_generator