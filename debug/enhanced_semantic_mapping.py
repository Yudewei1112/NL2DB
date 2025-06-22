# 基于大模型的语义映射增强方案

import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional
import json
import re

class LLMSemanticMapper:
    """基于大模型的语义映射器"""
    
    def __init__(self, db_path: str):
        """初始化语义映射器"""
        self.db_path = db_path
        self.schema_info = self._analyze_database()
        
    def _analyze_database(self) -> Dict[str, Any]:
        """分析数据库结构"""
        conn = sqlite3.connect(self.db_path)
        
        # 获取所有表
        tables_query = "SELECT name FROM sqlite_master WHERE type='table';"
        tables = pd.read_sql_query(tables_query, conn)['name'].tolist()
        
        schema_info = {}
        for table_name in tables:
            # 获取列信息
            columns_query = f"PRAGMA table_info([{table_name}])"
            columns_df = pd.read_sql_query(columns_query, conn)
            
            # 获取样本数据
            sample_query = f"SELECT * FROM [{table_name}] LIMIT 5"
            sample_data = pd.read_sql_query(sample_query, conn)
            
            schema_info[table_name] = {
                'columns': columns_df['name'].tolist(),
                'types': columns_df['type'].tolist(),
                'sample_data': sample_data
            }
        
        conn.close()
        return schema_info
    
    def generate_semantic_mapping_prompt(self, table_name: str) -> str:
        """生成用于大模型的语义映射提示词"""
        table_info = self.schema_info[table_name]
        
        # 构建样本数据字符串
        sample_data_str = ""
        for i, row in table_info['sample_data'].iterrows():
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
{chr(10).join([f"{i+1}. {col} ({typ})" for i, (col, typ) in enumerate(zip(table_info['columns'], table_info['types']))])}

样本数据:
{sample_data_str}

请根据列名和数据内容，推断每个列的业务含义，并以JSON格式返回映射关系。

要求:
1. 分析每列的数据特征
2. 基于列名、数据内容推断业务含义（如：产品名称、品牌、价格、规格等）
3. 主要为中文业务场景
# 输出要求
严格按照json格式输出 {"列名": "业务概念", ...}

示例输出:
{
    "Unnamed: 0": "序号",
    "Unnamed: 1": "产品名称", 
    "Unnamed: 2": "技术规格",
    "Unnamed: 3": "品牌信息",
    "Unnamed: 4": "单位"
}

请开始分析:
        """
        
        return prompt.strip()
    

    
