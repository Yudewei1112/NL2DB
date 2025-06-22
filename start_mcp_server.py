#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NL2DB MCP服务启动脚本

这个脚本用于启动NL2DB的MCP服务，提供自然语言到数据库查询的功能。

使用方法:
    python start_mcp_server.py

服务将在 http://127.0.0.1:9001 启动
"""

import sys
import os
from pathlib import Path
import json
from datetime import datetime

def check_requirements():
    """检查必要的依赖是否安装"""
    required_packages = [
        'fastmcp',
        'langchain_core',
        'langchain_community', 
        'langchain_huggingface',
        'pandas',
        'sqlite3',
        'faiss',
        'sentence_transformers',
        'FlagEmbedding'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'sqlite3':
                import sqlite3
            elif package == 'faiss':
                import faiss
            else:
                __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("❌ 缺少以下依赖包:")
        for pkg in missing_packages:
            print(f"   - {pkg}")
        print("\n请运行以下命令安装依赖:")
        print("   pip install -r requirements.txt")
        return False
    
    return True

def check_environment():
    """检查环境配置"""
    env_file = Path('.env')
    if not env_file.exists():
        print("⚠️  未找到 .env 文件")
        print("请确保已配置必要的环境变量（如API密钥等）")
        return False
    
    # 检查必要的目录
    uploads_dir = Path('uploads')
    if not uploads_dir.exists():
        print("📁 创建 uploads 目录...")
        uploads_dir.mkdir(exist_ok=True)
    
    faiss_dir = Path('Faiss')
    if not faiss_dir.exists():
        print("📁 创建 Faiss 目录...")
        faiss_dir.mkdir(exist_ok=True)
    
    # 重新启用列名映射功能
    column_mapping_dir = Path('column_mapping_docs')
    if not column_mapping_dir.exists():
        print("📁 创建 column_mapping_docs 目录...")
        column_mapping_dir.mkdir(exist_ok=True)
    
    # 主动创建列名映射配置文件
    create_column_mapping_configs()
    
    return True

def create_column_mapping_configs():
    """创建列名映射配置文件"""
    try:
        from column_mapping_generator import get_column_mapping_generator
        
        print("📝 检查并创建列名映射配置文件...")
        
        # 创建配置目录
        config_dir = "column_mapping_docs"
        os.makedirs(config_dir, exist_ok=True)
        
        # 初始化列名映射生成器（这会触发启动检查）
        try:
            column_mapping_generator = get_column_mapping_generator(config_dir)
            print("✅ 列名映射生成器初始化完成")
            
            # 获取映射状态
            status = column_mapping_generator.get_mapping_status()
            print(f"📊 映射状态: {status['mapped_tables']}/{status['total_tables']} 个表已配置映射")
            
            if status['unmapped_tables'] > 0:
                print(f"💡 还有 {status['unmapped_tables']} 个表需要配置映射")
                print("   系统将在启动时自动检查并生成")
                
        except Exception as e:
            print(f"⚠️ 列名映射生成器初始化失败: {e}")
            print("将创建基础配置文件...")
        
        print("✅ 列名映射配置文件检查完成")
        
    except ImportError as e:
        print(f"⚠️ 导入列名映射管理器失败: {e}")
        print("将跳过配置文件创建")
    except Exception as e:
        print(f"⚠️ 创建列名映射配置文件时发生错误: {e}")
        print("系统将继续启动，但可能需要手动创建配置文件")

def main():
    """主函数"""
    print("🔍 NL2DB MCP服务启动检查...")
    print("=" * 50)
    
    # 检查依赖
    if not check_requirements():
        sys.exit(1)
    
    # 检查环境
    if not check_environment():
        print("\n⚠️  环境检查未完全通过，但服务仍可启动")
    
    print("\n✅ 环境检查完成")
    print("🚀 正在启动MCP服务...")
    print("=" * 50)
    
    # 启动MCP服务
    try:
        from NL2DB_mcp_server import main as start_server
        start_server()
    except KeyboardInterrupt:
        print("\n\n👋 服务已停止")
    except Exception as e:
        print(f"\n❌ 启动服务时发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()