#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
列名映射生成工具

用法:
    python generate_column_mappings.py --all                    # 为所有表生成映射
    python generate_column_mappings.py --table table_name       # 为指定表生成映射
    python generate_column_mappings.py --list                   # 列出所有已生成的映射
    python generate_column_mappings.py --delete table_name      # 删除指定表的映射
    python generate_column_mappings.py --info                   # 显示映射生成器信息
"""

import argparse
import asyncio
import sys
import os

# 添加父目录到Python路径，以便导入上级目录的模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from column_mapping_generator import get_column_mapping_generator
from database_manager import get_database_manager

def print_banner():
    """打印工具横幅"""
    print("="*60)
    print("🔧 NL2DB 列名映射生成工具")
    print("📋 自动生成数据库表的列名与业务含义映射配置")
    print("="*60)

def print_separator():
    """打印分隔线"""
    print("-"*60)

async def generate_all_mappings():
    """为所有表生成列名映射"""
    print("🚀 开始为所有数据库表生成列名映射...")
    print_separator()
    
    generator = get_column_mapping_generator()
    results = await generator.generate_mappings_for_all_tables()
    
    print_separator()
    if results:
        success_count = sum(results.values())
        total_count = len(results)
        print(f"📊 生成结果统计:")
        print(f"   ✅ 成功: {success_count} 个表")
        print(f"   ❌ 失败: {total_count - success_count} 个表")
        print(f"   📁 配置文件保存在: column_mapping_docs/")
    else:
        print("❌ 未找到任何表或生成失败")

async def generate_table_mapping(table_name: str):
    """为指定表生成列名映射"""
    print(f"🔄 为表 '{table_name}' 生成列名映射...")
    print_separator()
    
    generator = get_column_mapping_generator()
    success = await generator.generate_mapping_for_table(table_name)
    
    print_separator()
    if success:
        print(f"✅ 表 '{table_name}' 的列名映射生成成功")
        print(f"📁 配置文件保存在: column_mapping_docs/{table_name}_column_mapping.json")
    else:
        print(f"❌ 表 '{table_name}' 的列名映射生成失败")

def list_all_mappings():
    """列出所有已生成的映射"""
    print("📋 已生成的列名映射配置:")
    print_separator()
    
    generator = get_column_mapping_generator()
    mappings = generator.list_all_mappings()
    
    if not mappings:
        print("📭 暂无已生成的列名映射配置")
        print("💡 使用 --all 参数为所有表生成映射")
        return
    
    for i, (table_name, info) in enumerate(mappings.items(), 1):
        print(f"{i}. 表名: {table_name}")
        print(f"   📄 配置文件: {info['config_file']}")
        print(f"   🕒 生成时间: {info['generated_at']}")
        print()
    
    print(f"📊 总计: {len(mappings)} 个表的映射配置")

def delete_table_mapping(table_name: str):
    """删除指定表的列名映射"""
    print(f"🗑️ 删除表 '{table_name}' 的列名映射...")
    print_separator()
    
    generator = get_column_mapping_generator()
    success = generator.delete_mapping_for_table(table_name)
    
    print_separator()
    if success:
        print(f"✅ 表 '{table_name}' 的列名映射已删除")
    else:
        print(f"❌ 删除表 '{table_name}' 的列名映射失败")

def show_info():
    """显示映射生成器信息"""
    print("ℹ️ 列名映射生成器信息:")
    print_separator()
    
    # 数据库信息
    db_manager = get_database_manager()
    db_info = db_manager.get_database_info()
    
    print(f"📊 数据库信息:")
    print(f"   📁 数据库路径: {db_info.get('database_path', 'N/A')}")
    print(f"   💾 数据库大小: {db_info.get('database_size_mb', 0)} MB")
    print(f"   📋 活跃文件数: {db_info.get('active_files', 0)}")
    print(f"   🗂️ 数据表总数: {db_info.get('total_tables', 0)}")
    print()
    
    # 映射配置信息
    generator = get_column_mapping_generator()
    mappings = generator.list_all_mappings()
    
    print(f"🔧 映射配置信息:")
    print(f"   📁 配置目录: column_mapping_docs/")
    print(f"   📄 已生成映射: {len(mappings)} 个表")
    print(f"   📋 注册表文件: column_mapping_docs/mapping_registry.json")
    
    # 检查配置目录
    mapping_dir = "column_mapping_docs"
    if os.path.exists(mapping_dir):
        config_files = [f for f in os.listdir(mapping_dir) if f.endswith('.json')]
        print(f"   📂 配置文件数: {len(config_files)}")
    else:
        print(f"   📂 配置目录: 不存在（将自动创建）")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="NL2DB 列名映射生成工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python generate_column_mappings.py --all                    # 为所有表生成映射
  python generate_column_mappings.py --table table_1          # 为 table_1 生成映射
  python generate_column_mappings.py --list                   # 列出所有映射
  python generate_column_mappings.py --delete table_1         # 删除 table_1 的映射
  python generate_column_mappings.py --info                   # 显示系统信息
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true', help='为所有数据库表生成列名映射')
    group.add_argument('--table', type=str, help='为指定表生成列名映射')
    group.add_argument('--list', action='store_true', help='列出所有已生成的映射')
    group.add_argument('--status', action='store_true', help='显示映射配置状态和覆盖率')
    group.add_argument('--delete', type=str, help='删除指定表的映射配置')
    group.add_argument('--check', action='store_true', help='检查并执行增量更新')
    group.add_argument('--info', action='store_true', help='显示映射生成器信息')
    
    args = parser.parse_args()
    
    print_banner()
    
    try:
        if args.all:
            asyncio.run(generate_all_mappings())
        elif args.table:
            asyncio.run(generate_table_mapping(args.table))
        elif args.list:
            list_all_mappings()
        elif args.status:
            # 显示映射配置状态
            generator = get_column_mapping_generator()
            status = generator.get_mapping_status()
            print(f"\n📊 列名映射配置状态:")
            print(f"   数据库表总数: {status['total_tables']}")
            print(f"   已映射表数量: {status['mapped_tables']}")
            print(f"   未映射表数量: {status['unmapped_tables']}")
            print(f"   映射覆盖率: {status['mapping_coverage']:.1f}%")
            print(f"   注册表文件: {'✅ 存在' if status['registry_file_exists'] else '❌ 不存在'}")
            print(f"   映射目录: {'✅ 存在' if status['mapping_dir_exists'] else '❌ 不存在'}")
            
            if status['unmapped_tables'] > 0:
                print(f"\n🔍 未映射的表 (显示前10个):")
                for i, table in enumerate(status['unmapped_table_list'], 1):
                    print(f"   {i}. {table}")
                if status['unmapped_tables'] > 10:
                    print(f"   ... 还有 {status['unmapped_tables'] - 10} 个表未显示")
        elif args.check:
            # 检查并执行增量更新
            print("\n🔄 执行增量更新检查...")
            generator = get_column_mapping_generator()
            generator._check_incremental_updates()
        elif args.delete:
            delete_table_mapping(args.delete)
        elif args.info:
            show_info()
    
    except KeyboardInterrupt:
        print("\n⚠️ 操作被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 操作失败: {e}")
        sys.exit(1)
    
    print_separator()
    print("🎉 操作完成")

if __name__ == "__main__":
    main()