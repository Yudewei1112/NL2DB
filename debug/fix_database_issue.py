import os
import json
from database_manager import get_database_manager

def fix_database_issue():
    """修复数据库不一致问题"""
    
    print("🔧 开始修复数据库不一致问题...")
    
    # 1. 清空file_registry.json，强制重新处理所有文件
    registry_file = "file_registry.json"
    if os.path.exists(registry_file):
        print(f"🗑️ 清空文件注册表: {registry_file}")
        with open(registry_file, 'w', encoding='utf-8') as f:
            json.dump({}, f)
    
    # 2. 清空数据库中的元数据表
    db_manager = get_database_manager()
    import sqlite3
    
    try:
        conn = sqlite3.connect(db_manager.db_path)
        cursor = conn.cursor()
        
        print("🗑️ 清空数据库元数据表...")
        cursor.execute("DELETE FROM file_versions")
        cursor.execute("DELETE FROM table_mappings")
        
        # 删除所有以table_开头的用户数据表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'table_%'")
        user_tables = cursor.fetchall()
        
        for table in user_tables:
            table_name = table[0]
            print(f"🗑️ 删除数据表: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS [{table_name}]")
        
        conn.commit()
        conn.close()
        
        print("✅ 数据库清理完成")
        
    except Exception as e:
        print(f"❌ 清理数据库失败: {e}")
    
    # 3. 重新处理Excel文件
    excel_path = "uploads/(会议确定稿整理版)附件2：无价材料询价核定表（泛光、道路、景观灯具 ）2.14定价.xlsx"
    
    if os.path.exists(excel_path):
        print(f"\n🔄 重新处理Excel文件: {excel_path}")
        
        # 重新创建数据库管理器实例以清空内存缓存
        global _db_manager
        from database_manager import _db_manager
        _db_manager = None
        
        db_manager = get_database_manager()
        updated, table_mapping = db_manager.update_if_changed(excel_path)
        
        print(f"处理结果: 更新={updated}, 映射={table_mapping}")
        
        # 验证结果
        print("\n🔍 验证处理结果...")
        try:
            conn = sqlite3.connect(db_manager.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'table_%'")
            tables = cursor.fetchall()
            print(f"创建的数据表: {[t[0] for t in tables]}")
            
            cursor.execute("SELECT * FROM file_versions")
            versions = cursor.fetchall()
            print(f"文件版本记录: {len(versions)} 条")
            
            cursor.execute("SELECT * FROM table_mappings")
            mappings = cursor.fetchall()
            print(f"表映射记录: {len(mappings)} 条")
            
            # 检查数据表内容
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                count = cursor.fetchone()[0]
                print(f"表 {table_name} 行数: {count}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 验证失败: {e}")
    
    else:
        print(f"❌ Excel文件不存在: {excel_path}")
    
    print("\n🎯 修复完成！")

if __name__ == "__main__":
    fix_database_issue()