import os
import sqlite3
from database_manager import get_database_manager

def reinitialize_database():
    """重新初始化数据库"""
    
    print("🔧 开始重新初始化数据库...")
    
    # 1. 删除现有数据库文件
    db_path = "database.db"
    if os.path.exists(db_path):
        print(f"🗑️ 删除现有数据库: {db_path}")
        os.remove(db_path)
    
    # 2. 删除文件注册表
    registry_file = "file_registry.json"
    if os.path.exists(registry_file):
        print(f"🗑️ 删除文件注册表: {registry_file}")
        os.remove(registry_file)
    
    # 3. 重新创建数据库管理器
    print("🔄 重新创建数据库管理器...")
    
    # 清空全局实例
    import database_manager
    database_manager._db_manager = None
    
    # 创建新的数据库管理器实例
    db_manager = get_database_manager()
    
    # 4. 验证数据库表结构
    print("🔍 验证数据库表结构...")
    try:
        conn = sqlite3.connect(db_manager.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"数据库中的表: {[t[0] for t in tables]}")
        
        # 检查表结构
        for table_name in ['file_versions', 'table_mappings']:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print(f"表 {table_name} 的列: {[col[1] for col in columns]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 验证数据库失败: {e}")
        return
    
    # 5. 处理Excel文件
    excel_path = "uploads/(会议确定稿整理版)附件2：无价材料询价核定表（泛光、道路、景观灯具 ）2.14定价.xlsx"
    
    if os.path.exists(excel_path):
        print(f"\n📊 处理Excel文件: {excel_path}")
        
        try:
            updated, table_mapping = db_manager.update_if_changed(excel_path)
            print(f"处理结果: 更新={updated}, 映射={table_mapping}")
            
            # 验证处理结果
            print("\n🔍 验证处理结果...")
            conn = sqlite3.connect(db_manager.db_path)
            cursor = conn.cursor()
            
            # 检查所有表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            all_tables = cursor.fetchall()
            print(f"所有表: {[t[0] for t in all_tables]}")
            
            # 检查数据表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'table_%'")
            data_tables = cursor.fetchall()
            print(f"数据表: {[t[0] for t in data_tables]}")
            
            # 检查元数据
            cursor.execute("SELECT COUNT(*) FROM file_versions")
            file_count = cursor.fetchone()[0]
            print(f"文件版本记录: {file_count} 条")
            
            cursor.execute("SELECT COUNT(*) FROM table_mappings")
            mapping_count = cursor.fetchone()[0]
            print(f"表映射记录: {mapping_count} 条")
            
            # 显示具体记录
            if file_count > 0:
                cursor.execute("SELECT * FROM file_versions")
                versions = cursor.fetchall()
                for v in versions:
                    print(f"  文件版本: {v}")
            
            if mapping_count > 0:
                cursor.execute("SELECT * FROM table_mappings")
                mappings = cursor.fetchall()
                for m in mappings:
                    print(f"  表映射: {m}")
            
            # 检查数据表内容
            for table in data_tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                row_count = cursor.fetchone()[0]
                print(f"表 {table_name} 行数: {row_count}")
                
                if row_count > 0:
                    cursor.execute(f"SELECT * FROM [{table_name}] LIMIT 3")
                    sample_rows = cursor.fetchall()
                    print(f"  样本数据: {sample_rows}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 处理Excel文件失败: {e}")
            import traceback
            traceback.print_exc()
    
    else:
        print(f"❌ Excel文件不存在: {excel_path}")
    
    print("\n✅ 数据库重新初始化完成！")

if __name__ == "__main__":
    reinitialize_database()