from fastmcp import FastMCP
from typing import Dict, Any, List
import asyncio
import os
import json
from dotenv import load_dotenv
from fastmcp import FastMCP

# 导入原有的NL2DB功能
from NL2DB import (
    run_flow, 
    EXCEL_DIR
)
from database_manager import get_database_manager

# 加载环境变量
load_dotenv()

# 创建FastMCP实例
mcp = FastMCP("NL2DB Service")

@mcp.tool()
async def query_excel_data(query: str) -> Dict[str, Any]:
    """
    查询Excel数据的MCP工具
    
    Args:
        query: 用户的自然语言查询
        
    Returns:
        包含查询结果的字典，包括SQL查询、数据库结果和自然语言答案
    """
    # 调试输出：输入的查询
    print("\n" + "="*60)
    print(f"🔍 [DEBUG] 收到查询请求: {query}")
    print("="*60)
    
    try:
        # 检查uploads目录下的Excel文件
        excel_files = [
            os.path.join(EXCEL_DIR, f) 
            for f in os.listdir(EXCEL_DIR) 
            if f.endswith(('.xlsx', '.xls'))
        ]
        
        if not excel_files:
            error_response = {
                "status": "error",
                "message": f"{EXCEL_DIR} 目录下未找到 Excel 文件，请先上传！",
                "query": query,
                "context": {},
                "answer": "未找到Excel文件",
                "metadata": {
                    "total_sheets_found": 0,
                    "sheets_used_for_query": 0,
                    "has_results": False
                }
            }
            
            # 调试输出：错误响应
            print(f"❌ [DEBUG] 错误响应:")
            print(json.dumps(error_response, ensure_ascii=False, indent=2))
            print("="*60 + "\n")
            
            return error_response
        
        # 使用第一个Excel文件进行查询
        excel_file = excel_files[0]
        db_file = "database.db"
        
        print(f"📁 [DEBUG] 使用Excel文件: {excel_file}")
        
        # 执行查询流程
        mcp_response = await run_flow(query, excel_file, db_file)
        
        # 添加SQL调试信息到响应中
        sql_query = mcp_response.get('context', {}).get('sql_query', '')
        if sql_query:
            print(f"\n🔧 [MCP DEBUG] 向客户端返回SQL语句: {sql_query}")
            # 在响应中添加调试信息
            if 'debug_info' not in mcp_response:
                mcp_response['debug_info'] = {}
            mcp_response['debug_info']['generated_sql'] = sql_query
            mcp_response['debug_info']['sql_execution_status'] = 'success' if mcp_response.get('context', {}).get('database_results', {}).get('data') else 'no_results'
        
        # 调试输出：成功响应
        print(f"✅ [DEBUG] 查询成功，响应结果:")
        print(json.dumps(mcp_response, ensure_ascii=False, indent=2))
        print("="*60 + "\n")
        
        return mcp_response
        
    except Exception as e:
        error_response = {
            "status": "error",
            "message": f"查询过程中发生错误: {str(e)}",
            "query": query,
            "context": {},
            "answer": "查询失败",
            "metadata": {
                "total_sheets_found": 0,
                "sheets_used_for_query": 0,
                "has_results": False
            },
            "debug_info": {
                "error_details": str(e),
                "generated_sql": "SQL生成失败",
                "sql_execution_status": "error"
            }
        }
        
        # 调试输出：异常响应
        print(f"💥 [DEBUG] 异常发生: {str(e)}")
        print(f"❌ [DEBUG] 异常响应:")
        print(json.dumps(error_response, ensure_ascii=False, indent=2))
        print("="*60 + "\n")
        
        return error_response



async def initialize_vector_database():
    """
    初始化向量数据库
    """
    try:
        print("🧠 初始化向量数据库...")
        from NL2DB import model_manager, create_and_store_vectors
        
        # 获取模型实例
        llm = model_manager.get_llm()
        embedding_model = model_manager.get_embedding_model()
        
        # 创建向量数据库
        vectorstore = await create_and_store_vectors(EXCEL_DIR, llm, embedding_model)
        
        if vectorstore:
            print("✅ 向量数据库初始化完成")
        else:
            print("⚠️ 向量数据库初始化失败")
            
    except Exception as e:
        print(f"❌ 向量数据库初始化异常: {e}")
        print("系统将继续启动，但可能影响查询准确性")

def main():
    """
    启动MCP服务器
    """
    print("🚀 启动 NL2DB MCP 服务...")
    print("📍 服务地址: http://127.0.0.1:9001")
    print("📊 传输方式: Server-Sent Events (SSE)")
    print("📁 Excel文件目录:", EXCEL_DIR)
    print("🔍 向量数据库目录: Faiss")
    print("\n可用工具:")
    print("  - query_excel_data: 查询Excel数据")
    print("\n按 Ctrl+C 停止服务")
    print("-" * 50)
    
    # 初始化数据库管理器并检查所有Excel文件
    print("📊 初始化数据库管理器...")
    db_manager = get_database_manager()
    db_manager.check_all_files(EXCEL_DIR)
    print("✅ 数据库初始化完成")
    
    # 初始化列名映射生成器
    print("📝 初始化列名映射生成器...")
    try:
        from column_mapping_generator import get_column_mapping_generator
        column_mapping_generator = get_column_mapping_generator()
        print("✅ 列名映射生成器初始化完成")
        
        # 获取映射状态
        status = column_mapping_generator.get_mapping_status()
        print(f"📊 映射状态: {status['mapped_tables']}/{status['total_tables']} 个表已配置映射")
        
    except Exception as e:
        print(f"⚠️ 列名映射生成器初始化失败: {e}")
        print("系统将继续启动，但可能影响查询准确性")
    
    # 初始化向量数据库
    try:
        asyncio.run(initialize_vector_database())
    except Exception as e:
        print(f"❌ 向量数据库初始化失败: {e}")
        print("系统将继续启动，但可能影响查询准确性")
    
    # 启动服务器
    mcp.run(
        transport="sse",
        host="127.0.0.1",
        port=9001
    )

if __name__ == "__main__":
    main()