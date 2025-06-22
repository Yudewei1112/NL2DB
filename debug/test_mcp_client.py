#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NL2DB MCP服务测试客户端

这个脚本用于测试NL2DB MCP服务的各种功能，包括:
- 基本查询功能测试
- 响应时间性能测试
- 错误处理测试
- 交互式查询模式

使用方法:
    # 运行自动测试
    python test_mcp_client.py
    
    # 运行交互模式
    python test_mcp_client.py --interactive

前置条件:
1. 确保MCP服务已在 http://127.0.0.1:9001/sse 启动
2. 确保uploads目录下有Excel文件
3. 确保已安装fastmcp依赖: pip install fastmcp

版本: 2024-01-15
作者: NL2DB Team
"""

import asyncio
import json
import time
from typing import Dict, Any
from fastmcp import Client

class MCPClient:
    """MCP客户端 - 使用FastMCP客户端库"""
    
    def __init__(self, server_url: str = "http://127.0.0.1:9001/sse"):
        self.server_url = server_url
        self.client = None
    
    async def __aenter__(self):
        self.client = Client(self.server_url)
        await self.client.__aenter__()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def call_tool(self, tool_name: str, **kwargs) -> Dict[str, Any]:
        """调用MCP工具"""
        try:
            result = await self.client.call_tool(tool_name, kwargs)
            
            # 处理不同类型的响应格式
            if hasattr(result, 'content') and result.content:
                if isinstance(result.content, list) and len(result.content) > 0:
                    # 如果content是列表，取第一个元素
                    content_item = result.content[0]
                    if hasattr(content_item, 'text'):
                        result_text = content_item.text
                    else:
                        result_text = str(content_item)
                else:
                    result_text = str(result.content)
            else:
                result_text = str(result)
            
            # 尝试解析JSON内容
            try:
                # 如果result_text是JSON字符串，解析它
                parsed_result = json.loads(result_text)
                return {
                    "status": "success",
                    "result": parsed_result
                }
            except (json.JSONDecodeError, TypeError):
                # 如果不是有效的JSON，返回原始文本
                return {
                    "status": "success",
                    "result": result_text
                }
                
        except Exception as e:
            return {
                "error": "连接错误",
                "message": str(e)
            }
    
    async def test_query(self, query: str):
        """测试查询功能"""
        print(f"\n🔎 测试查询: {query}")
        
        # 记录开始时间
        start_time = time.time()
        result = await self.call_tool("query_excel_data", query=query)
        end_time = time.time()
        
        # 计算响应时间
        response_time = end_time - start_time
        print(f"⏱️ 响应时间: {response_time:.2f}秒")
        
        # 如果结果包含解析后的JSON数据，格式化显示
        if result.get("status") == "success" and isinstance(result.get("result"), dict):
            query_result = result["result"]
            
            # 检查是否是错误响应
            if query_result.get('status') == 'error':
                print(f"❌ 查询失败: {query_result.get('message', 'N/A')}")
                print(f"💬 查询内容: {query_result.get('query', 'N/A')}")
                print(f"🎯 答案: {query_result.get('answer', 'N/A')}")
            else:
                # 成功响应 - 适配新的简化格式
                print(f"✅ 查询成功")
                print(f"💬 查询内容: {query_result.get('query', 'N/A')}")
                print(f"🎯 答案: {query_result.get('answer', 'N/A')}")
            
            # 显示上下文信息（如果存在）
            context = query_result.get('context', {})
            if context:
                print("📋 上下文信息:")
                if 'excel_file' in context:
                    print(f"  📁 Excel文件: {context['excel_file']}")
                if 'selected_sheets' in context:
                    print(f"  📄 使用的工作表: {context['selected_sheets']}")
                if 'sql_query' in context:
                    print(f"  🔍 SQL查询: {context['sql_query']}")
                if 'database_results' in context:
                    db_results = context['database_results']
                    if isinstance(db_results, dict) and 'data' in db_results:
                        data_list = db_results['data']
                        if isinstance(data_list, list) and data_list:
                            print(f"  📈 数据库结果: 找到 {len(data_list)} 条记录")
                        else:
                            print(f"  📈 数据库结果: 无数据")
                    else:
                        print(f"  📈 数据库结果: {db_results}")
            
            # 显示元数据（如果存在）
            metadata = query_result.get('metadata', {})
            if metadata:
                print("📊 元数据:")
                for key, value in metadata.items():
                    print(f"  {key}: {value}")
            
            # 显示调试信息（如果存在）
            debug_info = query_result.get('debug_info', {})
            if debug_info:
                print("🔧 调试信息:")
                for key, value in debug_info.items():
                    print(f"  {key}: {value}")
        else:
            # 如果不是预期的JSON格式，显示原始结果
            print("📄 原始结果:")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        
        return result
    
    async def list_available_tools(self):
        """列出可用工具"""
        try:
            tools = await self.client.list_tools()
            print("\n🛠️ 可用工具:")
            for tool in tools.tools:
                print(f"  - {tool.name}: {tool.description}")
            return tools
        except Exception as e:
            print(f"获取工具列表失败: {e}")
            return None
    
    async def check_excel_files(self):
        """检查Excel文件是否存在"""
        print("\n📁 检查Excel文件...")
        # 直接调用查询工具来检查文件状态
        result = await self.call_tool("query_excel_data", query="检查文件状态")
        
        if isinstance(result, dict) and "result" in result:
            # 现在result["result"]已经是解析后的字典或字符串
            result_data = result["result"]
            
            # 如果是字典类型（已解析的JSON）
            if isinstance(result_data, dict):
                if result_data.get("status") == "error" and "未找到" in result_data.get("message", ""):
                    print("❌ 未找到Excel文件")
                    return False
                else:
                    print("✅ Excel文件已找到")
                    return True
            # 如果是字符串类型，尝试解析
            elif isinstance(result_data, str):
                try:
                    parsed_data = json.loads(result_data)
                    if parsed_data.get("status") == "error" and "未找到" in parsed_data.get("message", ""):
                        print("❌ 未找到Excel文件")
                        return False
                    else:
                        print("✅ Excel文件已找到")
                        return True
                except json.JSONDecodeError:
                    print("⚠️  无法解析文件检查结果")
                    return False
        
        return False

async def main():
    """主测试函数"""
    print("🧪 NL2DB MCP服务测试客户端")
    print("=" * 50)
    
    try:
        async with MCPClient() as client:
            # 首先列出可用工具
            await client.list_available_tools()
            
            # 检查Excel文件是否存在
            has_excel_files = await client.check_excel_files()
            
            # 如果有Excel文件，进行查询测试
            if has_excel_files:
                test_queries = [
                    "LED线性洗墙灯的定价是多少？",
                    "景观照明类产品有哪些？",
                    "总价最高的产品是什么？",
                    "显示所有泛光照明产品的信息",
                    "道路照明产品的平均价格是多少？"
                ]
                
                print("\n🚀 开始查询测试...")
                for i, query in enumerate(test_queries, 1):
                    print(f"\n--- 测试 {i}/{len(test_queries)} ---")
                    await client.test_query(query)
                    await asyncio.sleep(1)  # 避免请求过快
            else:
                print("\n⚠️  未找到Excel文件，跳过查询测试")
                print("请在uploads目录下放置Excel文件后重新测试")
                
                # 即使没有文件，也可以测试一下服务响应
                print("\n🔍 测试服务响应...")
                await client.test_query("测试查询")
        
        print("\n✅ 测试完成")
        
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        print("请确保MCP服务正在运行在 http://127.0.0.1:9001/sse")

def interactive_mode():
    """交互模式"""
    print("\n🎯 进入交互模式")
    print("输入查询语句，输入 'quit' 退出")
    print("-" * 30)
    
    async def interactive_session():
        async with MCPClient() as client:
            while True:
                try:
                    query = input("\n请输入查询: ").strip()
                    if query.lower() in ['quit', 'exit', 'q']:
                        break
                    if query:
                        await client.test_query(query)
                except KeyboardInterrupt:
                    break
                except EOFError:
                    break
    
    asyncio.run(interactive_session())
    print("\n👋 退出交互模式")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        interactive_mode()
    else:
        print("运行自动测试...")
        print("如需交互模式，请使用: python test_mcp_client.py --interactive")
        print()
        asyncio.run(main())