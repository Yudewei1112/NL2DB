from typing import List, Dict, Tuple, Any, Optional, TypedDict
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.documents import Document
from langchain_community.document_loaders import UnstructuredExcelLoader
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from FlagEmbedding import FlagReranker
import pandas as pd
import sqlite3
import os
import asyncio
import json
import hashlib
import time
import threading
from typing import List


# --- 步骤 0: 定义全局配置 ---
VECTOR_DB_PATH = "vector_db.faiss"
VECTOR_DB_METADATA_PATH = "vector_db.pkl"
EXCEL_DIR = "uploads"  # 存放 Excel 文件的目录
EMBEDDING_MODEL_NAME = "moka-ai/m3e-base"
CACHE_DIR = "cache"
HEADER_CACHE_DIR = os.path.join(CACHE_DIR, "headers")

# 创建缓存目录
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(HEADER_CACHE_DIR, exist_ok=True)

# --- 单例模式的模型管理器 ---
class ModelManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._llm = None
            self._embedding_model = None
            self._reranker = None
            self._llm_config = {
                "provider": os.getenv("LLM_PROVIDER", "glm"),
                "model_name": os.getenv("LLM_MODEL_NAME", "glm-4-plus"),
                "temperature": float(os.getenv("LLM_TEMPERATURE", 0.2))
            }
            self._initialized = True
    
    def get_llm(self):
        """懒加载LLM模型"""
        if self._llm is None:
            self._llm = self._create_llm(self._llm_config)
        return self._llm
    
    def get_embedding_model(self):
        """懒加载嵌入模型"""
        if self._embedding_model is None:
            self._embedding_model = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL_NAME)
        return self._embedding_model
    
    def get_reranker(self):
        """懒加载重排序模型"""
        if self._reranker is None:
            self._reranker = FlagReranker('BAAI/bge-reranker-v2-m3', use_fp16=True)
        return self._reranker
    
    def _create_llm(self, config):
        """创建LLM实例"""
        provider = config.get("provider", "glm")
        if provider == "openai":
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                openai_api_key=os.getenv("OPENAI_API_KEY"),
                base_url=os.getenv("OPENAI_BASE_URL"),
                model=config.get("model_name", "gpt-3.5-turbo"),
                temperature=config.get("temperature", 0.2)
            )
        elif provider == "glm":
            from langchain_community.chat_models.zhipuai import ChatZhipuAI
            return ChatZhipuAI(
                zhipuai_api_key=os.getenv("GLM_4_PLUS_API_KEY"),
                base_url=os.getenv("GLM_4_PLUS_API_BASE"),
                model=config.get("model_name", "glm-4-plus"),
                temperature=config.get("temperature", 0.2)
            )
        elif provider == "qwen":
            from langchain_community.chat_models.tongyi import ChatTongyi
            return ChatTongyi(
                dashscope_api_key=os.getenv("QWEN_API_KEY"),
                base_url=os.getenv("QWEN_API_BASE"),
                model_name=config.get("model_name", "qwen-turbo"),
                temperature=config.get("temperature", 0.2)
            )
        elif provider == "deepseek":
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                openai_api_key=os.getenv("DEEPSEEK_API_KEY"),
                base_url=os.getenv("DEEPSEEK_API_BASE"),
                model=config.get("model_name", "deepseek-coder"),
                temperature=config.get("temperature", 0.2)
            )
        elif provider == "claude":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                anthropic_api_key=os.getenv("CLAUDE_API_KEY"),
                base_url=os.getenv("CLAUDE_API_BASE"),
                model=config.get("model_name", "claude-3-7-sonnet"),
                temperature=config.get("temperature", 0.2)
            )
        else:
            print("Using mock LLM.")
            async def mock_llm(messages, config=None):
                return "SQL Query Placeholder"
            return RunnableLambda(mock_llm)

# 全局模型管理器实例
model_manager = ModelManager()

# ============================================================================
# --- 数据准备部分 ---
# ============================================================================

# --- 导入新的数据库管理器 ---
from database_manager import get_database_manager

def get_table_mapping(excel_path: str) -> Dict[str, str]:
    """
    获取指定Excel文件的表映射
    
    Args:
        excel_path: Excel文件路径
        
    Returns:
        表映射字典 {sheet_name: table_name}
    """
    return get_database_manager().get_table_mapping(excel_path)

def get_enhanced_table_mapping(excel_name: str = None) -> Dict[Tuple[str, str], str]:
    """
    获取增强的表映射（方案1：包含excel_name的映射结构）
    
    Args:
        excel_name: Excel文件名，如果为None则返回所有映射
        
    Returns:
        增强映射字典 {(excel_name, sheet_name): table_name}
    """
    return get_database_manager().get_enhanced_table_mapping(excel_name)

def get_table_name_by_excel_sheet(excel_name: str, sheet_name: str) -> str:
    """
    根据Excel文件名和Sheet名获取对应的表名（方案1专用方法）
    
    Args:
        excel_name: Excel文件名
        sheet_name: Sheet名称
        
    Returns:
        对应的数据库表名，如果未找到返回None
    """
    return get_database_manager().get_table_name_by_excel_sheet(excel_name, sheet_name)

# --- 表头信息缓存管理器 ---
class HeaderCacheManager:
    def __init__(self, cache_dir: str = HEADER_CACHE_DIR):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.memory_cache = {}  # 内存缓存
        self.max_memory_cache = 100  # 最大内存缓存数量
    
    def get_cache_key(self, excel_path: str, sheet_name: str) -> str:
        """生成缓存键"""
        excel_name = os.path.splitext(os.path.basename(excel_path))[0]
        return f"{excel_name}_{sheet_name}"
    
    def get_cache_path(self, excel_path: str, sheet_name: str) -> str:
        """获取缓存文件路径"""
        cache_key = self.get_cache_key(excel_path, sheet_name)
        return os.path.join(self.cache_dir, f"{cache_key}.json")
    
    def get_file_hash(self, file_path: str) -> str:
        """计算文件哈希"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def load_cached_header(self, excel_path: str, sheet_name: str) -> Optional[str]:
        """加载缓存的表头信息"""
        cache_key = self.get_cache_key(excel_path, sheet_name)
        
        # 先检查内存缓存
        if cache_key in self.memory_cache:
            cache_data = self.memory_cache[cache_key]
            if self._is_cache_valid(excel_path, cache_data):
                return cache_data['header_info']
            else:
                del self.memory_cache[cache_key]
        
        # 检查文件缓存
        cache_path = self.get_cache_path(excel_path, sheet_name)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                
                if self._is_cache_valid(excel_path, cache_data):
                    # 加载到内存缓存
                    self._add_to_memory_cache(cache_key, cache_data)
                    return cache_data['header_info']
                else:
                    os.remove(cache_path)
            except Exception:
                if os.path.exists(cache_path):
                    os.remove(cache_path)
        
        return None
    
    def cache_header_analysis(self, excel_path: str, sheet_name: str, header_info: str):
        """缓存表头分析结果"""
        cache_data = {
            'excel_path': excel_path,
            'sheet_name': sheet_name,
            'header_info': header_info,
            'timestamp': time.time(),
            'file_hash': self.get_file_hash(excel_path),
            'model_version': 'v1.0'
        }
        
        # 保存到文件缓存
        cache_path = self.get_cache_path(excel_path, sheet_name)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        
        # 保存到内存缓存
        cache_key = self.get_cache_key(excel_path, sheet_name)
        self._add_to_memory_cache(cache_key, cache_data)
    
    def _is_cache_valid(self, excel_path: str, cache_data: dict) -> bool:
        """检查缓存是否有效"""
        try:
            # 检查文件是否存在
            if not os.path.exists(excel_path):
                return False
            
            # 检查文件哈希是否匹配
            current_hash = self.get_file_hash(excel_path)
            if current_hash != cache_data.get('file_hash'):
                return False
            
            # 检查模型版本
            if cache_data.get('model_version') != 'v1.0':
                return False
            
            return True
        except Exception:
            return False
    
    def _add_to_memory_cache(self, cache_key: str, cache_data: dict):
        """添加到内存缓存"""
        if len(self.memory_cache) >= self.max_memory_cache:
            # 移除最旧的缓存项
            oldest_key = min(self.memory_cache.keys(), 
                           key=lambda k: self.memory_cache[k]['timestamp'])
            del self.memory_cache[oldest_key]
        
        self.memory_cache[cache_key] = cache_data
    
    def clear_cache(self):
        """清理所有缓存"""
        self.memory_cache.clear()
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
    
    def get_cache_stats(self) -> dict:
        """获取缓存统计信息"""
        file_cache_count = len([f for f in os.listdir(self.cache_dir) if f.endswith('.json')])
        return {
            'memory_cache_count': len(self.memory_cache),
            'file_cache_count': file_cache_count,
            'max_memory_cache': self.max_memory_cache
        }

# 全局缓存管理器实例
header_cache_manager = HeaderCacheManager()



# --- 优化后的辅助函数 ---
def excel_to_sqlite(excel_path: str, db_path: str = "database.db") -> Tuple[str, Dict[str, str]]:
    """将 Excel 文件转换为 SQLite 数据库"""
    db_manager = get_database_manager()
    db_manager.update_if_changed(excel_path)
    return db_manager.db_path, db_manager.get_table_mapping(excel_path)

async def identify_header_with_cache(excel_path: str, sheet_name: str, llm_model) -> Optional[str]:
    """带缓存的表头识别"""
    # 尝试从缓存加载
    cached_header = header_cache_manager.load_cached_header(excel_path, sheet_name)
    if cached_header:
        return cached_header
    
    # 缓存未命中，执行LLM分析
    header_info = await identify_header(excel_path, sheet_name, llm_model)
    
    # 缓存结果
    if header_info:
        header_cache_manager.cache_header_analysis(excel_path, sheet_name, str(header_info))
    
    return header_info

async def identify_header(excel_path: str, sheet_name: str, llm_model):
    """使用大模型识别 Excel Sheet 的表头和关键信息"""
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        
        content_lines = []
        headers = df.columns.tolist()
        content_lines.append("表头: " + " | ".join(str(h) for h in headers))
        
        content_lines.append("\n数据样本:")
        for idx, row in df.iterrows():
            row_data = " | ".join(str(val) for val in row.values)
            content_lines.append(f"第{idx+1}行: {row_data}")
        
        content_lines.append(f"\n数据统计: 共{len(df)}行，{len(df.columns)}列")
        content = "\n".join(content_lines)
        
        with open("excel_header_prompt.txt", "r", encoding="utf-8") as f:
            HEADER_PROMPT = f.read()
        prompt = f"{HEADER_PROMPT}\n\n请分析以下 Excel 表格片段，并识别出表格名称、表头和关键信息：\n---\n{content}\n---\n表头和关键信息是: "
        
        messages = [HumanMessage(content=prompt)]
        response = await llm_model.ainvoke(messages)
        
        # 修复AIMessage对象处理 - 提取content属性
        if hasattr(response, 'content'):
            return response.content
        else:
            return str(response)
    except Exception as e:
        return None

# 并发处理表头识别
async def identify_headers_concurrently(excel_path: str, sheet_names: List[str], llm_model, max_workers: int = 3):
    """并发处理多个表头识别"""
    results = {}
    
    # 使用信号量限制并发数
    semaphore = asyncio.Semaphore(max_workers)
    
    async def process_sheet(sheet_name):
        async with semaphore:
            return sheet_name, await identify_header_with_cache(excel_path, sheet_name, llm_model)
    
    # 创建并发任务
    tasks = [process_sheet(sheet_name) for sheet_name in sheet_names]
    
    # 等待所有任务完成
    completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 处理结果
    for result in completed_tasks:
        if isinstance(result, Exception):
            continue
        sheet_name, header = result
        if header:
            results[sheet_name] = header
    
    return results

def get_file_modification_time(file_path: str) -> float:
    """获取文件的修改时间戳"""
    try:
        return os.path.getmtime(file_path)
    except OSError:
        return 0.0

def save_vector_db_metadata(vector_db_dir: str, excel_files_info: Dict[str, float]):
    """保存向量数据库的元数据信息"""
    metadata_path = os.path.join(vector_db_dir, "vector_db_metadata.json")
    metadata = {
        "last_update": time.time(),
        "excel_files": excel_files_info
    }
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

def load_vector_db_metadata(vector_db_dir: str) -> Dict[str, Any]:
    """加载向量数据库的元数据信息"""
    metadata_path = os.path.join(vector_db_dir, "vector_db_metadata.json")
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def check_excel_files_changes(excel_dir: str, existing_metadata: Dict[str, Any]) -> Tuple[bool, Dict[str, float]]:
    """检查Excel文件是否有变化（新增、修改或删除）"""
    current_files = {}
    
    # 获取当前所有Excel文件的信息
    if os.path.exists(excel_dir):
        for filename in os.listdir(excel_dir):
            if filename.endswith(('.xlsx', '.xls')):
                file_path = os.path.join(excel_dir, filename)
                current_files[filename] = get_file_modification_time(file_path)
    
    # 获取之前记录的文件信息
    previous_files = existing_metadata.get('excel_files', {})
    
    # 检查是否有变化
    has_changes = False
    
    # 检查新增或修改的文件
    for filename, mod_time in current_files.items():
        if filename not in previous_files or previous_files[filename] != mod_time:
            has_changes = True
            print(f"检测到文件变化: {filename}")
            break
    
    # 检查删除的文件
    if not has_changes:
        for filename in previous_files:
            if filename not in current_files:
                has_changes = True
                print(f"检测到文件删除: {filename}")
                break
    
    return has_changes, current_files

async def create_and_store_vectors(excel_dir: str, llm_model, embedding_model, force_recreate: bool = False):
    """创建和存储向量数据库（集成了加载和创建功能，支持增量更新）"""
    # 修改全局配置
    VECTOR_DB_DIR = "Faiss"
    FAISS_INDEX_PATH = os.path.join(VECTOR_DB_DIR, "faiss_index.faiss")
    FAISS_INDEX_PKL_PATH = os.path.join(VECTOR_DB_DIR, "faiss_index.pkl")
    
    os.makedirs(VECTOR_DB_DIR, exist_ok=True)
    
    # 加载现有的元数据
    existing_metadata = load_vector_db_metadata(VECTOR_DB_DIR)
    
    # 检查Excel文件是否有变化
    has_changes, current_files_info = check_excel_files_changes(excel_dir, existing_metadata)
    
    # 如果不强制重新创建且向量数据库存在且没有文件变化，则直接加载
    if (not force_recreate and 
        os.path.exists(FAISS_INDEX_PATH) and 
        os.path.exists(FAISS_INDEX_PKL_PATH) and 
        not has_changes):
        try:
            print("向量数据库已存在且Excel文件无变化，直接加载现有数据库")
            return FAISS.load_local(VECTOR_DB_DIR, embedding_model, index_name="faiss_index", allow_dangerous_deserialization=True)
        except Exception as e:
            print(f"加载现有向量数据库失败: {e}，将重新创建")
            try:
                if os.path.exists(FAISS_INDEX_PATH):
                    os.remove(FAISS_INDEX_PATH)
                if os.path.exists(FAISS_INDEX_PKL_PATH):
                    os.remove(FAISS_INDEX_PKL_PATH)
            except Exception:
                pass
    
    # 检查是否需要重新创建向量数据库
    need_recreate = (
        has_changes or 
        force_recreate or 
        not os.path.exists(FAISS_INDEX_PATH) or 
        not os.path.exists(FAISS_INDEX_PKL_PATH)
    )
    
    if need_recreate:
        if has_changes:
            print("检测到Excel文件变化，正在更新向量数据库...")
        elif force_recreate:
            print("强制重新创建向量数据库...")
        else:
            print("向量数据库文件不存在，正在创建向量数据库...")
    else:
        # 如果不需要重新创建，直接返回None（这种情况不应该发生，因为前面已经处理了加载逻辑）
        print("向量数据库无需重新创建")
        return None
    
    # 创建新的向量数据库
    all_documents = []
    
    # 处理目录中的所有Excel文件
    for filename in os.listdir(excel_dir):
        if filename.endswith(('.xlsx', '.xls')):
            excel_path = os.path.join(excel_dir, filename)
            print(f"处理Excel文件: {excel_path}")
            
            try:
                excel_file = pd.ExcelFile(excel_path)
                sheet_names = excel_file.sheet_names
                
                # 并发处理表头识别
                headers_results = await identify_headers_concurrently(excel_path, sheet_names, llm_model)
                
                for sheet_name in sheet_names:
                    header = headers_results.get(sheet_name)
                    if header:
                        sheet_header_mapping = f"Sheet名称: {sheet_name}, 表头: {header}"
                        text_to_embed = f"{os.path.basename(excel_path)}-{sheet_header_mapping}"
                        
                        doc = Document(
                            page_content=text_to_embed,
                            metadata={
                                "excel_name": os.path.basename(excel_path),
                                "sheet_name": sheet_name,
                                "header": header,
                                "mapping_text": sheet_header_mapping
                            }
                        )
                        all_documents.append(doc)
                        
            except Exception as e:
                print(f"处理文件 {excel_path} 时出错: {e}")
                continue
    
    # 创建向量数据库
    if all_documents:
        vectorstore = FAISS.from_documents(all_documents, embedding_model)
        vectorstore.save_local(VECTOR_DB_DIR, index_name="faiss_index")
        print(f"成功创建向量数据库，包含 {len(all_documents)} 个文档")
        
        # 保存元数据信息
        save_vector_db_metadata(VECTOR_DB_DIR, current_files_info)
        print("已保存向量数据库元数据信息")
        
        return vectorstore
    else:
        print("未找到有效文档，创建空的向量数据库")
        dummy_doc = Document(page_content="dummy", metadata={})
        vectorstore = FAISS.from_documents([dummy_doc], embedding_model)
        vectorstore.save_local(VECTOR_DB_DIR, index_name="faiss_index")
        
        # 即使是空数据库也要保存元数据
        save_vector_db_metadata(VECTOR_DB_DIR, current_files_info)
        
        return vectorstore
# ============================================================================
# --- LangGraph 工作流部分 ---
# ============================================================================

from langgraph.graph import StateGraph, END
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

class GraphState(TypedDict):
    """LangGraph工作流状态定义"""
    query: str
    excel_path: str
    db_path: str
    table_mapping: Dict[str, str]
    vectorstore: FAISS
    relevant_sheets: List[Tuple[str, str]]
    reranked_sheets: List[Tuple[str, str]]
    sql_query: str
    db_results: Any
    response: str

def get_relevant_sheets(state: GraphState):
    """从向量数据库中检索与查询最相关的Excel Sheets"""
    query = state['query']
    vectorstore = state['vectorstore']
    
    results = vectorstore.similarity_search_with_score(query, k=5)
    
    print(f"\n🔍 [SIMILARITY DEBUG] 向量检索结果 (查询: {query})")
    relevant_sheets = []
    seen_sheets = set()  # 用于去重
    
    for i, (doc, score) in enumerate(results):
        excel_name = doc.metadata.get('excel_name', '')
        sheet_name = doc.metadata.get('sheet_name', '')
        header_info = doc.metadata.get('header', '')
        
        print(f"  第{i+1}名: Excel={excel_name}, Sheet={sheet_name}, 相似度={score:.4f}")
        print(f"         表头信息: {header_info[:100]}..." if len(header_info) > 100 else f"         表头信息: {header_info}")
        
        if excel_name and sheet_name:
            # 创建唯一标识符进行去重
            sheet_key = (excel_name, sheet_name)
            if sheet_key not in seen_sheets:
                relevant_sheets.append((excel_name, sheet_name))
                seen_sheets.add(sheet_key)
                print(f"         ✅ 已添加到候选列表")
            else:
                print(f"         ⚠️ 重复sheet，已跳过")
    
    print(f"\n📋 [SIMILARITY DEBUG] 最终候选sheets数量: {len(relevant_sheets)}")
    for i, (excel_name, sheet_name) in enumerate(relevant_sheets):
        print(f"  候选{i+1}: {excel_name} - {sheet_name}")
    
    return {"relevant_sheets": relevant_sheets}

def rerank_sheets(state: GraphState):
    """使用 rerank 模型对召回的 Excel Sheets 进行重排序"""
    query = state['query']
    reranker = model_manager.get_reranker()
    
    print(f"\n🔄 [RERANK DEBUG] 开始重排序 (候选数量: {len(state['relevant_sheets'])})")
    
    if len(state['relevant_sheets']) <= 2:
        print(f"📝 [RERANK DEBUG] 候选数量≤2，跳过重排序")
        state['reranked_sheets'] = state['relevant_sheets']
    else:
        pairs = []
        
        print(f"🔍 [RERANK DEBUG] 构建重排序对比文本:")
        for i, (excel_name, sheet_name) in enumerate(state['relevant_sheets']):
            results = state['vectorstore'].similarity_search(
                f"{excel_name}-{sheet_name}", k=1
            )
            if results:
                header_info = results[0].metadata.get('mapping_text', '')
                pairs.append((query, header_info))
                print(f"  对比{i+1}: {excel_name}-{sheet_name}")
                print(f"         映射文本: {header_info}")
            else:
                pairs.append((query, f"{excel_name}-{sheet_name}"))
                print(f"  对比{i+1}: {excel_name}-{sheet_name} (未找到映射文本)")
        
        print(f"\n🧮 [RERANK DEBUG] 计算重排序分数...")
        scores = reranker.compute_score(pairs)
        
        print(f"📊 [RERANK DEBUG] 重排序分数结果:")
        for i, ((excel_name, sheet_name), score) in enumerate(zip(state['relevant_sheets'], scores)):
            print(f"  第{i+1}名: {excel_name}-{sheet_name}, 重排序分数={score:.4f}")
        
        ranked_results = sorted(
            zip(state['relevant_sheets'], scores), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        print(f"\n🏆 [RERANK DEBUG] 重排序后的最终排名:")
        for i, ((excel_name, sheet_name), score) in enumerate(ranked_results):
            print(f"  排名{i+1}: {excel_name}-{sheet_name}, 分数={score:.4f}")
        
        state['reranked_sheets'] = [item[0] for item in ranked_results[:3]]
        
        print(f"\n✅ [RERANK DEBUG] 最终选择的前3名sheets:")
        for i, (excel_name, sheet_name) in enumerate(state['reranked_sheets']):
            print(f"  选择{i+1}: {excel_name}-{sheet_name}")
    
    return {"reranked_sheets": state['reranked_sheets']}

async def generate_sql(state: GraphState):
    """根据重排序的sheets和用户问题生成SQL查询（支持方案1和方案2，包含列名业务含义映射）"""
    query = state['query']
    reranked_sheets = state['reranked_sheets']
    llm = model_manager.get_llm()
    
    # 导入database_manager并获取实例
    from database_manager import DatabaseManager
    db_manager = DatabaseManager()
    
    schema_info = []
    table_names = []
    column_mappings_text = ""
    
    # 方案1：尝试使用增强映射（如果存在的话）
    try:
        enhanced_mapping = db_manager.get_enhanced_table_mapping()
        print(f"\n🔧 [SQL DEBUG] 获取到增强映射: {len(enhanced_mapping)} 条记录")
    except AttributeError:
        enhanced_mapping = {}
        print(f"\n🔧 [SQL DEBUG] 增强映射方法不存在，使用方案2")
    
    # 读取映射注册表
    mapping_registry_path = os.path.join("column_mapping_docs", "mapping_registry.json")
    mapping_registry = {}
    try:
        with open(mapping_registry_path, 'r', encoding='utf-8') as f:
            mapping_registry = json.load(f)
        print(f"📋 [SQL DEBUG] 成功加载映射注册表，包含 {len(mapping_registry)} 个表的映射配置")
    except Exception as e:
        print(f"⚠️ [SQL DEBUG] 加载映射注册表失败: {e}")
    
    # 遍历重排序的sheets
    for excel_name, sheet_name in reranked_sheets:
        print(f"\n🔍 [SQL DEBUG] 处理 Excel: {excel_name}, Sheet: {sheet_name}")
        
        table_name = None
        
        # 方案1：优先使用增强映射
        if (excel_name, sheet_name) in enhanced_mapping:
            table_name = enhanced_mapping[(excel_name, sheet_name)]
            print(f"✅ [SQL DEBUG] 方案1成功: ({excel_name}, {sheet_name}) -> {table_name}")
        else:
            # 方案2：回退到动态获取映射
            print(f"🔄 [SQL DEBUG] 方案1未找到，使用方案2动态获取")
            try:
                excel_path = os.path.join("uploads", excel_name)
                file_table_mapping = db_manager.get_table_mapping(excel_path)
                print(f"📋 [SQL DEBUG] 动态获取的表映射: {file_table_mapping}")
                
                if sheet_name in file_table_mapping:
                    table_name = file_table_mapping[sheet_name]
                    print(f"✅ [SQL DEBUG] 方案2成功: {sheet_name} -> {table_name}")
                else:
                    print(f"❌ [SQL DEBUG] 方案2失败: {sheet_name} 不在 {list(file_table_mapping.keys())}")
            except Exception as e:
                print(f"❌ [SQL DEBUG] 方案2异常: {e}")
        
        if table_name:
            table_names.append(table_name)
            
            try:
                conn = sqlite3.connect(db_manager.db_path)
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                conn.close()
                
                column_names = [col[1] for col in columns]
                column_names_display = ', '.join(column_names)
                schema_info.append(f"表名: {table_name} (来源: {excel_name}-{sheet_name}), 列名: {column_names_display}")
                
                # 获取列名业务含义映射
                if table_name in mapping_registry:
                    config_path = mapping_registry[table_name]['config_path']
                    full_config_path = os.path.join("column_mapping_docs", os.path.basename(config_path))
                    
                    try:
                        with open(full_config_path, 'r', encoding='utf-8') as f:
                            column_config = json.load(f)
                        
                        column_mappings = column_config.get('column_mappings', {})
                        if column_mappings:
                            column_mappings_text += f"\n\n表 {table_name} 的列名业务含义映射:\n"
                            for db_col, business_meaning in column_mappings.items():
                                column_mappings_text += f"  - {db_col} → {business_meaning}\n"
                            print(f"📋 [SQL DEBUG] 成功加载表 {table_name} 的列名映射配置")
                        else:
                            print(f"⚠️ [SQL DEBUG] 表 {table_name} 的映射配置为空")
                            
                    except Exception as e:
                        print(f"⚠️ [SQL DEBUG] 加载表 {table_name} 的列名映射配置失败: {e}")
                else:
                    print(f"⚠️ [SQL DEBUG] 未找到表 {table_name} 的列名映射配置")
                
                print(f"✅ [SQL映射] 成功映射: {excel_name}-{sheet_name} -> {table_name}")
            except Exception as e:
                print(f"❌ [SQL映射] 获取表结构失败 {table_name}: {e}")
        else:
            print(f"⚠️ [SQL映射] 未找到映射: {excel_name}-{sheet_name}")
    
    schema_text = "\n".join(schema_info)
    
    # 构建完整的映射说明
    mapping_instruction = ""
    if column_mappings_text:
        mapping_instruction = f"\n\n列名业务含义映射:{column_mappings_text}"
    
    # 构建多表查询指导
    multi_table_instruction = ""
    if len(table_names) > 1:
        table_list = "、".join(table_names)
        multi_table_instruction = f"\n\n🔍 多表查询要求：\n- 当前召回了 {len(table_names)} 个相关表：{table_list}\n- 必须从所有相关表中查询数据，使用 UNION ALL 合并结果\n- 每个表都应该包含相同的查询条件和列选择\n- 确保查询覆盖所有可能包含目标数据的表"
    
    sql_prompt = f"""根据以下数据库表结构和用户问题，生成相应的SQL查询语句。

数据库表结构：
{schema_text}{mapping_instruction}

用户问题：{query}

重要提示：
1. 请仔细理解用户问题中提到的业务术语，并根据列名映射找到对应的数据库列名
2. 在WHERE条件中，对于文本匹配请使用LIKE操作符和通配符%，而不是精确匹配（=）
3. 例如：如果用户问题涉及"产品名称"相关内容，应该使用对应的数据库列名如 WHERE `Unnamed: 1` LIKE '%LED纳米模块灯%'
4. 在SQL查询中必须使用数据库列名（如Unnamed: 1），而不是业务含义名称
5. 根据用户问题的语义，智能选择需要查询的列和过滤条件
6. 如果提供了列名业务含义映射，请根据映射关系将用户问题中的业务术语转换为对应的数据库列名
7. ⚠️ 关键要求：如果系统召回了多个表，请为每个表生成一个独立的SQL查询语句。
8. 单表查询示例格式：
   SELECT * FROM table1 WHERE condition;
   SELECT * FROM table2 WHERE condition;
   SELECT * FROM table3 WHERE condition;

请生成SQL查询语句（只返回SQL语句，不要其他解释）："""
    
    messages = [HumanMessage(content=sql_prompt)]
    response = await llm.ainvoke(messages)
    
    sql_query = str(response.content).strip()
    
    if sql_query.startswith('```sql'):
        sql_query = sql_query[6:]
    if sql_query.startswith('```'):
        sql_query = sql_query[3:]
    if sql_query.endswith('```'):
        sql_query = sql_query[:-3]
    
    sql_query = sql_query.strip()
    
    # 调试输出：生成的SQL语句
    print(f"\n🔧 [SQL DEBUG] 生成的SQL查询语句:")
    print(f"📝 [SQL DEBUG] {sql_query}")
    print(f"🎯 [SQL DEBUG] 查询目标表: {', '.join(table_names)}")
    print(f"🔗 [SQL DEBUG] 列名映射信息: {column_mappings_text}")
    
    return {"sql_query": sql_query}

def execute_sql(state: GraphState):
    """执行生成的SQL查询并返回结果
    
    支持执行多条独立的SQL语句，每个查询结果以JSON格式独立输出
    """
    import json
    
    sql_query = state['sql_query']
    db_path = state['db_path']
    
    print(f"\n⚡ [SQL DEBUG] 开始执行SQL查询")
    print(f"🗄️ [SQL DEBUG] 数据库路径: {db_path}")
    print(f"📝 [SQL DEBUG] 执行的SQL: {sql_query}")
    
    # 分割多条SQL语句（以分号分隔）
    sql_statements = [stmt.strip() for stmt in sql_query.split(';') if stmt.strip()]
    
    if not sql_statements:
        print(f"❌ [SQL DEBUG] 没有找到有效的SQL语句")
        return {"db_results": {"query_results": []}}
    
    print(f"🔢 [SQL DEBUG] 检测到 {len(sql_statements)} 条SQL语句")
    
    query_results = []
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        for i, sql_stmt in enumerate(sql_statements, 1):
            print(f"\n🔍 [SQL DEBUG] 执行第 {i} 条SQL: {sql_stmt[:100]}...")
            
            try:
                cursor.execute(sql_stmt)
                results = cursor.fetchall()
                columns = [description[0] for description in cursor.description] if cursor.description else []
                
                print(f"✅ [SQL DEBUG] 第 {i} 条SQL执行成功")
                print(f"📊 [SQL DEBUG] 返回列数: {len(columns)}")
                print(f"📈 [SQL DEBUG] 返回行数: {len(results)}")
                
                if columns:
                    print(f"🏷️ [SQL DEBUG] 列名: {', '.join(columns)}")
                
                # 构建当前查询的JSON结果
                current_query_result = {
                    "sql_index": i,
                    "sql_statement": sql_stmt,
                    "columns": columns,
                    "data": []
                }
                
                if results:
                    # 将每行结果转换为JSON格式
                    for row in results:
                        row_dict = {}
                        for j, value in enumerate(row):
                            if j < len(columns):
                                row_dict[columns[j]] = str(value) if value is not None else None
                        current_query_result["data"].append(row_dict)
                    
                    # 显示查询结果预览
                    if len(results) <= 3:
                        print(f"📋 [SQL DEBUG] 第 {i} 条查询结果:")
                        for j, row_dict in enumerate(current_query_result["data"]):
                            print(f"   行{j+1}: {json.dumps(row_dict, ensure_ascii=False)}")
                    else:
                        print(f"📋 [SQL DEBUG] 第 {i} 条查询结果（前3行）:")
                        for j, row_dict in enumerate(current_query_result["data"][:3]):
                            print(f"   行{j+1}: {json.dumps(row_dict, ensure_ascii=False)}")
                else:
                    print(f"📋 [SQL DEBUG] 第 {i} 条SQL无查询结果")
                
                # 添加到总结果中
                query_results.append(current_query_result)
                    
            except Exception as e:
                print(f"❌ [SQL DEBUG] 第 {i} 条SQL执行失败: {str(e)}")
                # 即使失败也添加错误信息到结果中
                error_result = {
                    "sql_index": i,
                    "sql_statement": sql_stmt,
                    "error": str(e),
                    "columns": [],
                    "data": []
                }
                query_results.append(error_result)
                continue
        
        conn.close()
        
        print(f"\n🎯 [SQL DEBUG] 所有SQL执行完成")
        print(f"📊 [SQL DEBUG] 总查询数: {len(query_results)}")
        
        # 统计总结果数
        total_data_count = sum(len(result["data"]) for result in query_results)
        print(f"📈 [SQL DEBUG] 总数据行数: {total_data_count}")
        
        # 输出最终的JSON格式结果
        final_result = {"db_results": query_results}
        print(f"\n📋 [SQL DEBUG] 最终JSON结果:")
        print(json.dumps(final_result, ensure_ascii=False, indent=2))
        
        return final_result
        
    except Exception as e:
        print(f"❌ [SQL DEBUG] 数据库连接失败: {str(e)}")
        return {"db_results": {"error": str(e), "query_results": []}}

async def generate_answer(state: GraphState):
    """根据查询结果生成最终的自然语言答案
    
    处理新的JSON格式查询结果
    """
    import json
    
    query = state['query']
    db_results = state['db_results']
    llm = model_manager.get_llm()
    
    print(f"\n📋 [DEBUG] 开始生成答案")
    print(f"🔍 [DEBUG] 查询问题: {query}")
    
    # 检查是否有查询结果
    is_empty = all(not res['data'] for res in db_results)
    if is_empty:
        final_answer = "抱歉，查询执行失败，无法回答您的问题。"
        print(f"❌ [DEBUG] 查询执行失败，返回错误答案") 
        
    else:
        answer_prompt = f"""根据以下数据库查询结果，回答用户的问题。                             

用户问题：{query}

查询结果{db_results}

该查询结果是一个严格json文档，键"data"对应的值为准确答案，部分键"data"中的值为空，不用理会。
请根据查询结果，用自然语言回答用户的问题。如果有多个查询结果，请综合所有结果进行回答："""
            
        print(f"🤖 [DEBUG] 调用LLM生成答案")
        messages = [HumanMessage(content=answer_prompt)]
        response = await llm.ainvoke(messages)
        final_answer = str(response.content)            
        print(f"✅ [DEBUG] 答案生成完成final_answer: {final_answer[:100]}...")    
    return {"response": final_answer}

# 构建LangGraph
builder = StateGraph(GraphState)

builder.add_node("get_relevant", get_relevant_sheets)
builder.add_node("rerank", rerank_sheets)
builder.add_node("generate_sql", generate_sql)
builder.add_node("execute_sql", execute_sql)
builder.add_node("generate_answer", generate_answer)

builder.set_entry_point("get_relevant")

builder.add_edge("get_relevant", "rerank")
builder.add_edge("rerank", "generate_sql")
builder.add_edge("generate_sql", "execute_sql")
builder.add_edge("execute_sql", "generate_answer")
builder.add_edge("generate_answer", END)

graph = builder.compile()

async def run_flow(query: str, excel_path: str, db_path: str):
    """优化的主流程"""
    print(f"\n🚀 [DEBUG] 开始处理查询流程")
    print(f"📝 [DEBUG] 查询内容: {query}")
    print(f"📁 [DEBUG] Excel文件: {excel_path}")
    print(f"🗄️ [DEBUG] 数据库路径: {db_path}")
    print(f"🔧 [DEBUG] run_flow函数已启动，版本: 2024-01-15")
    
    # 1. 使用缓存的Excel到SQLite转换
    print(f"\n⚡ [DEBUG] 步骤1: Excel到SQLite转换")
    db_path, table_mapping = excel_to_sqlite(excel_path, db_path)
    print(f"📊 [DEBUG] 表映射: {table_mapping}")
    print(f"💾 [DEBUG] 缓存数据库路径: {db_path}")

    # 2. 懒加载向量数据库
    print(f"\n🧠 [DEBUG] 步骤2: 加载模型和向量数据库")
    llm = model_manager.get_llm()
    embedding_model = model_manager.get_embedding_model()
    vectorstore = await create_and_store_vectors(EXCEL_DIR, llm, embedding_model)
    print(f"✅ [DEBUG] 模型和向量数据库加载完成")

    # 3. 运行LangGraph
    print(f"\n🔄 [DEBUG] 步骤3: 执行LangGraph工作流")
    inputs = {
        "query": query,
        "excel_path": excel_path,
        "db_path": db_path,
        "table_mapping": table_mapping,
        "vectorstore": vectorstore,

    }
    result = await graph.ainvoke(inputs)
    print(f"🎯 [DEBUG] LangGraph执行完成")
    
    # 4. 构建MCP响应
    print(f"\n📋 [DEBUG] 步骤4: 构建MCP响应")
    db_results = result.get('db_results', {'db_results': []})
    
    # 检查是否有查询结果数据
    is_empty = all(not res['data'] for res in db_results)
    
    # 获取LangGraph生成的最终答案
    final_answer = result.get("response", '')
    print(f"🔍 [DEBUG] LangGraph返回的response: {final_answer}")
    
    # 如果LangGraph没有生成答案，则根据数据情况生成默认答案
    if not final_answer:
        if not is_empty:
            final_answer = "查询成功，已找到相关数据"
        else:
            final_answer = "未找到相关数据"
        print(f"🔄 [DEBUG] 使用默认答案: {final_answer}")
    else:
        print(f"✅ [DEBUG] 使用LangGraph生成的答案: {final_answer[:100]}...")
    
    print(f"💬 [DEBUG] 最终答案: {final_answer}")
    
    mcp_response = {
        "query": result.get('query', ''),
        "answer": final_answer
    }
    
    print(f"✅ [DEBUG] MCP响应构建完成")
    
    return mcp_response

def format_mcp_output(mcp_response: dict) -> str:
    """格式化MCP输出为友好的JSON字符串"""
    return json.dumps(mcp_response, ensure_ascii=False, indent=2)

def main(query: str = None):
    """主函数，支持传入查询参数"""
    db_file = "database.db"
    
    # 初始化数据库管理器并检查所有Excel文件
    db_manager = get_database_manager()
    db_manager.check_all_files(EXCEL_DIR)
    
    # 初始化列名映射生成器并检查映射配置
    from column_mapping_generator import get_column_mapping_generator
    try:
        print("🔧 初始化列名映射生成器...")
        column_mapping_generator = get_column_mapping_generator()
        print("✅ 列名映射生成器初始化完成")
    except Exception as e:
        print(f"⚠️ 列名映射生成器初始化失败: {e}")
        print("系统将继续运行，但可能影响查询准确性")
    
    excel_files = [os.path.join(EXCEL_DIR, f) for f in os.listdir(EXCEL_DIR) if f.endswith(('.xlsx', '.xls'))]
    if not excel_files:
        print(f"{EXCEL_DIR} 目录下未找到 Excel 文件，请先上传！")
        return None
    
    if not query:
        query = "定制LED景观灯01的工程量是多少？总价是多少？"
    
    excel_file = excel_files[0]
    mcp_response = asyncio.run(run_flow(query, excel_file, db_file))
    
    return mcp_response

# 服务预热函数
def warm_up_service():
    """预热服务，提前加载模型"""
    try:
        # 预加载模型
        model_manager.get_embedding_model()
        print("✅ 嵌入模型预热完成")
        
        model_manager.get_reranker()
        print("✅ 重排序模型预热完成")
        
        # LLM采用懒加载，在首次查询时加载
        print("✅ 服务预热完成")
        
    except Exception as e:
        print(f"⚠️ 服务预热失败: {e}")

if __name__ == "__main__":
    # 服务启动时预热
    warm_up_service()
    
    # 当直接运行时，使用默认查询
    result = main()
    if result:
        print("\n" + "="*50)
        print("📤 最终MCP响应:")
        print("="*50)
        print(format_mcp_output(result))