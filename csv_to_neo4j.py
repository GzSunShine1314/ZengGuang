from neo4j import GraphDatabase
import csv

# 连接配置
URI = "bolt://47.107.128.88:7687"
AUTH = ("neo4j", "neo4j_matrix")
BATCH_SIZE = 100  # 批量提交大小（每100行数据提交一次，可根据数据量调整）

def import_csv_to_neo4j():
    file_path = "C:/Users/ZengGuang/Desktop/tables/pd_production_log_silver_mes.xlsx.csv"
    batch_data = []  # 存储批量数据
    
    # 创建驱动（复用连接，减少连接开销）
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        try:
            # 首先检查CSV文件的列名
            with open(file_path, 'r', encoding='utf-8-sig') as f:  # 使用utf-8-sig自动处理BOM
                reader = csv.DictReader(f)
                # 获取实际的列名
                actual_columns = reader.fieldnames
                print(f"CSV文件中的实际列名: {actual_columns}")
                
                # 检查必需的列是否存在（考虑可能的BOM字符）
                required_columns = ['sfc_no', 'processing_status', 'process_step', 'process_step_desc', 
                                  'resource_code','resource_desc','weight','created_at','default_unit']
                
                # 创建列名映射，处理BOM字符
                column_mapping = {}
                for col in actual_columns:
                    clean_col = col.replace('/ufeff', '')  # 移除BOM字符
                    column_mapping[clean_col] = col
                
                print(f"清理后的列名映射: {column_mapping}")
                
                missing_columns = [col for col in required_columns if col not in column_mapping]
                if missing_columns:
                    print(f"警告：以下列名在CSV中不存在: {missing_columns}")
                    print("请检查CSV文件的列名是否与代码中的期望列名匹配")
                    return
                
                # 重新打开文件进行数据读取
                f.seek(0)
                reader = csv.DictReader(f)
                row_count = 0  # 计数，方便查看进度
                
                for row in reader:
                    # 1. 收集单条数据（转为字典，对应Cypher参数）
                    # 使用映射后的列名获取数据
                    data = {
                        "sfc_no": row.get(column_mapping['sfc_no'], ''),
                        "processing_status": row.get(column_mapping['processing_status'], ''),
                        "process_step": row.get(column_mapping['process_step'], ''),
                        "process_step_desc": row.get(column_mapping['process_step_desc'], ''),
                        "resource_code": row.get(column_mapping['resource_code'], ''),
                        "resource_desc": row.get(column_mapping['resource_desc'], ''),
                        "weight": row.get(column_mapping['weight'], ''),
                        "created_at": row.get(column_mapping['created_at'], ''),
                        "default_unit": row.get(column_mapping['default_unit'], ''),
                    }
                    
                    # 过滤掉空行或无效数据
                    if any(data.values()):  # 如果至少有一个非空值
                        batch_data.append(data)
                        row_count += 1
                        
                        # 2. 达到批量大小，执行一次批量写入
                        if len(batch_data) >= BATCH_SIZE:
                            # 批量CREATE：用UNWIND展开列表，一次插入多条数据
                            driver.execute_query("""
                                UNWIND $batch_data AS item
                                CREATE (mppls:mes_pd_production_log_silver{
                                    sfc_no: item.sfc_no,
                                    processing_status: item.processing_status,
                                    process_step: item.process_step,
                                    process_step_desc: item.process_step_desc,
                                    resource_code: item.resource_code,
                                    resource_desc: item.resource_desc,
                                    weight: item.weight,
                                    created_at: item.created_at,
                                    default_unit: item.default_unit
                                })
                            """, batch_data=batch_data)  # 传入批量数据列表
                            
                            print(f"已导入 {row_count} 行数据")  # 打印进度，确认代码在运行
                            batch_data.clear()  # 清空批量列表，准备下一批
                
                # 3. 处理剩余不足批量大小的数据
                if batch_data:
                    driver.execute_query("""
                        UNWIND $batch_data AS item
                        CREATE (mppls:mes_pd_production_log_silver{
                                    sfc_no: item.sfc_no,
                                    processing_status: item.processing_status,
                                    process_step: item.process_step,
                                    process_step_desc: item.process_step_desc,
                                    resource_code: item.resource_code,
                                    resource_desc: item.resource_desc,
                                    weight: item.weight,
                                    created_at: item.created_at,
                                    default_unit: item.default_unit
                        })
                    """, batch_data=batch_data)
                    print(f"最终导入：共 {row_count} 行数据")
                    
        except FileNotFoundError:
            print(f"错误：找不到文件 {file_path}")
            print("请检查文件路径是否正确")
        except UnicodeDecodeError:
            print("错误：文件编码问题，尝试使用不同的编码")
            # 尝试其他编码
            try:
                with open(file_path, 'r', encoding='gbk') as f:
                    reader = csv.DictReader(f)
                    actual_columns = reader.fieldnames
                    print(f"使用GBK编码成功，列名: {actual_columns}")
            except Exception as e:
                print(f"编码问题无法解决: {e}")
        except Exception as e:
            print(f"导入过程中发生错误: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    import_csv_to_neo4j()
    print("CSV数据已成功导入Neo4j!")