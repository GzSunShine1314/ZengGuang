# 需安装依赖：pip install neo4j mysql-connector-python

from neo4j import GraphDatabase
import pymysql
from pymysql import Error
import csv # Added for CSV export/import
import time

# 1. 连接源数据库（以MySQL为例）
try:
    mysql_conn = pymysql.connect(
        host="115.190.113.11",
        port=9030,
        user="root",
        password="matrix2025",
        database="matrix_silver",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    if mysql_conn.open:
        print("成功连接到MySQL服务器")
        mysql_cursor = mysql_conn.cursor()

except Error as e:
    print(f"MySQL连接错误: {e}")
    print(f"错误代码: {e.errno if hasattr(e, 'errno') else '未知'}")
    print(f"SQL状态: {e.sqlstate if hasattr(e, 'sqlstate') else '未知'}")
    exit(1)
except Exception as e:
    print(f"未知错误: {e}")
    print(f"错误类型: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    exit(1)

# 2. 连接Neo4j
try:
    neo4j_driver = GraphDatabase.driver(
        "bolt://47.107.128.88:7687",
        auth=("neo4j", "neo4j_matrix")
    )
    
    # 测试连接
    with neo4j_driver.session() as session:
        result = session.run("RETURN 1")
        print("成功连接到Neo4j")

except Exception as e:
    print(f"Neo4j连接错误: {e}")
    exit(1)

# 3. 读取MySQL表数据并导入Neo4j
def import_data():
    try:
        print("=== 开始数据导入流程 ===")
        
        # 先获取总记录数，用于显示进度
        mysql_cursor.execute("SELECT COUNT(*) AS total FROM reference")
        total_count = mysql_cursor.fetchone()['total']
        print(f"数据总量: {total_count:,} 条记录")
        
        # 读取MySQL的reference表
        mysql_cursor.execute("SELECT * FROM reference")
        
        # 批量处理
        batch_size = 1000  # 每批1000条
        batch_data = []
        processed_count = 0
        start_time = time.time()
        
        print(f"批量大小: {batch_size} 条/批")
        print("=" * 50)
        
        with neo4j_driver.session() as session:
            for i, record in enumerate(mysql_cursor, 1):
                batch_data.append({
                    'handle': record['handle'],
                    'from_node_type': record['from_node_type'],
                    'from_node': record['from_node'],
                    'from_node_time': record['from_node_time'],
                    'to_node_type': record['to_node_type'],
                    'to_node': record['to_node'],
                    'to_node_time': record['to_node_time'],
                    'description': record['description']
                })
                
                # 每1000条或最后一批时执行批量插入
                if len(batch_data) >= batch_size or i == total_count:
                    batch_start_time = time.time()
                    
                    try:
                        # 使用 UNWIND 进行批量插入
                        result = session.run("""
                        UNWIND $batch AS row
                        CREATE (r:Reference {
                            handle: row.handle,
                            from_node_type: row.from_node_type,
                            from_node: row.from_node,
                            to_node_type: row.to_node_type,
                            to_node: row.to_node,
                            description: row.description
                        })
                        """, batch=batch_data)
                        
                        # 确保查询执行完成
                        result.consume()
                        
                        processed_count += len(batch_data)
                        batch_end_time = time.time()
                        batch_duration = batch_end_time - batch_start_time
                        
                        # 计算进度和速度
                        progress = (processed_count / total_count) * 100
                        elapsed_time = time.time() - start_time
                        avg_speed = processed_count / elapsed_time if elapsed_time > 0 else 0
                        remaining_count = total_count - processed_count
                        eta = remaining_count / avg_speed if avg_speed > 0 else 0
                        
                        # 显示详细进度信息
                        print(f"[{processed_count:,}/{total_count:,}] 批次导入完成")
                        print(f"  进度: {progress:.2f}%")
                        print(f"  本批次耗时: {batch_duration:.2f}秒")
                        print(f"  平均速度: {avg_speed:.1f} 条/秒")
                        print(f"  预计剩余时间: {eta/60:.1f} 分钟")
                        print(f"  已用时间: {elapsed_time/60:.1f} 分钟")
                        print("-" * 30)
                        
                        batch_data = []  # 清空批次
                        
                        # 每处理5000条记录后暂停一下，避免服务器压力过大
                        if processed_count % 5000 == 0:
                            print(f"已处理 {processed_count:,} 条记录，暂停2秒...")
                            time.sleep(2)
                            print("继续处理...")
                            print("-" * 30)
                            
                    except Exception as e:
                        print(f"批次导入失败: {e}")
                        print(f"  失败批次大小: {len(batch_data)} 条")
                        print(f"  当前进度: {processed_count:,}/{total_count:,}")
                        # 继续处理下一批，不中断整个流程
                        batch_data = []
                        continue
        
        # 最终统计
        total_time = time.time() - start_time
        final_speed = total_count / total_time if total_time > 0 else 0
        
        print("=" * 50)
        print("数据导入完成")
        print(f"总记录数: {total_count:,} 条")
        print(f"成功导入: {processed_count:,} 条")
        print(f"总耗时: {total_time/60:.2f} 分钟")
        print(f"平均速度: {final_speed:.1f} 条/秒")
        print("=" * 50)
        
    except Error as e:
        print(f"MySQL操作错误: {e}")
    except Exception as e:
        print(f"Neo4j操作错误: {e}")
        import traceback
        traceback.print_exc()

def export_to_csv():
    """导出数据到CSV文件，然后使用LOAD CSV导入"""
    try:
        print("=== 开始CSV导出和导入流程 ===")
        
        # 获取总记录数
        mysql_cursor.execute("SELECT COUNT(*) AS total FROM reference")
        total_count = mysql_cursor.fetchone()['total']
        print(f"数据总量: {total_count:,} 条记录")
        
        # 导出到CSV
        print("正在导出数据到CSV文件...")
        mysql_cursor.execute("SELECT * FROM reference")
        
        csv_start_time = time.time()
        with open('reference_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['handle', 'from_node_type', 'from_node', 'from_node_time', 'to_node_type', 'to_node', 'to_node_time', 'description']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for i, record in enumerate(mysql_cursor, 1):
                writer.writerow(record)
        
                # 显示CSV导出进度
                if i % 10000 == 0 or i == total_count:
                    progress = (i / total_count) * 100
                    print(f"CSV导出进度: [{i:,}/{total_count:,}] {progress:.2f}%")
        
        csv_time = time.time() - csv_start_time
        print(f"✓ CSV文件导出完成，耗时: {csv_time:.2f}秒")
        
        # 使用LOAD CSV导入Neo4j
        print("正在使用LOAD CSV导入Neo4j...")
        neo4j_start_time = time.time()
        
        with neo4j_driver.session() as session:
            result = session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///reference_data.csv' AS row
                CREATE (r:Reference {
                    handle: row.handle,
                    from_node_type: row.from_node_type,
                    from_node: row.from_node,
                    from_node_time: row.from_node_time,
                    to_node_type: row.to_node_type,
                    to_node: row.to_node,
                    to_node_time: row.to_node_time,
                    description: row.description
                })
            """)
        
            # 获取导入结果
            summary = result.consume()
            nodes_created = summary.counters.nodes_created
            
        neo4j_time = time.time() - neo4j_start_time
        total_time = time.time() - csv_start_time
        
        print("=" * 50)
        print("CSV导出和导入完成")
        print(f"CSV导出耗时: {csv_time:.2f}秒")
        print(f"Neo4j导入耗时: {neo4j_time:.2f}秒")
        print(f"总耗时: {total_time:.2f}秒")
        print(f"成功导入节点: {nodes_created:,} 个")
        print("=" * 50)
        
    except Exception as e:
        print(f"导出/导入错误: {e}")
        import traceback
        traceback.print_exc()

def import_from_csv():
    """从已存在的CSV文件导入Neo4j"""
    try:
        print("=== 从CSV文件导入Neo4j ===")
        
        # 检查CSV文件是否存在
        import os
        csv_file = 'reference_data.csv'
        if not os.path.exists(csv_file):
            print(f"错误：找不到CSV文件 {csv_file}")
            print("请确保CSV文件在当前目录下")
            return
        
        # 获取CSV文件大小
        file_size = os.path.getsize(csv_file) / (1024 * 1024)  # MB
        print(f"CSV文件: {csv_file}")
        print(f"文件大小: {file_size:.2f} MB")
        
        # 方案1：使用Python读取CSV并批量导入
        print("使用Python读取CSV并批量导入...")
        start_time = time.time()
        
        # 先清空已有的Reference节点
        with neo4j_driver.session() as session:
            print("正在清空已有的Reference节点...")
            clear_result = session.run("MATCH (r:Reference) DETACH DELETE r")
            deleted_count = clear_result.consume().counters.nodes_deleted
            print(f"已删除 {deleted_count} 个旧节点")
        
        # 读取CSV文件并批量导入
        batch_size = 1000
        batch_data = []
        total_imported = 0
        
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for i, row in enumerate(reader, 1):
                batch_data.append({
                    'handle': row['handle'],
                    'from_node_type': row['from_node_type'],
                    'from_node': row['from_node'],
                    'from_node_time': row['from_node_time'],
                    'to_node_type': row['to_node_type'],
                    'to_node': row['to_node'],
                    'to_node_time': row['to_node_time'],
                    'description': row['description']
                })
                
                # 每1000条或最后一批时执行批量插入
                if len(batch_data) >= batch_size or i % 10000 == 0:
                    try:
                        with neo4j_driver.session() as session:
                            result = session.run("""
                                UNWIND $batch AS row
                                CREATE (r:Reference {
                                    handle: row.handle,
                                    from_node_type: row.from_node_type,
                                    from_node: row.from_node,
                                    from_node_time: row.from_node_time,
                                    to_node_type: row.to_node_type,
                                    to_node: row.to_node,
                                    to_node_time: row.to_node_time,
                                    description: row.description
                                })
                            """, batch=batch_data)
                            
                            result.consume()
                            total_imported += len(batch_data)
                            
                            # 显示进度
                            print(f"已导入: {total_imported:,} 条记录")
                            
                            batch_data = []  # 清空批次
                            
                    except Exception as e:
                        print(f"批次导入失败: {e}")
                        batch_data = []
                        continue
        
        # 处理剩余的批次
        if batch_data:
            try:
                with neo4j_driver.session() as session:
                    result = session.run("""
                        UNWIND $batch AS row
                        CREATE (r:Reference {
                            handle: row.handle,
                            from_node_type: row.from_node_type,
                            from_node: row.from_node,
                            from_node_time: row.from_node_time,
                            to_node_type: row.to_node_type,
                            to_node: row.to_node,
                            to_node_time: row.to_node_time,
                            description: row.description
                        })
                    """, batch=batch_data)
                    
                    result.consume()
                    total_imported += len(batch_data)
            except Exception as e:
                print(f"最后批次导入失败: {e}")
        
        import_time = time.time() - start_time
        
        print("=" * 50)
        print("CSV导入完成！")
        print(f"成功导入节点: {total_imported:,} 个")
        print(f"导入耗时: {import_time:.2f}秒")
        print(f"平均速度: {total_imported/import_time:.1f} 节点/秒")
        print("=" * 50)
        
        # 验证导入结果
        print("正在验证导入结果...")
        with neo4j_driver.session() as session:
            count_result = session.run("MATCH (r:Reference) RETURN count(r) as total")
            total_count = count_result.single()['total']
            print(f"Neo4j中Reference节点总数: {total_count:,}")
            
            if total_count == total_imported:
                print("导入验证成功！")
            else:
                print("导入验证失败，数量不匹配")
        
    except Exception as e:
        print(f"CSV导入错误: {e}")
        import traceback
        traceback.print_exc()

def upload_csv_to_neo4j():
    """将CSV文件上传到Neo4j服务器可访问的位置"""
    try:
        print("=== 上传CSV文件到Neo4j服务器 ===")
        print("注意：此方法需要Neo4j服务器配置允许文件访问")
        
        # 检查Neo4j配置
        with neo4j_driver.session() as session:
            # 尝试获取Neo4j配置信息
            try:
                result = session.run("CALL dbms.listConfig() YIELD name, value WHERE name CONTAINS 'dbms.directories' RETURN name, value")
                configs = list(result)
                print("Neo4j目录配置:")
                for config in configs:
                    print(f"  {config['name']}: {config['value']}")
            except Exception as e:
                print(f"无法获取Neo4j配置: {e}")
        
        print("\n请将CSV文件放到Neo4j服务器的import目录中，然后使用以下命令:")
        print("LOAD CSV WITH HEADERS FROM 'file:///reference_data.csv' AS row")
        print("CREATE (r:Reference { ... })")
        
    except Exception as e:
        print(f"获取配置信息失败: {e}")

def create_indexes():
    """创建Neo4j索引以提高查询性能"""
    try:
        print("=== 步骤1: 创建Neo4j索引 ===")
        
        with neo4j_driver.session() as session:
            # 检查Neo4j版本
            try:
                version_result = session.run("CALL dbms.components() YIELD name, versions, edition")
                version_info = version_result.single()
                if version_info:
                    print(f"Neo4j版本: {version_info['name']} {version_info['versions'][0]}")
            except Exception as e:
                print(f"无法获取版本信息: {e}")
            
            # 为不同节点类型创建索引
            indexes = [
                # Reference节点索引
                "CREATE INDEX reference_handle_index IF NOT EXISTS FOR (n:Reference) ON (n.handle)",
                "CREATE INDEX reference_from_node_index IF NOT EXISTS FOR (n:Reference) ON (n.from_node)",
                "CREATE INDEX reference_to_node_index IF NOT EXISTS FOR (n:Reference) ON (n.to_node)",
                "CREATE INDEX reference_from_node_type_index IF NOT EXISTS FOR (n:Reference) ON (n.from_node_type)",
                "CREATE INDEX reference_to_node_type_index IF NOT EXISTS FOR (n:Reference) ON (n.to_node_type)",
                
                # 业务节点索引
                "CREATE INDEX sales_order_id_index IF NOT EXISTS FOR (n:SalesOrder) ON (n.order_id)",
                "CREATE INDEX sales_order_created_index IF NOT EXISTS FOR (n:SalesOrder) ON (n.created_at)",
                "CREATE INDEX receiving_batch_index IF NOT EXISTS FOR (n:ReceivingBatch) ON (n.batch_no)",
                "CREATE INDEX receiving_batch_created_index IF NOT EXISTS FOR (n:ReceivingBatch) ON (n.created_at)",
                "CREATE INDEX inventory_id_index IF NOT EXISTS FOR (n:Inventory) ON (n.inventory_id)",
                "CREATE INDEX inventory_created_index IF NOT EXISTS FOR (n:Inventory) ON (n.created_at)",
                "CREATE INDEX sfc_code_index IF NOT EXISTS FOR (n:SFC) ON (n.sfc_code)",
                "CREATE INDEX sfc_created_index IF NOT EXISTS FOR (n:SFC) ON (n.created_at)",
                "CREATE INDEX process_step_index IF NOT EXISTS FOR (n:ProcessStep) ON (n.step_desc)",
                "CREATE INDEX process_sfc_index IF NOT EXISTS FOR (n:ProcessStep) ON (n.sfc_no)",
                "CREATE INDEX process_created_index IF NOT EXISTS FOR (n:ProcessStep) ON (n.created_at)"
            ]
            
            total_indexes = len(indexes)
            for i, index_query in enumerate(indexes, 1):
                try:
                    session.run(index_query)
                    progress = (i / total_indexes) * 100
                    print(f"  [{i}/{total_indexes}] 索引创建成功: {index_query[:50]}... ({progress:.1f}%)")
                except Exception as e:
                    print(f"  [{i}/{total_indexes}] 索引可能已存在: {e}")
                    
        print(f"步骤1完成: 共创建 {total_indexes} 个索引")
            
    except Exception as e:
        print(f"步骤1失败: 创建索引时发生错误: {e}")

def create_business_nodes():
    """根据SQL逻辑创建业务节点"""
    try:
        print("=== 步骤2: 创建业务节点 ===")
        
        with neo4j_driver.session() as session:
            # 根据SQL逻辑创建不同类型的节点
            nodes_to_create = [
                # 销售订单节点
                {
                    'label': 'SalesOrder',
                    'properties': {'order_id': 'SO001', 'created_at': '2024-01-01T00:00:00'},
                    'description': '销售订单节点'
                },
                # 收货批节点
                {
                    'label': 'ReceivingBatch',
                    'properties': {'batch_no': 'RB001', 'created_at': '2024-01-01T00:00:00'},
                    'description': '收货批节点'
                },
                # 库存节点
                {
                    'label': 'Inventory',
                    'properties': {'inventory_id': 'INV001', 'created_at': '2024-01-01T00:00:00'},
                    'description': '库存节点'
                },
                # SFC节点
                {
                    'label': 'SFC',
                    'properties': {'sfc_code': 'SFC001', 'created_at': '2024-01-01T00:00:00'},
                    'description': 'SFC节点'
                },
                # 工序节点
                {
                    'label': 'ProcessStep',
                    'properties': {'step_desc': '工序1', 'sfc_no': 'SFC001', 'step_order': 1, 'created_at': '2024-01-01T00:00:00'},
                    'description': '工序节点'
                }
            ]
            
            total_nodes = len(nodes_to_create)
            for i, node_info in enumerate(nodes_to_create, 1):
                try:
                    # 构建属性字符串
                    props_str = ', '.join([f"{k}: '{v}'" for k, v in node_info['properties'].items()])
                    query = f"MERGE (n:{node_info['label']} {{{props_str}}})"
                    
                    session.run(query)
                    progress = (i / total_nodes) * 100
                    print(f"  [{i}/{total_nodes}] {node_info['description']} 创建成功 ({progress:.1f}%)")
                except Exception as e:
                    print(f"  [{i}/{total_nodes}] {node_info['description']} 创建失败: {e}")
            
        print(f"步骤2完成: 共创建 {total_nodes} 个业务节点")
        
    except Exception as e:
        print(f"步骤2失败: 创建业务节点时发生错误: {e}")

def create_business_relationships():
    """根据SQL逻辑创建业务关系"""
    try:
        print("=== 步骤3: 创建业务关系 ===")
        
        with neo4j_driver.session() as session:
            # 根据SQL逻辑创建关系
            relationships = [
                # 1. 销售订单 -> 收货批 (下单)
                {
                    'name': '销售订单到收货批',
                    'query': """
                        MATCH (so:SalesOrder)
                        MATCH (rb:ReceivingBatch)
                        WHERE so.order_id = rb.source_order_id
                        MERGE (so)-[:ORDERS {description: '下单', created_at: so.created_at}]->(rb)
                    """,
                    'description': '下单关系'
                },
                # 2. 收货批 -> 库存 (理货到称重)
                {
                    'name': '收货批到库存',
                    'query': """
                        MATCH (rb:ReceivingBatch)
                        MATCH (inv:Inventory)
                        WHERE rb.batch_no = inv.source_batch_no
                        MERGE (rb)-[:RECEIVES_TO_INVENTORY {description: '理货到称重', created_at: inv.created_at}]->(inv)
                    """,
                    'description': '理货到称重关系'
                },
                # 3. 库存 -> SFC (称重到产出)
                {
                    'name': '库存到SFC',
                    'query': """
                        MATCH (inv:Inventory)
                        MATCH (sfc:SFC)
                        WHERE inv.inventory_id = sfc.source_inventory_id
                        MERGE (inv)-[:PRODUCES_SFC {description: '称重到产出', created_at: sfc.created_at}]->(sfc)
                    """,
                    'description': '称重到产出关系'
                },
                # 4. SFC -> 工序步骤
                {
                    'name': 'SFC到工序步骤',
                    'query': """
                        MATCH (sfc:SFC)
                        MATCH (ps:ProcessStep)
                        WHERE sfc.sfc_code = ps.sfc_no
                        MERGE (sfc)-[:HAS_PROCESS_STEP {description: '包含工序', created_at: ps.created_at}]->(ps)
                    """,
                    'description': '包含工序关系'
                },
                # 5. 工序步骤之间的顺序关系 - 修复数据类型问题
                {
                    'name': '工序步骤顺序关系',
                    'query': """
                        MATCH (ps1:ProcessStep)
                        MATCH (ps2:ProcessStep)
                        WHERE ps1.sfc_no = ps2.sfc_no 
                        AND toInteger(ps1.step_order) = toInteger(ps2.step_order) - 1
                        MERGE (ps1)-[:NEXT_STEP {description: '工序顺序', created_at: ps2.created_at}]->(ps2)
                    """,
                    'description': '工序顺序关系'
                },
                # 6. SFC拆合关系
                {
                    'name': 'SFC拆合关系',
                    'query': """
                        MATCH (sfc1:SFC)
                        MATCH (sfc2:SFC)
                        WHERE sfc1.sfc_code = sfc2.source_sfc_code
                        MERGE (sfc1)-[:SPLITS_TO {description: 'SFC拆合记录', created_at: sfc1.created_at}]->(sfc2)
                    """,
                    'description': 'SFC拆合记录关系'
                },
                # 7. SFC -> 最终库存 (包装入库)
                {
                    'name': 'SFC到最终库存',
                    'query': """
                        MATCH (sfc:SFC)
                        MATCH (inv:Inventory)
                        WHERE sfc.sfc_code = inv.source_sfc_code
                        MERGE (sfc)-[:PACKAGES_TO_INVENTORY {description: '包装入库', created_at: inv.created_at}]->(inv)
                    """,
                    'description': '包装入库关系'
                }
            ]
            
            total_rels = len(relationships)
            for i, rel_info in enumerate(relationships, 1):
                try:
                    print(f"  [{i}/{total_rels}] 正在创建 {rel_info['name']}...")
                    session.run(rel_info['query'])
                    progress = (i / total_rels) * 100
                    print(f"  [{i}/{total_rels}] {rel_info['description']} 创建成功 ({progress:.1f}%)")
                except Exception as e:
                    print(f"  [{i}/{total_rels}] {rel_info['description']} 创建失败: {e}")
            
        print(f"步骤3完成: 共创建 {total_rels} 种业务关系")
        
    except Exception as e:
        print(f"步骤3失败: 创建业务关系时发生错误: {e}")
        import traceback
        traceback.print_exc()

def create_constraints():
    """创建约束确保数据完整性"""
    try:
        print("=== 步骤4: 创建约束 ===")
        
        with neo4j_driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT sales_order_unique IF NOT EXISTS FOR (n:SalesOrder) REQUIRE n.order_id IS UNIQUE",
                "CREATE CONSTRAINT receiving_batch_unique IF NOT EXISTS FOR (n:ReceivingBatch) REQUIRE n.batch_no IS UNIQUE",
                "CREATE CONSTRAINT inventory_unique IF NOT EXISTS FOR (n:Inventory) REQUIRE n.inventory_id IS UNIQUE",
                "CREATE CONSTRAINT sfc_unique IF NOT EXISTS FOR (n:SFC) REQUIRE n.sfc_code IS UNIQUE",
                "CREATE CONSTRAINT process_step_unique IF NOT EXISTS FOR (n:ProcessStep) REQUIRE (n.sfc_no, n.step_order) IS UNIQUE",
                "CREATE CONSTRAINT reference_unique IF NOT EXISTS FOR (n:Reference) REQUIRE n.handle IS UNIQUE"
            ]
            
            total_constraints = len(constraints)
            for i, constraint in enumerate(constraints, 1):
                try:
                    session.run(constraint)
                    progress = (i / total_constraints) * 100
                    print(f"  [{i}/{total_constraints}] 约束创建成功 ({progress:.1f}%)")
                except Exception as e:
                    print(f"  [{i}/{total_constraints}] 约束可能已存在: {e}")
            
        print(f"步骤4完成: 共创建 {total_constraints} 个约束")
        
    except Exception as e:
        print(f"步骤4失败: 创建约束时发生错误: {e}")

def build_complete_graph_model():
    """构建完整的图数据模型"""
    try:
        print("=== 构建完整的图数据模型 ===")
        
        # 1. 创建索引
        create_indexes()
        print()
        
        # 2. 创建约束
        create_constraints()
        print()
        
        # 3. 创建业务节点
        create_business_nodes()
        print()
        
        # 4. 创建业务关系
        create_business_relationships()
        print()
        
        print("=" * 60)
        print("完整的图数据模型构建完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"构建图数据模型失败: {e}")
        import traceback
        traceback.print_exc()

def clean_duplicate_nodes():
    """清理重复节点"""
    try:
        print("=== 清理重复节点 ===")
        
        with neo4j_driver.session() as session:
            # 先删除约束，然后清理重复节点
            print("正在删除约束...")
            constraints_to_drop = [
                "DROP CONSTRAINT sales_order_unique IF EXISTS",
                "DROP CONSTRAINT receiving_batch_unique IF EXISTS", 
                "DROP CONSTRAINT inventory_unique IF EXISTS",
                "DROP CONSTRAINT sfc_unique IF EXISTS",
                "DROP CONSTRAINT process_step_unique IF EXISTS",
                "DROP CONSTRAINT reference_unique IF EXISTS"
            ]
            
            for constraint in constraints_to_drop:
                try:
                    session.run(constraint)
                    print(f"  约束删除成功: {constraint}")
                except Exception as e:
                    print(f"  约束删除失败: {e}")
            
            # 清理各种类型的重复节点
            node_types = [
                ('SalesOrder', 'order_id'),
                ('ReceivingBatch', 'batch_no'),
                ('Inventory', 'inventory_id'),
                ('SFC', 'sfc_code'),
                ('ProcessStep', ['sfc_no', 'step_order'])
            ]
            
            for node_type, key_props in node_types:
                print(f"正在清理 {node_type} 重复节点...")
                
                if isinstance(key_props, list):
                    # 复合键的情况
                    key_str = ', '.join([f'n.{prop}' for prop in key_props])
                    cleanup_query = f"""
                        MATCH (n:{node_type})
                        WITH {key_str}, collect(n) as nodes
                        WHERE size(nodes) > 1
                        UNWIND tail(nodes) as duplicate
                        DETACH DELETE duplicate
                        RETURN count(duplicate) as deleted_count
                    """
                else:
                    # 单键的情况
                    cleanup_query = f"""
                        MATCH (n:{node_type})
                        WITH n.{key_props} as key_value, collect(n) as nodes
                        WHERE size(nodes) > 1
                        UNWIND tail(nodes) as duplicate
                        DETACH DELETE duplicate
                        RETURN count(duplicate) as deleted_count
                    """
                
                try:
                    result = session.run(cleanup_query)
                    deleted_count = result.single()['deleted_count']
                    print(f"  {node_type}: 删除了 {deleted_count} 个重复节点")
                except Exception as e:
                    print(f"  {node_type}: 清理失败 - {e}")
            
        print("重复节点清理完成！")
        
    except Exception as e:
        print(f"清理重复节点失败: {e}")
        import traceback
        traceback.print_exc()

def fix_failed_relationships():
    """修复失败的关系创建"""
    try:
        print("=== 修复失败的关系创建 ===")
        
        with neo4j_driver.session() as session:
            # 先确保没有重复节点
            print("检查是否还有重复节点...")
            
            # 检查ProcessStep重复节点
            duplicates = session.run("""
                MATCH (ps:ProcessStep)
                WITH ps.sfc_no as sfc_no, ps.step_order as step_order, collect(ps) as nodes
                WHERE size(nodes) > 1
                RETURN sfc_no, step_order, size(nodes) as count
            """)
            
            duplicate_count = 0
            for record in duplicates:
                sfc_no = record['sfc_no']
                step_order = record['step_order']
                count = record['count']
                print(f"  发现重复: SFC {sfc_no}, 工序 {step_order}, 共 {count} 个节点")
                duplicate_count += count
            
            if duplicate_count > 0:
                print("仍有重复节点，请先选择选项6清理重复节点")
                return
            
            # 确保step_order是整数类型
            print("正在修复数据类型...")
            session.run("""
                MATCH (ps:ProcessStep)
                WHERE ps.step_order IS NOT NULL
                SET ps.step_order = toInteger(ps.step_order)
            """)
            
            # 重新创建工序步骤顺序关系
            print("正在重新创建工序步骤顺序关系...")
            result = session.run("""
                MATCH (ps1:ProcessStep)
                MATCH (ps2:ProcessStep)
                WHERE ps1.sfc_no = ps2.sfc_no 
                AND toInteger(ps1.step_order) = toInteger(ps2.step_order) - 1
                AND NOT (ps1)-[:NEXT_STEP]->(ps2)
                MERGE (ps1)-[:NEXT_STEP {description: '工序顺序', created_at: ps2.created_at}]->(ps2)
                RETURN count(*) as relationships_created
            """)
            
            count = result.single()['relationships_created']
            print(f"工序步骤顺序关系修复完成，创建了 {count} 个关系")
            
        print("关系修复完成！")
        
    except Exception as e:
        print(f"关系修复失败: {e}")
        import traceback
        traceback.print_exc()

def rebuild_graph_model():
    """重新构建图数据模型"""
    try:
        print("=== 重新构建图数据模型 ===")
        
        with neo4j_driver.session() as session:
            # 1. 清理所有数据
            print("正在清理所有现有数据...")
            session.run("MATCH (n) DETACH DELETE n")
            print("数据清理完成")
            
            # 2. 重新创建索引
            print("\n正在重新创建索引...")
            create_indexes()
            
            # 3. 重新创建约束
            print("\n正在重新创建约束...")
            create_constraints()
            
            # 4. 重新创建业务节点
            print("\n正在重新创建业务节点...")
            create_business_nodes()
            
            # 5. 重新创建业务关系
            print("\n正在重新创建业务关系...")
            create_business_relationships()
            
        print("=" * 60)
        print("图数据模型重新构建完成！")
        print("=" * 60)
        
    except Exception as e:
        print(f"重新构建图数据模型失败: {e}")
        import traceback
        traceback.print_exc()

def verify_graph_model():
    """验证图数据模型"""
    try:
        print("=== 验证图数据模型 ===")
        
        with neo4j_driver.session() as session:
            # 统计各种节点数量
            print("节点统计:")
            node_counts = [
                ("Reference", "MATCH (r:Reference) RETURN count(r) as count"),
                ("SalesOrder", "MATCH (so:SalesOrder) RETURN count(so) as count"),
                ("ReceivingBatch", "MATCH (rb:ReceivingBatch) RETURN count(rb) as count"),
                ("Inventory", "MATCH (inv:Inventory) RETURN count(inv) as count"),
                ("SFC", "MATCH (sfc:SFC) RETURN count(sfc) as count"),
                ("ProcessStep", "MATCH (ps:ProcessStep) RETURN count(ps) as count")
            ]
            
            for node_type, query in node_counts:
                result = session.run(query)
                count = result.single()['count']
                print(f"  {node_type}: {count:,} 个")
            
            # 统计关系数量
            print("\n关系统计:")
            rel_counts = [
                ("ORDERS", "MATCH ()-[:ORDERS]->() RETURN count(*) as count"),
                ("RECEIVES_TO_INVENTORY", "MATCH ()-[:RECEIVES_TO_INVENTORY]->() RETURN count(*) as count"),
                ("PRODUCES_SFC", "MATCH ()-[:PRODUCES_SFC]->() RETURN count(*) as count"),
                ("HAS_PROCESS_STEP", "MATCH ()-[:HAS_PROCESS_STEP]->() RETURN count(*) as count"),
                ("NEXT_STEP", "MATCH ()-[:NEXT_STEP]->() RETURN count(*) as count"),
                ("SPLITS_TO", "MATCH ()-[:SPLITS_TO]->() RETURN count(*) as count"),
                ("PACKAGES_TO_INVENTORY", "MATCH ()-[:PACKAGES_TO_INVENTORY]->() RETURN count(*) as count")
            ]
            
            for rel_type, query in rel_counts:
                result = session.run(query)
                count = result.single()['count']
                print(f"  {rel_type}: {count:,} 个")
            
            # 检查数据完整性
            print("\n数据完整性检查:")
            
            # 检查是否有孤立节点
            isolated_nodes = session.run("""
                MATCH (n)
                WHERE NOT (n)--()
                RETURN labels(n) as labels, count(n) as count
            """)
            
            isolated_count = 0
            for record in isolated_nodes:
                isolated_count += record['count']
                print(f"  孤立节点 {record['labels']}: {record['count']} 个")
            
            if isolated_count == 0:
                print("  所有节点都有连接关系")
            else:
                print(f"  发现 {isolated_count} 个孤立节点")
        
        print("\n图数据模型验证完成！")
        
    except Exception as e:
        print(f"验证失败: {e}")
        import traceback
        traceback.print_exc()

def build_reference_relationships():
    """根据Reference数据建立业务关系"""
    try:
        print("=== 根据Reference数据建立业务关系 ===")
        
        with neo4j_driver.session() as session:
            # 1. 建立销售订单到收货批的关系
            print("正在建立销售订单到收货批的关系...")
            result = session.run("""
                MATCH (r:Reference)
                WHERE r.from_node_type = '销售订单号' AND r.to_node_type = '收货批'
                MERGE (so:SalesOrder {order_id: r.from_node})
                MERGE (rb:ReceivingBatch {batch_no: r.to_node})
                MERGE (so)-[:ORDERS {description: r.description, created_at: r.from_node_time}]->(rb)
                RETURN count(*) as relationships_created
            """)
            count1 = result.single()['relationships_created']
            print(f"  销售订单到收货批: {count1} 个关系")
            
            # 2. 建立收货批到库存的关系
            print("正在建立收货批到库存的关系...")
            result = session.run("""
                MATCH (r:Reference)
                WHERE r.from_node_type = '收货批' AND r.to_node_type = '库存id'
                MERGE (rb:ReceivingBatch {batch_no: r.from_node})
                MERGE (inv:Inventory {inventory_id: r.to_node})
                MERGE (rb)-[:RECEIVES_TO_INVENTORY {description: r.description, created_at: r.from_node_time}]->(inv)
                RETURN count(*) as relationships_created
            """)
            count2 = result.single()['relationships_created']
            print(f"  收货批到库存: {count2} 个关系")
            
            # 3. 建立库存到SFC的关系
            print("正在建立库存到SFC的关系...")
            result = session.run("""
                MATCH (r:Reference)
                WHERE r.from_node_type = '库存id' AND r.to_node_type = 'SFC'
                MERGE (inv:Inventory {inventory_id: r.from_node})
                MERGE (sfc:SFC {sfc_code: r.to_node})
                MERGE (inv)-[:PRODUCES_SFC {description: r.description, created_at: r.from_node_time}]->(sfc)
                RETURN count(*) as relationships_created
            """)
            count3 = result.single()['relationships_created']
            print(f"  库存到SFC: {count3} 个关系")
            
            # 4. 建立SFC工序关系
            print("正在建立SFC工序关系...")
            result = session.run("""
                MATCH (r:Reference)
                WHERE r.from_node_type = 'SFC' AND r.to_node_type = 'SFC' AND r.description = '工序'
                MERGE (sfc1:SFC {sfc_code: r.from_node})
                MERGE (sfc2:SFC {sfc_code: r.to_node})
                MERGE (sfc1)-[:NEXT_STEP {description: r.description, created_at: r.from_node_time}]->(sfc2)
                RETURN count(*) as relationships_created
            """)
            count4 = result.single()['relationships_created']
            print(f"  SFC工序关系: {count4} 个关系")
            
            # 5. 建立SFC拆合关系
            print("正在建立SFC拆合关系...")
            result = session.run("""
                MATCH (r:Reference)
                WHERE r.from_node_type = 'SFC' AND r.to_node_type = 'SFC' AND r.description = 'SFC拆合记录'
                MERGE (sfc1:SFC {sfc_code: r.from_node})
                MERGE (sfc2:SFC {sfc_code: r.to_node})
                MERGE (sfc1)-[:SPLITS_TO {description: r.description, created_at: r.from_node_time}]->(sfc2)
                RETURN count(*) as relationships_created
            """)
            count5 = result.single()['relationships_created']
            print(f"  SFC拆合关系: {count5} 个关系")
            
            # 6. 建立SFC到最终库存的关系
            print("正在建立SFC到最终库存的关系...")
            result = session.run("""
                MATCH (r:Reference)
                WHERE r.from_node_type = 'SFC' AND r.to_node_type = '库存id' AND r.description = '包装入库'
                MERGE (sfc:SFC {sfc_code: r.from_node})
                MERGE (inv:Inventory {inventory_id: r.to_node})
                MERGE (sfc)-[:PACKAGES_TO_INVENTORY {description: r.description, created_at: r.from_node_time}]->(inv)
                RETURN count(*) as relationships_created
            """)
            count6 = result.single()['relationships_created']
            print(f"  SFC到最终库存: {count6} 个关系")
            
            total_relationships = count1 + count2 + count3 + count4 + count5 + count6
            print(f"\n总共建立了 {total_relationships} 个业务关系")
            
        print("业务关系建立完成！")
        
    except Exception as e:
        print(f"建立业务关系失败: {e}")
        import traceback
        traceback.print_exc()

def create_process_step_relationships():
    """创建工序步骤之间的顺序关系"""
    try:
        print("=== 创建工序步骤顺序关系 ===")
        
        with neo4j_driver.session() as session:
            # 从mes_pd_production_log_silver创建工序步骤
            print("正在从生产日志创建工序步骤...")
            
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                WHERE log.processing_status = 'COMPLETE'
                WITH log.sfc_no as sfc_no, log.process_step_desc as step_desc, 
                     log.created_at as created_at, log.processing_order as step_order
                ORDER BY sfc_no, step_order
                MERGE (ps:ProcessStep {sfc_no: sfc_no, step_desc: step_desc, 
                                      created_at: created_at, step_order: toInteger(step_order)})
                RETURN count(ps) as nodes_created
            """)
            
            nodes_created = result.single()['nodes_created']
            print(f"  创建了 {nodes_created} 个工序步骤节点")
            
            # 建立工序步骤之间的顺序关系
            print("正在建立工序步骤顺序关系...")
            
            result = session.run("""
                MATCH (ps1:ProcessStep)
                MATCH (ps2:ProcessStep)
                WHERE ps1.sfc_no = ps2.sfc_no 
                AND toInteger(ps1.step_order) = toInteger(ps2.step_order) - 1
                AND NOT (ps1)-[:NEXT_STEP]->(ps2)
                MERGE (ps1)-[:NEXT_STEP {description: '工序顺序', created_at: ps2.created_at}]->(ps2)
                RETURN count(*) as relationships_created
            """)
            
            rels_created = result.single()['relationships_created']
            print(f"  创建了 {rels_created} 个工序顺序关系")
            
        print("工序步骤关系创建完成！")
        
    except Exception as e:
        print(f"创建工序步骤关系失败: {e}")
        import traceback
        traceback.print_exc()

def link_isolated_nodes():
    """关联孤立的业务节点到主流程"""
    try:
        print("=== 关联孤立的业务节点 ===")
        
        with neo4j_driver.session() as session:
            # 关联mes_pd_lot_history_silver到主流程
            print("正在关联库存历史记录...")
            
            result = session.run("""
                MATCH (log:mes_pd_lot_history_silver)
                MATCH (inv:Inventory {inventory_id: log.inventory_id})
                WHERE log.inventory_id IS NOT NULL
                MERGE (log)-[:HISTORY_OF]->(inv)
                RETURN count(*) as relationships_created
            """)
            
            count1 = result.single()['relationships_created']
            print(f"  库存历史记录关联: {count1} 个关系")
            
            # 关联mes_pd_sfc_silver到SFC节点
            print("正在关联SFC详细信息...")
            
            result = session.run("""
                MATCH (detail:mes_pd_sfc_silver)
                MATCH (sfc:SFC {sfc_code: detail.sfc_code})
                WHERE detail.sfc_code IS NOT NULL
                MERGE (detail)-[:DETAILS_OF]->(sfc)
                RETURN count(*) as relationships_created
            """)
            
            count2 = result.single()['relationships_created']
            print(f"  SFC详细信息关联: {count2} 个关系")
            
        print("孤立节点关联完成！")
        
    except Exception as e:
        print(f"关联孤立节点失败: {e}")
        import traceback
        traceback.print_exc()

def create_sfc_process_workflow():
    """创建SFC工序工作流：SFC → 工序1 → 工序2 → ... → 下一个SFC"""
    try:
        print("=== 创建SFC工序工作流 ===")
        
        with neo4j_driver.session() as session:
            # 1. 从生产日志创建工序步骤节点，使用时间排序
            print("正在从生产日志创建工序步骤节点...")
            
            # 使用created_at时间排序，自动生成工序顺序
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                WHERE log.processing_status = 'COMPLETE'
                WITH log.sfc_no as sfc_no, log.process_step_desc as step_desc, 
                     log.created_at as created_at
                ORDER BY sfc_no, created_at
                WITH sfc_no, collect({step_desc: step_desc, created_at: created_at}) as steps
                UNWIND range(0, size(steps)-1) as index
                WITH sfc_no, steps[index] as step, index + 1 as step_order
                MERGE (ps:ProcessStep {sfc_no: sfc_no, step_desc: step.step_desc, 
                                      created_at: step.created_at, step_order: step_order})
                RETURN count(ps) as nodes_created
            """)
            
            nodes_created = result.single()['nodes_created']
            print(f"  创建了 {nodes_created} 个工序步骤节点")
            
            # 2. 建立SFC到第一个工序的关系
            print("\n正在建立SFC到第一个工序的关系...")
            
            result = session.run("""
                MATCH (sfc:SFC)
                MATCH (ps:ProcessStep)
                WHERE sfc.sfc_code = ps.sfc_no AND ps.step_order = 1
                MERGE (sfc)-[:HAS_FIRST_STEP {description: '开始工序', created_at: ps.created_at}]->(ps)
                RETURN count(*) as relationships_created
            """)
            
            sfc_to_first = result.single()['relationships_created']
            print(f"  SFC到第一个工序: {sfc_to_first} 个关系")
            
            # 3. 建立工序之间的顺序关系
            print("正在建立工序之间的顺序关系...")
            
            result = session.run("""
                MATCH (ps1:ProcessStep)
                MATCH (ps2:ProcessStep)
                WHERE ps1.sfc_no = ps2.sfc_no 
                AND toInteger(ps1.step_order) = toInteger(ps2.step_order) - 1
                AND NOT (ps1)-[:NEXT_STEP]->(ps2)
                MERGE (ps1)-[:NEXT_STEP {description: '工序顺序', created_at: ps2.created_at}]->(ps2)
                RETURN count(*) as relationships_created
            """)
            
            step_to_step = result.single()['relationships_created']
            print(f"  工序到工序: {step_to_step} 个关系")
            
            # 4. 建立最后一个工序到下一个SFC的关系
            print("正在建立最后一个工序到下一个SFC的关系...")
            
            # 先找到每个SFC的最后一个工序
            result = session.run("""
                MATCH (ps:ProcessStep)
                WITH ps.sfc_no as sfc_no, max(ps.step_order) as max_step
                MATCH (ps:ProcessStep)
                WHERE ps.sfc_no = sfc_no AND ps.step_order = max_step
                RETURN ps.sfc_no as sfc_no, ps.step_order as step_order, ps.created_at as created_at
                ORDER BY sfc_no
            """)
            
            last_steps = list(result)
            print(f"  找到 {len(last_steps)} 个SFC的最后一个工序")
            
            # 建立最后一个工序到下一个SFC的关系
            relationships_created = 0
            for i in range(len(last_steps) - 1):
                current_sfc = last_steps[i]['sfc_no']
                next_sfc = last_steps[i + 1]['sfc_no']  # 修复：使用正确的字段名
                created_at = last_steps[i]['created_at']
                
                try:
                    result = session.run("""
                        MATCH (ps:ProcessStep {sfc_no: $current_sfc, step_order: $step_order})
                        MATCH (next_sfc:SFC {sfc_code: $next_sfc})
                        MERGE (ps)-[:LEADS_TO_NEXT_SFC {description: '工序完成到下一个SFC', created_at: $created_at}]->(next_sfc)
                        RETURN count(*) as count
                    """, current_sfc=current_sfc, step_order=last_steps[i]['step_order'], 
                         next_sfc=next_sfc, created_at=created_at)
                    
                    count = result.single()['count']
                    relationships_created += count
                    
                except Exception as e:
                    print(f"    建立 {current_sfc} 到 {next_sfc} 的关系失败: {e}")
                    continue
            
            print(f"  最后一个工序到下一个SFC: {relationships_created} 个关系")
            
            # 5. 统计总关系数
            total_relationships = sfc_to_first + step_to_step + relationships_created
            print(f"\n总共建立了 {total_relationships} 个工序工作流关系")
            
        print("SFC工序工作流创建完成！")
        
    except Exception as e:
        print(f"创建SFC工序工作流失败: {e}")
        import traceback
        traceback.print_exc()

def check_available_fields():
    """检查生产日志中可用的字段"""
    try:
        print("=== 检查生产日志中可用的字段 ===")
        
        with neo4j_driver.session() as session:
            # 获取一个示例节点的所有属性
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                RETURN keys(log) as available_fields
                LIMIT 1
            """)
            
            fields = result.single()['available_fields']
            print("可用的字段:")
            for field in fields:
                print(f"  {field}")
            
            # 检查一些关键字段的值
            key_fields = ['sfc_no', 'process_step_desc', 'processing_status', 'created_at']
            print(f"\n关键字段值检查:")
            
            for field in key_fields:
                if field in fields:
                    result = session.run(f"""
                        MATCH (log:mes_pd_production_log_silver)
                        WHERE log.{field} IS NOT NULL
                        RETURN count(log.{field}) as count, 
                               collect(DISTINCT log.{field})[0..5] as sample_values
                    """)
                    
                    data = result.single()
                    print(f"  {field}: {data['count']} 个非空值, 示例: {data['sample_values']}")
                else:
                    print(f"  {field}: 字段不存在")
            
        print("字段检查完成！")
        
    except Exception as e:
        print(f"检查字段失败: {e}")
        import traceback
        traceback.print_exc()

def create_simple_process_workflow():
    """创建简单的工序工作流（基于现有数据）"""
    try:
        print("=== 创建简单的工序工作流 ===")
        
        with neo4j_driver.session() as session:
            # 1. 从生产日志创建工序步骤节点
            print("正在从生产日志创建工序步骤节点...")
            
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                WHERE log.processing_status = 'COMPLETE'
                WITH log.sfc_no as sfc_no, log.process_step_desc as step_desc, 
                     log.created_at as created_at
                ORDER BY sfc_no, created_at
                WITH sfc_no, collect({step_desc: step_desc, created_at: created_at}) as steps
                UNWIND range(0, size(steps)-1) as index
                WITH sfc_no, steps[index] as step, index + 1 as step_order
                MERGE (ps:ProcessStep {sfc_no: sfc_no, step_desc: step.step_desc, 
                                      created_at: step.created_at, step_order: step_order})
                RETURN count(ps) as nodes_created
            """)
            
            nodes_created = result.single()['nodes_created']
            print(f"  创建了 {nodes_created} 个工序步骤节点")
            
            # 2. 建立SFC到工序的关系
            print("\n正在建立SFC到工序的关系...")
            
            result = session.run("""
                MATCH (sfc:SFC)
                MATCH (ps:ProcessStep)
                WHERE sfc.sfc_code = ps.sfc_no
                MERGE (sfc)-[:HAS_PROCESS_STEP {description: '包含工序', created_at: ps.created_at}]->(ps)
                RETURN count(*) as relationships_created
            """)
            
            sfc_to_process = result.single()['relationships_created']
            print(f"  SFC到工序: {sfc_to_process} 个关系")
            
            # 3. 建立工序之间的顺序关系
            print("正在建立工序之间的顺序关系...")
            
            result = session.run("""
                MATCH (ps1:ProcessStep)
                MATCH (ps2:ProcessStep)
                WHERE ps1.sfc_no = ps2.sfc_no 
                AND toInteger(ps1.step_order) = toInteger(ps2.step_order) - 1
                AND NOT (ps1)-[:NEXT_STEP]->(ps2)
                MERGE (ps1)-[:NEXT_STEP {description: '工序顺序', created_at: ps2.created_at}]->(ps2)
                RETURN count(*) as relationships_created
            """)
            
            step_to_step = result.single()['relationships_created']
            print(f"  工序到工序: {step_to_step} 个关系")
            
            total_relationships = sfc_to_process + step_to_step
            print(f"\n总共建立了 {total_relationships} 个工序工作流关系")
            
        print("简单工序工作流创建完成！")
        
    except Exception as e:
        print(f"创建简单工序工作流失败: {e}")
        import traceback
        traceback.print_exc()

def check_production_log_structure():
    """检查生产日志的数据结构"""
    try:
        print("=== 检查生产日志数据结构 ===")
        
        with neo4j_driver.session() as session:
            # 检查字段是否存在
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                RETURN DISTINCT 
                    log.sfc_no IS NOT NULL as has_sfc_no,
                    log.process_step_desc IS NOT NULL as has_step_desc,
                    log.processing_status IS NOT NULL as has_status,
                    log.created_at IS NOT NULL as has_created_at
                LIMIT 1
            """)
            
            fields = result.single()
            print("字段存在性检查:")
            print(f"  sfc_no: {fields['has_sfc_no']}")
            print(f"  process_step_desc: {fields['has_step_desc']}")
            print(f"  processing_status: {fields['has_status']}")
            print(f"  created_at: {fields['has_created_at']}")
            
            # 检查processing_status的值分布
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                RETURN 
                    count(log) as total_records,
                    count(log.processing_status) as records_with_status
            """)
            
            stats = result.single()
            print(f"\n记录统计:")
            print(f"  总记录数: {stats['total_records']}")
            print(f"  有processing_status的记录: {stats['records_with_status']}")
            
            # 检查一些示例数据
            result = session.run("""
                MATCH (log:mes_pd_production_log_silver)
                RETURN log.sfc_no as sfc_no, log.process_step_desc as step_desc, 
                       log.processing_status as status, log.created_at as created_at
                LIMIT 10
            """)
            
            print(f"\n示例数据:")
            for record in result:
                print(f"  SFC: {record['sfc_no']}, 工序: {record['step_desc']}, 状态: {record['status']}, 时间: {record['created_at']}")
            
        print("数据结构检查完成！")
        
    except Exception as e:
        print(f"检查数据结构失败: {e}")
        import traceback
        traceback.print_exc()

def visualize_sfc_workflow():
    """可视化SFC工作流"""
    try:
        print("=== 可视化SFC工作流 ===")
        
        with neo4j_driver.session() as session:
            # 查找一个完整的SFC工作流示例
            result = session.run("""
                MATCH (sfc:SFC)-[:HAS_FIRST_STEP]->(first:ProcessStep)
                OPTIONAL MATCH path = (first)-[:NEXT_STEP*]->(last:ProcessStep)
                OPTIONAL MATCH (last)-[:LEADS_TO_NEXT_SFC]->(next_sfc:SFC)
                RETURN sfc.sfc_code as start_sfc, 
                       [node in nodes(path) | node.step_desc] as process_steps,
                       next_sfc.sfc_code as next_sfc
                LIMIT 5
            """)
            
            print("SFC工作流示例:")
            for record in result:
                start_sfc = record['start_sfc']
                process_steps = record['process_steps']
                next_sfc = record['next_sfc']
                
                print(f"  {start_sfc} → {process_steps} → {next_sfc}")
            
            # 如果没有完整的工作流，显示部分工作流
            if not list(result):
                print("没有找到完整的工作流，显示部分工序信息:")
                
                result = session.run("""
                    MATCH (sfc:SFC)
                    OPTIONAL MATCH (sfc)-[:HAS_PROCESS_STEP]->(ps:ProcessStep)
                    RETURN sfc.sfc_code as sfc_code, 
                           collect(ps.step_desc) as process_steps
                    LIMIT 10
                """)
                
                for record in result:
                    sfc_code = record['sfc_code']
                    process_steps = record['process_steps']
                    if process_steps:
                        print(f"  {sfc_code}: {process_steps}")
                    else:
                        print(f"  {sfc_code}: 暂无工序")
            
        print("工作流可视化完成！")
        
    except Exception as e:
        print(f"可视化工作流失败: {e}")
        import traceback
        traceback.print_exc()

def show_workflow_statistics():
    """显示工作流统计信息"""
    try:
        print("=== 工作流统计信息 ===")
        
        with neo4j_driver.session() as session:
            # 统计各种关系数量
            relationships = [
                ("HAS_FIRST_STEP", "SFC到第一个工序"),
                ("HAS_PROCESS_STEP", "SFC到工序"),
                ("NEXT_STEP", "工序顺序"),
                ("LEADS_TO_NEXT_SFC", "工序到下一个SFC")
            ]
            
            print("工作流关系统计:")
            for rel_type, description in relationships:
                result = session.run(f"MATCH ()-[:{rel_type}]->() RETURN count(*) as count")
                count = result.single()['count']
                print(f"  {description}: {count:,} 个")
            
            # 统计工序步骤分布
            print("\n工序步骤分布:")
            result = session.run("""
                MATCH (ps:ProcessStep)
                WITH ps.sfc_no as sfc_no, count(ps) as step_count
                RETURN step_count, count(sfc_no) as sfc_count
                ORDER BY step_count
            """)
            
            for record in result:
                step_count = record['step_count']
                sfc_count = record['sfc_count']
                print(f"  {step_count} 个工序的SFC: {sfc_count:,} 个")
            
            # 查找最长的工作流
            print("\n最长工作流:")
            result = session.run("""
                MATCH path = (sfc:SFC)-[:HAS_FIRST_STEP]->(first:ProcessStep)-[:NEXT_STEP*]->(last:ProcessStep)
                RETURN sfc.sfc_code as sfc_code, length(path) as path_length
                ORDER BY path_length DESC
                LIMIT 5
            """)
            
            for record in result:
                sfc_code = record['sfc_code']
                path_length = record['path_length']
                print(f"  {sfc_code}: {path_length} 个步骤")
            
        print("工作流统计完成！")
        
    except Exception as e:
        print(f"显示工作流统计失败: {e}")
        import traceback
        traceback.print_exc()

def main():
    """主函数，选择操作"""
    print("请选择操作:")
    print("1. 从MySQL直接导入数据")
    print("2. 从CSV文件导入数据")
    print("3. 构建完整的图数据模型")
    print("4. 修复失败的关系")
    print("5. 验证图数据模型")
    print("6. 清理重复节点")
    print("7. 重新构建图数据模型")
    print("8. 根据Reference数据建立业务关系")
    print("9. 创建SFC工序工作流")
    print("10. 可视化SFC工作流")
    print("11. 检查生产日志数据结构")
    print("12. 检查可用字段")
    print("13. 创建简单工序工作流")
    print("14. 显示工作流统计")
    print("15. 导出CSV后导入")
    
    choice = input("请输入选择 (1-15): ").strip()
    
    if choice == "1":
        print("\n选择从MySQL直接导入方式...")
        import_data()
    elif choice == "2":
        print("\n选择从CSV文件导入...")
        import_from_csv()
    elif choice == "3":
        print("\n选择构建完整的图数据模型...")
        build_complete_graph_model()
    elif choice == "4":
        print("\n选择修复失败的关系...")
        fix_failed_relationships()
    elif choice == "5":
        print("\n选择验证图数据模型...")
        verify_graph_model()
    elif choice == "6":
        print("\n选择清理重复节点...")
        clean_duplicate_nodes()
    elif choice == "7":
        print("\n选择重新构建图数据模型...")
        rebuild_graph_model()
    elif choice == "8":
        print("\n选择根据Reference数据建立业务关系...")
        build_reference_relationships()
    elif choice == "9":
        print("\n选择创建SFC工序工作流...")
        create_sfc_process_workflow()
    elif choice == "10":
        print("\n选择可视化SFC工作流...")
        visualize_sfc_workflow()
    elif choice == "11":
        print("\n选择检查生产日志数据结构...")
        check_production_log_structure()
    elif choice == "12":
        print("\n选择检查可用字段...")
        check_available_fields()
    elif choice == "13":
        print("\n选择创建简单工序工作流...")
        create_simple_process_workflow()
    elif choice == "14":
        print("\n选择显示工作流统计...")
        show_workflow_statistics()
    elif choice == "15":
        print("\n选择导出CSV后导入方式...")
        export_to_csv()
    else:
        print("无效选择，使用默认的图数据模型验证...")
        verify_graph_model()

# 执行主流程
if __name__ == "__main__":
    main()

# 关闭连接
try:
    mysql_cursor.close()
    mysql_conn.close()
    print("MySQL连接已关闭")
    
    neo4j_driver.close()
    print("Neo4j连接已关闭")
except Exception as e:
    print(f"关闭连接时出错: {e}")
