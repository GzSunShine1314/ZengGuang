# 需安装依赖：pip install neo4j mysql-connector-python

from neo4j import GraphDatabase
import pymysql
from pymysql import Error
import csv # Added for CSV export/import

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
        # 先获取总记录数，用于显示进度
        mysql_cursor.execute("SELECT COUNT(*) AS total FROM reference")
        total_count = mysql_cursor.fetchone()['total']
        print(f"开始导入数据，共 {total_count} 条记录")
        
        # 读取MySQL的reference表
        mysql_cursor.execute("SELECT * FROM reference")
        
        # 批量处理
        batch_size = 1000  # 每批1000条
        batch_data = []
        
        with neo4j_driver.session() as session:
            for i, record in enumerate(mysql_cursor, 1):
                batch_data.append({
                    'handle': record['handle'],
                    'from_node_type': record['from_node_type'],
                    'from_node': record['from_node'],
                    'to_node_type': record['to_node_type'],
                    'to_node': record['to_node'],
                    'description': record['description']
                })
                
                # 每1000条或最后一批时执行批量插入
                if len(batch_data) >= batch_size or i == total_count:
                    # 使用 UNWIND 进行批量插入
                    session.run("""
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
                    
                    batch_data = []  # 清空批次
                    
                    # 显示进度
                    progress = (i / total_count) * 100
                    print(f"已导入 {i}/{total_count} 条记录，进度：{progress:.2f}%")
        
        print(f"导入完成，共导入 {total_count} 个Reference节点")
        
    except Error as e:
        print(f"MySQL操作错误: {e}")
    except Exception as e:
        print(f"Neo4j操作错误: {e}")

def export_to_csv():
    """导出数据到CSV文件，然后使用LOAD CSV导入"""
    try:
        # 导出到CSV
        mysql_cursor.execute("SELECT * FROM reference")
        
        with open('reference_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['handle', 'from_node_type', 'from_node', 'to_node_type', 'to_node', 'description']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in mysql_cursor:
                writer.writerow(record)
        
        print("CSV文件导出完成")
        
        # 使用LOAD CSV导入Neo4j
        with neo4j_driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///reference_data.csv' AS row
                CREATE (r:Reference {
                    handle: row.handle,
                    from_node_type: row.from_node_type,
                    from_node: row.from_node,
                    to_node_type: row.to_node_type,
                    to_node: row.to_node,
                    description: row.description
                })
            """)
        
        print("LOAD CSV导入完成")
        
    except Exception as e:
        print(f"导出/导入错误: {e}")

# 执行导入
import_data()

# 关闭连接
try:
    mysql_cursor.close()
    mysql_conn.close()
    print("MySQL连接已关闭")
    
    neo4j_driver.close()
    print("Neo4j连接已关闭")
except Exception as e:
    print(f"关闭连接时出错: {e}")
