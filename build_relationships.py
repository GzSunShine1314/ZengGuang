# 构建Neo4j关系模型的脚本
from neo4j import GraphDatabase
import pymysql
from pymysql import Error

# 连接MySQL
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
    exit(1)

# 连接Neo4j
try:
    neo4j_driver = GraphDatabase.driver(
        "bolt://47.107.128.88:7687",
        auth=("neo4j", "neo4j_matrix")
    )
    
    with neo4j_driver.session() as session:
        result = session.run("RETURN 1")
        print("成功连接到Neo4j")

except Exception as e:
    print(f"Neo4j连接错误: {e}")
    exit(1)

def create_relationships():
    """创建Neo4j关系"""
    try:
        with neo4j_driver.session() as session:
            print("开始创建关系...")
            
            # 1. 创建销售订单号，客户物料编码 → 收货批 (下单) 关系
            print("创建销售订单到收货批的关系...")
            session.run("""
                MATCH (r1:Reference {from_node_type: "销售订单号，客户物料编码"})
                MATCH (r2:Reference {to_node_type: "收货批"})
                WHERE r1.from_node = r2.from_node
                CREATE (r1)-[:下单]->(r2)
            """)
            
            # 2. 创建收货批 → 库存id (理货到称重) 关系
            print("创建收货批到库存id的关系...")
            session.run("""
                MATCH (r1:Reference {from_node_type: "收货批"})
                MATCH (r2:Reference {to_node_type: "库存id"})
                WHERE r1.from_node = r2.from_node
                CREATE (r1)-[:理货到称重]->(r2)
            """)
            
            # 3. 创建库存id → SFC (称重到产出) 关系
            print("创建库存id到SFC的关系...")
            session.run("""
                MATCH (r1:Reference {from_node_type: "库存id"})
                MATCH (r2:Reference {to_node_type: "SFC"})
                WHERE r1.from_node = r2.from_node
                CREATE (r1)-[:称重到产出]->(r2)
            """)
            
            # 4. 创建SFC → SFC (SFC拆合记录) 关系
            print("创建SFC到SFC的关系...")
            session.run("""
                MATCH (r1:Reference {from_node_type: "SFC"})
                MATCH (r2:Reference {to_node_type: "SFC"})
                WHERE r1.from_node = r2.from_node AND r1.handle != r2.handle
                CREATE (r1)-[:SFC拆合记录]->(r2)
            """)
            
            # 5. 创建SFC → 库存id (包装入库) 关系
            print("创建SFC到库存id的关系...")
            session.run("""
                MATCH (r1:Reference {from_node_type: "SFC"})
                MATCH (r2:Reference {to_node_type: "库存id"})
                WHERE r1.from_node = r2.from_node
                CREATE (r1)-[:包装入库]->(r2)
            """)
            
            print("关系创建完成！")
            
    except Exception as e:
        print(f"创建关系时出错: {e}")

def create_entity_nodes():
    """创建实体节点（如果还没有的话）"""
    try:
        with neo4j_driver.session() as session:
            print("开始创建实体节点...")
            
            # 创建销售订单节点
            session.run("""
                MATCH (r:Reference {from_node_type: "销售订单号，客户物料编码"})
                MERGE (s:销售订单 {订单号: r.from_node})
                ON CREATE SET s.客户物料编码 = r.from_node
            """)
            
            # 创建收货批节点
            session.run("""
                MATCH (r:Reference {to_node_type: "收货批"})
                MERGE (b:收货批 {批次号: r.to_node})
            """)
            
            # 创建库存节点
            session.run("""
                MATCH (r:Reference {to_node_type: "库存id"})
                MERGE (i:库存 {库存ID: r.to_node})
            """)
            
            # 创建SFC节点
            session.run("""
                MATCH (r:Reference {to_node_type: "SFC"})
                MERGE (s:SFC {SFC编码: r.to_node})
            """)
            
            print("实体节点创建完成！")
            
    except Exception as e:
        print(f"创建实体节点时出错: {e}")

def create_entity_relationships():
    """创建实体之间的关系"""
    try:
        with neo4j_driver.session() as session:
            print("开始创建实体关系...")
            
            # 销售订单 → 收货批
            session.run("""
                MATCH (s:销售订单)
                MATCH (b:收货批)
                MATCH (r:Reference {from_node_type: "销售订单号，客户物料编码", to_node_type: "收货批"})
                WHERE s.订单号 = r.from_node AND b.批次号 = r.to_node
                MERGE (s)-[:下单]->(b)
            """)
            
            # 收货批 → 库存
            session.run("""
                MATCH (b:收货批)
                MATCH (i:库存)
                MATCH (r:Reference {from_node_type: "收货批", to_node_type: "库存id"})
                WHERE b.批次号 = r.from_node AND i.库存ID = r.to_node
                MERGE (b)-[:理货到称重]->(i)
            """)
            
            # 库存 → SFC
            session.run("""
                MATCH (i:库存)
                MATCH (s:SFC)
                MATCH (r:Reference {from_node_type: "库存id", to_node_type: "SFC"})
                WHERE i.库存ID = r.from_node AND s.SFC编码 = r.to_node
                MERGE (i)-[:称重到产出]->(s)
            """)
            
            # SFC → SFC (拆合关系)
            session.run("""
                MATCH (s1:SFC)
                MATCH (s2:SFC)
                MATCH (r:Reference {from_node_type: "SFC", to_node_type: "SFC"})
                WHERE s1.SFC编码 = r.from_node AND s2.SFC编码 = r.to_node AND s1.SFC编码 != s2.SFC编码
                MERGE (s1)-[:SFC拆合记录]->(s2)
            """)
            
            # SFC → 库存 (包装入库)
            session.run("""
                MATCH (s:SFC)
                MATCH (i:库存)
                MATCH (r:Reference {from_node_type: "SFC", to_node_type: "库存id"})
                WHERE s.SFC编码 = r.from_node AND i.库存ID = r.to_node
                MERGE (s)-[:包装入库]->(i)
            """)
            
            print("实体关系创建完成！")
            
    except Exception as e:
        print(f"创建实体关系时出错: {e}")

def query_graph():
    """查询图结构"""
    try:
        with neo4j_driver.session() as session:
            print("\n=== 图结构查询结果 ===")
            
            # 查询节点数量
            result = session.run("MATCH (n) RETURN labels(n) as labels, count(n) as count")
            print("节点统计:")
            for record in result:
                print(f"  {record['labels']}: {record['count']} 个")
            
            # 查询关系数量
            result = session.run("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count")
            print("\n关系统计:")
            for record in result:
                print(f"  {record['type']}: {record['count']} 条")
            
            # 查询一个完整的路径示例
            print("\n完整路径示例:")
            result = session.run("""
                MATCH path = (s:销售订单)-[:下单]->(b:收货批)-[:理货到称重]->(i:库存)-[:称重到产出]->(sfc:SFC)
                RETURN path LIMIT 1
            """)
            for record in result:
                print(f"  找到路径: {record['path']}")
                
    except Exception as e:
        print(f"查询图结构时出错: {e}")

if __name__ == "__main__":
    try:
        # 1. 创建实体节点
        create_entity_nodes()
        
        # 2. 创建实体关系
        create_entity_relationships()
        
        # 3. 查询图结构
        query_graph()
        
        print("\n关系模型构建完成！")
        
    except Exception as e:
        print(f"执行过程中出错: {e}")
    finally:
        # 关闭连接
        try:
            mysql_cursor.close()
            mysql_conn.close()
            print("MySQL连接已关闭")
            
            neo4j_driver.close()
            print("Neo4j连接已关闭")
        except Exception as e:
            print(f"关闭连接时出错: {e}")
