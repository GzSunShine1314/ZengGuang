import pandas as pd
import pyarrow.parquet as pq
import os
from typing import List, Dict, Any
import logging
import duckdb
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SQLParquetProcessor:
    """使用SQL直接查询.parquet文件的处理器，并将结果保存到数据湖"""
    
    def __init__(self, data_lake_base_path: str):
        """
        初始化DuckDB连接
        
        Args:
            data_lake_base_path: 数据湖基础路径
        """
        self.conn = duckdb.connect()
        self.table_paths = {}
        self.data_lake_base_path = data_lake_base_path
        
    def register_parquet_table(self, table_name: str, file_path: str):
        """
        注册.parquet文件为SQL表
        
        Args:
            table_name: SQL中使用的表名
            file_path: .parquet文件的完整路径
        """
        try:
            # 检查文件是否存在
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")
                
            # 注册表
            self.conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')")
            self.table_paths[table_name] = file_path
            
            # 获取表信息
            result = self.conn.execute(f"SELECT COUNT(*) as count FROM {table_name}").fetchone()
            logger.info(f"表 {table_name} 注册成功，行数: {result[0]}")
            
        except Exception as e:
            logger.error(f"注册表 {table_name} 失败: {str(e)}")
            raise
    
    def execute_sql_query(self, sql: str) -> pd.DataFrame:
        """
        执行SQL查询
        
        Args:
            sql: SQL查询语句
            
        Returns:
            查询结果DataFrame
        """
        try:
            logger.info(f"执行SQL查询: {sql[:100]}...")
            result = self.conn.execute(sql).fetchdf()
            logger.info(f"查询完成，返回 {len(result)} 行数据")
            return result
            
        except Exception as e:
            logger.error(f"SQL查询执行失败: {str(e)}")
            raise
    
    def save_to_data_lake(self, df: pd.DataFrame, table_name: str, output_dir: str = None):
        """
        将DataFrame保存到数据湖指定路径
        
        Args:
            df: 要保存的DataFrame
            table_name: 表名
            output_dir: 输出目录（可选，默认使用数据湖基础路径）
        """
        try:
            # 确定输出目录
            if output_dir is None:
                output_dir = self.data_lake_base_path
            
            # 确保输出目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 生成带时间戳的文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{table_name}_{timestamp}.snappy.parquet"
            full_path = os.path.join(output_dir, filename)
            
            # 保存为.parquet文件
            df.to_parquet(full_path, index=False, compression='snappy')
            logger.info(f"表 {table_name} 已保存到数据湖: {full_path}")
            
            # 同时保存一个不带时间戳的版本（用于覆盖）
            latest_filename = f"{table_name}_latest.snappy.parquet"
            latest_path = os.path.join(output_dir, latest_filename)
            df.to_parquet(latest_path, index=False, compression='snappy')
            logger.info(f"表 {table_name} 最新版本已保存: {latest_path}")
            
            return full_path
            
        except Exception as e:
            logger.error(f"保存表 {table_name} 到数据湖失败: {str(e)}")
            raise
    
    def process_reference_relationships(self) -> pd.DataFrame:
        """
        处理reference表的关系数据
        使用您原有的SQL逻辑
        """
        logger.info("开始处理reference表关系数据...")
        
        # 第一个INSERT语句（主要关系）
        sql1 = """
        WITH t1 AS (
            SELECT "SFC" as "from_node_type"
            ,substring_index(source_reference_value,',',-1) as "from_node"
            ,"SFC" as "to_node_type"
            ,substring_index(target_value,',',-1) as "to_node"
            ,"SFC拆合记录" as "description" 
            FROM mes_pd_lot_history t1 WHERE source_type="SFC"
        ),
        t2 AS (
            SELECT "库存id" as "from_node_type"
            ,c.inventory_id as "from_node"
            ,"SFC" as "to_node_type"
            ,sfc_code as "to_node"
            ,"称重到产出" as "description" 
            FROM mes_wm_inventory c 
            JOIN mes_pd_item_assy t1 ON c.inventory_id=t1.inventory_id 
            JOIN mes_pd_sfc d ON t1.related_project=d.primary_key
            WHERE t1.related_project_type="SFC" 
            AND sfc_code IN (SELECT from_node FROM t1)
        ),
        t3 AS (
            SELECT "收货批" as "from_node_type"
            ,receiving_batch_no as "from_node"
            ,"库存id" as "to_node_type"
            ,inventory_id as "to_node"
            ,"理货到称重" as "description" 
            FROM mes_wm_inventory c 
            JOIN mes_sales_order_detail t1 ON c.receiving_batch_number=t1.receiving_batch_no
            WHERE inventory_id IN (SELECT from_node FROM t2) 
            GROUP BY receiving_batch_no,inventory_id
        ),
        t4 AS (
            SELECT "销售订单号，客户物料编码" as "from_node_type"
            ,substring_index(primary_key,',',-2) as "from_node"
            ,"收货批" as "to_node_type"
            ,receiving_batch_no as "to_node"
            ,"下单" as "description"
            FROM mes_sales_order_detail 
            WHERE receiving_batch_no IN (SELECT from_node FROM t3)
        )
        SELECT * FROM t4
        UNION ALL
        SELECT * FROM t3
        UNION ALL 
        SELECT * FROM t2
        UNION ALL
        SELECT * FROM t1
        """
        
        # 执行第一个查询
        result1 = self.execute_sql_query(sql1)
        
        # 第二个INSERT语句（包装入库关系）
        sql2 = """
        SELECT "SFC" as "from_node_type"
        ,substring_index(target_value,',',-1) as "from_node"
        ,"库存id" as "to_node_type"
        ,inventory_id as "to_node"
        ,"包装入库" as "description" 
        FROM mes_pd_lot_history c 
        JOIN mes_wm_inventory t1 ON substring_index(c.target_value,',',-1)=t1.inventory_id
        WHERE storage_location_idx!="None" 
        GROUP BY substring_index(target_value,',',-1),inventory_id
        """
        
        # 执行第二个查询
        result2 = self.execute_sql_query(sql2)
        
        # 合并结果
        all_results = pd.concat([result1, result2], ignore_index=True)
        
        # 添加handle列（自增ID）
        all_results.insert(0, 'handle', range(1, len(all_results) + 1))
        
        # 添加weigth列（设为空值）
        all_results['weigth'] = None
        
        logger.info(f"所有关系数据处理完成，共 {len(all_results)} 条")
        return all_results
    
    def find_missing_sales_orders(self) -> pd.DataFrame:
        """
        查找缺失的销售订单
        使用您原有的SELECT查询
        """
        logger.info("开始查找缺失的销售订单...")
        
        sql = """
        SELECT substring_index(primary_key,',',-2) as missing_sales_order
        FROM mes_sales_order_detail 
        WHERE substring_index(primary_key,',',-2) NOT IN (
            SELECT from_node 
            FROM reference_latest 
            WHERE from_node_type="销售订单号，客户物料编码"
        )
        """
        
        result = self.execute_sql_query(sql)
        logger.info(f"缺失销售订单查询完成，共 {len(result)} 条")
        return result
    
    def create_reference_table(self):
        """创建reference表结构"""
        create_table_sql = """
        CREATE TABLE reference (
            handle BIGINT AUTO_INCREMENT,
            from_node_type VARCHAR(255),
            from_node VARCHAR(255),
            to_node_type VARCHAR(255),
            to_node VARCHAR(255),
            weigth VARCHAR(255),
            description VARCHAR(255)
        )
        """
        
        try:
            self.conn.execute(create_table_sql)
            logger.info("reference表创建成功")
        except Exception as e:
            logger.warning(f"创建表失败（可能已存在）: {str(e)}")
    
    def drop_reference_table(self):
        """删除reference表"""
        try:
            self.conn.execute("DROP TABLE IF EXISTS reference")
            logger.info("reference表删除成功")
        except Exception as e:
            logger.error(f"删除表失败: {str(e)}")
    
    def save_reference_to_data_lake(self, df: pd.DataFrame, output_dir: str = None):
        """
        将reference表保存到数据湖
        
        Args:
            df: reference表DataFrame
            output_dir: 输出目录（可选）
        """
        try:
            # 保存到数据湖
            saved_path = self.save_to_data_lake(df, 'reference', output_dir)
            
            # 同时注册为视图，方便后续查询
            self.conn.execute(f"CREATE OR REPLACE VIEW reference_latest AS SELECT * FROM read_parquet('{saved_path}')")
            logger.info("reference表已注册为视图，可用于后续查询")
            
            return saved_path
            
        except Exception as e:
            logger.error(f"保存reference表到数据湖失败: {str(e)}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        self.conn.close()
        logger.info("数据库连接已关闭")

def main():
    """主函数示例"""
    # 数据湖基础路径 - 请修改为实际路径
    data_lake_base_path = r"C:\data_lake\reference_tables"
    
    # 初始化处理器
    processor = SQLParquetProcessor(data_lake_base_path)
    
    try:
        # 配置表路径 - 请根据实际情况修改这些路径
        table_configs = {
            'mes_pd_lot_history': r'C:\path\to\mes_pd_lot_history.snappy.parquet',
            'mes_wm_inventory': r'C:\path\to\mes_wm_inventory.snappy.parquet',
            'mes_pd_item_assy': r'C:\path\to\mes_pd_item_assy.snappy.parquet',
            'mes_pd_sfc': r'C:\path\to\mes_pd_sfc.snappy.parquet',
            'mes_sales_order_detail': r'C:\path\to\mes_sales_order_detail.snappy.parquet'
        }
        
        # 注册所有表
        logger.info("开始注册.parquet文件...")
        for table_name, file_path in table_configs.items():
            processor.register_parquet_table(table_name, file_path)
        
        # 创建reference表
        processor.create_reference_table()
        
        # 处理关系数据
        logger.info("开始处理关系数据...")
        relationships_df = processor.process_reference_relationships()
        
        # 保存reference表到数据湖
        logger.info("保存reference表到数据湖...")
        reference_path = processor.save_reference_to_data_lake(relationships_df)
        
        # 查找缺失的销售订单
        missing_orders_df = processor.find_missing_sales_orders()
        
        # 保存缺失销售订单到数据湖
        if len(missing_orders_df) > 0:
            processor.save_to_data_lake(missing_orders_df, 'missing_sales_orders')
        
        # 显示结果统计
        print("\n=== 关系数据统计 ===")
        print(f"总关系数: {len(relationships_df)}")
        print("\n按描述统计:")
        print(relationships_df['description'].value_counts())
        
        print(f"\n缺失的销售订单数: {len(missing_orders_df)}")
        if len(missing_orders_df) > 0:
            print("前10个缺失的销售订单:")
            print(missing_orders_df.head(10))
        
        print(f"\n=== 数据湖保存路径 ===")
        print(f"Reference表: {reference_path}")
        print(f"数据湖基础路径: {data_lake_base_path}")
        
    except Exception as e:
        logger.error(f"处理过程中发生错误: {str(e)}")
        raise
    
    finally:
        # 清理资源
        processor.close()

if __name__ == "__main__":
    main()