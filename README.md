#查询索引
SHOW INDEXES
YIELD id, name, labelsOrTypes
WHERE labelsOrTypes = ["Reference"] 
RETURN id, name; 
#删除索引
DROP INDEX `reference_to_node`;
#删除节点
MATCH (n:Reference)
WITH n LIMIT 50000
DETACH DELETE n
#删除关系
MATCH ()-[r:SFC拆合记录]->()
DELETE r

; // 为业务节点创建索引
; CREATE INDEX sales_order_node_id IF NOT EXISTS FOR (s:SalesOrder) ON (s.node_id);
; CREATE INDEX receiving_batch_id IF NOT EXISTS FOR (rb:ReceivingBatch) ON (rb.batch_id);
; CREATE INDEX inventory_id IF NOT EXISTS FOR (i:Inventory) ON (i.inventory_id);
; CREATE INDEX sfc_code IF NOT EXISTS FOR (sfc:SFC) ON (sfc.sfc_code);
; CREATE INDEX production_log_sfc_no IF NOT EXISTS FOR (pl:mes_pd_production_log_silver) ON (pl.sfc_no);

; // 为关系属性创建索引
; CREATE INDEX relationship_description IF NOT EXISTS FOR ()-[r:下单]-() ON (r.description);
; CREATE INDEX relationship_created_at IF NOT EXISTS FOR ()-[r:包装入库]-() ON (r.created_at);

; // 复合索引
; CREATE INDEX production_log_sfc_status IF NOT EXISTS FOR (pl:mes_pd_production_log_silver) ON (pl.sfc_no, pl.processing_status);

// 为节点类型建立索引
CREATE INDEX node_type_index IF NOT EXISTS FOR (n:Node) ON (n.node_type);

// 为节点值建立索引
CREATE INDEX node_value_index IF NOT EXISTS FOR (n:Node) ON (n.node_value);

// 为关系类型建立索引
CREATE INDEX relationship_type_index IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.relationship_type);

// 复合索引
CREATE INDEX node_type_value_index IF NOT EXISTS FOR (n:Node) ON (n.node_type, n.node_value);

// 创建约束确保节点唯一性
CREATE CONSTRAINT node_unique_constraint IF NOT EXISTS FOR (n:Node) REQUIRE n.node_id IS UNIQUE;

// 创建节点标签约束
CREATE CONSTRAINT sfc_node_constraint IF NOT EXISTS FOR (n:SFC) REQUIRE n.sfc_code IS UNIQUE;
CREATE CONSTRAINT inventory_node_constraint IF NOT EXISTS FOR (n:Inventory) REQUIRE n.inventory_id IS UNIQUE;
CREATE CONSTRAINT receiving_batch_node_constraint IF NOT EXISTS FOR (n:ReceivingBatch) REQUIRE n.batch_no IS UNIQUE;
CREATE CONSTRAINT sales_order_node_constraint IF NOT EXISTS FOR (n:SalesOrder) REQUIRE n.order_key IS UNIQUE;

// 创建销售订单节点
MATCH (r:Reference)
WHERE r.from_node_type = "销售订单号，客户物料编码"
MERGE (s:SalesOrder {node_id: r.from_node})
SET s.node_type = "销售订单号，客户物料编码";
// 创建收货批节点
MATCH (r:Reference)
WHERE r.from_node_type = "收货批" OR r.to_node_type = "收货批"
MERGE (rb:ReceivingBatch {batch_no: COALESCE(r.from_node, r.to_node)})
SET rb.node_type = "收货批";
// 创建库存节点
MATCH (r:Reference)
WHERE r.from_node_type = "库存id" OR r.to_node_type = "库存id"
MERGE (i:Inventory {inventory_id: COALESCE(r.from_node, r.to_node)})
SET i.node_type = "库存id";
// 创建SFC节点
MATCH (r:Reference)
WHERE r.from_node_type = "SFC" OR r.to_node_type = "SFC"
MERGE (sfc:SFC {sfc_code: COALESCE(r.from_node, r.to_node)})
SET sfc.node_type = "SFC";
1. 下单关系
MATCH (r:Reference)
WHERE r.description = "下单"
MATCH (s:SalesOrder {node_id: r.from_node})
MATCH (rb:ReceivingBatch {batch_no: r.to_node})
MERGE (s)-[:下单 {description: r.description}]->(rb);
2. 理货到称重关系
MATCH (r:Reference)
WHERE r.description = "理货到称重"
MATCH (rb:ReceivingBatch {batch_no: r.from_node})
MATCH (i:Inventory {inventory_id: r.to_node})
MERGE (rb)-[:理货到称重 {description: r.description}]->(i);
3. 称重到产出关系
MATCH (r:Reference)
WHERE r.description = "称重到产出"
MATCH (i:Inventory {inventory_id: r.from_node})
MATCH (sfc:SFC {sfc_code: r.to_node})
MERGE (i)-[:称重到产出 {description: r.description}]->(sfc);
4. SFC拆合记录关系
MATCH (r:Reference)
WHERE r.description = "SFC拆合记录"
MATCH (sfc1:SFC {sfc_code: r.from_node})
MATCH (sfc2:SFC {sfc_code: r.to_node})
MERGE (sfc1)-[:SFC拆合记录 {description: r.description}]->(sfc2);
5. 包装入库关系
MATCH (r:Reference)
WHERE r.description = "包装入库"
MATCH (sfc:SFC {sfc_code: r.from_node})
MATCH (i:Inventory {inventory_id: r.to_node})
MERGE (sfc)-[:包装入库 {description: r.description}]->(i);

关系模型创建：
1. 下单关系（销售订单 → 收货批）
MATCH (r:Reference)
WHERE r.description = "下单"
MATCH (s:SalesOrder {node_id: r.from_node})
MATCH (rb:ReceivingBatch {batch_no: r.to_node})
MERGE (s)-[:下单 {description: r.description}]->(rb);
2. 理货到称重关系（收货批 → 库存）
MATCH (r:Reference)
WHERE r.description = "理货到称重"
MATCH (rb:ReceivingBatch {batch_no: r.from_node})
MATCH (i:Inventory {inventory_id: r.to_node})
MERGE (rb)-[:理货到称重 {description: r.description}]->(i);
3. 称重到产出关系（库存 → SFC）
MATCH (r:Reference)
WHERE r.description = "称重到产出"
MATCH (i:Inventory {inventory_id: r.from_node})
MATCH (sfc:SFC {sfc_code: r.to_node})
MERGE (i)-[:称重到产出 {description: r.description}]->(sfc);
4. SFC拆合记录关系（SFC → SFC）
MATCH (r:Reference)
WHERE r.description = "SFC拆合记录"
MATCH (sfc1:SFC {sfc_code: r.from_node})
MATCH (sfc2:SFC {sfc_code: r.to_node})
MERGE (sfc1)-[:SFC拆合记录 {description: r.description}]->(sfc2);
5. 包装入库关系（SFC → 库存）
MATCH (r:Reference)
WHERE r.description = "包装入库"
MATCH (sfc:SFC {sfc_code: r.from_node})
MATCH (i:Inventory {inventory_id: r.to_node})
MERGE (sfc)-[:包装入库 {description: r.description}]->(i);


// 在Neo4j浏览器中可视化显示关系
MATCH (s:SalesOrder {node_id: "SO-25-01376,3"})
OPTIONAL MATCH (s)-[r1:下单]->(rb:ReceivingBatch)
OPTIONAL MATCH (rb)-[r2:理货到称重]->(i1:Inventory)
OPTIONAL MATCH (i1)-[r3:称重到产出]->(sfc:SFC)
OPTIONAL MATCH (sfc)-[r4:SFC拆合记录]->(sfc2:SFC)
OPTIONAL MATCH (sfc2)-[r5:包装入库]->(i2:Inventory)
RETURN s, rb, i1, sfc, sfc2, i2, r1, r2, r3, r4, r5;




方法1：关联生产日志（推荐）
MATCH (s:SalesOrder {node_id: "SO-25-01376,3"})
OPTIONAL MATCH (s)-[r1:下单]->(rb:ReceivingBatch)
OPTIONAL MATCH (rb)-[r2:理货到称重]->(i1:Inventory)
OPTIONAL MATCH (i1)-[r3:称重到产出]->(sfc:SFC)
OPTIONAL MATCH (sfc)-[r4:SFC拆合记录]->(sfc2:SFC)
OPTIONAL MATCH (sfc2)-[r5:包装入库]->(i2:Inventory)
OPTIONAL MATCH (pl:mes_pd_production_log_silver {sfc_no: sfc.sfc_code, operation_status_code: "COMPLETE"})
RETURN DISTINCT s, rb, i1, sfc, sfc2, i2, pl, r1, r2, r3, r4, r5;

// 创建源SFC到工序的关系
MATCH (sfc:SFC)
MATCH (pl:mes_pd_production_log_silver {sfc_no: sfc.sfc_code, processing_status: "COMPLETE"})
MERGE (sfc)-[:包含工序 {process_step: pl.process_step, weight: pl.weight}]->(pl);

// 使用优化后的查询创建工序顺序关系
MATCH (pl:mes_pd_production_log_silver)
WHERE pl.processing_status = "COMPLETE"
WITH pl.sfc_no as sfc_no, pl.created_at as created_at, pl as pl
ORDER BY sfc_no, created_at
WITH sfc_no, COLLECT({pl: pl, created_at: created_at}) as processes
UNWIND RANGE(0, SIZE(processes)-2) as i
WITH processes[i].pl as pl1, processes[i+1].pl as pl2
MERGE (pl1)-[:下一工序]->(pl2);

// 删除所有"包含工序"关系
MATCH ()-[r:包含工序]->()
DELETE r;

// 为每个SFC找到最早的工序，并创建"开始工序"关系
MATCH (sfc:SFC)
MATCH (pl:mes_pd_production_log_silver {sfc_no: sfc.sfc_code, processing_status: "COMPLETE"})
WITH sfc, pl, pl.created_at as created_at
ORDER BY sfc.sfc_code, created_at
WITH sfc, HEAD(COLLECT({pl: pl, created_at: created_at})) as first_process
WITH sfc, first_process.pl as first_pl
MERGE (sfc)-[:开始工序 {process_step: first_pl.process_step, weight: first_pl.weight}]->(first_pl);
