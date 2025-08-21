def pandas_read_bronze_data(context: AssetExecutionContext, minio: MinIOResource,bronze_path):
    context.log.info(f"📖 正在使用pandas读取Bronze层Delta Lake数据: {bronze_path}")
    
    try:
        from deltalake import DeltaTable
        
        # 确保路径格式正确
        if not bronze_path.endswith('/'):
            bronze_path += '/'
        
        context.log.info(f"🔍 尝试读取Delta Lake表: {bronze_path}")
        
        # 读取Delta Lake表
        delta_table = DeltaTable(bronze_path, storage_options=minio.get_storage_options())
        bronze_data = delta_table.to_pandas()
        
        context.log.info(f"✅ Bronze层数据读取成功，共 {len(bronze_data)} 条记录")
        context.log.info(f"📊 数据列名: {list(bronze_data.columns)}")
        
    except Exception as e:
        context.log.error(f"❌ Bronze层数据读取失败: {e}")
        context.log.error("请确保Bronze层数据已正确摄取")
        raise
    return bronze_data

def identify_key_fields_exist(context: AssetExecutionContext, bronze_data, required_fields):
    missing_fields = [field for field in required_fields if field not in bronze_data.columns]
    if missing_fields:
        context.log.error(f"❌ 缺少关键字段: {missing_fields}")
        raise ValueError(f"缺少关键字段: {missing_fields}")
    context.log.info("✅ 所有关键字段都存在")

def process_datetime_field(bronze_data, source_field, target_field, cleaned_data, context):
    """
    处理日期时间字段
    
    Args:
        bronze_data: 原始数据
        source_field: 源字段名
        target_field: 目标字段名
        cleaned_data: 清洗后的数据
        context: 上下文对象
    
    Returns:
        cleaned_data: 更新后的清洗数据
    """
    # 日期时间字段处理 - 格式化为指定格式
    datetime_series = pd.to_datetime(
        bronze_data[source_field], errors='coerce'
    ).dt.floor('S')
    
    # 格式化为指定格式的字符串，确保不包含非法字符
    cleaned_data[target_field] = datetime_series.dt.strftime('%Y/%m/%d %H:%M:%S')
    
    # 确保时间格式正确，处理可能的异常值
    cleaned_data[target_field] = cleaned_data[target_field].apply(
        lambda x: x if pd.isna(x) or (isinstance(x, str) and len(x) > 0) else "未知时间"
    )
    
    return cleaned_data

def process_numeric_field(bronze_data, source_field, target_field,numeric_fields, cleaned_data, context):
    """
    处理数值字段
    
    Args:
        bronze_data: 原始数据
        source_field: 源字段名
        target_field: 目标字段名
        cleaned_data: 清洗后的数据
        context: 上下文对象
    
    Returns:
        cleaned_data: 更新后的清洗数据
    """
    # 数值字段处理
    default_value = 0.000
    cleaned_data[target_field] = pd.to_numeric(
        bronze_data[source_field], errors='coerce'
    ).fillna(default_value)
    
    # 检查负数值
    negative_mask = cleaned_data[target_field] < 0
    negative_count = negative_mask.sum()
    
    if negative_count > 0:
        context.log.warning(f"⚠️ 发现 {negative_count} 条记录的 {source_field} 为负数")
        # 删除该字段为负数的记录
        cleaned_data = cleaned_data[~negative_mask]
        context.log.info(f"✅ 已删除 {negative_count} 条 {source_field} 为负数的记录")
    
    # 特殊处理字段
    if source_field in numeric_fields:
        cleaned_data[target_field] = cleaned_data[target_field].round(3)
        context.log.info(f"✅ {source_field} 字段已处理，保留三位小数")
    
    return cleaned_data

def process_string_field(bronze_data, source_field, target_field, cleaned_data):
    """
    处理字符串字段
    
    Args:
        bronze_data: 原始数据
        source_field: 源字段名
        target_field: 目标字段名
        cleaned_data: 清洗后的数据
    
    Returns:
        cleaned_data: 更新后的清洗数据
    """
    cleaned_data[target_field] = bronze_data[source_field].fillna('').astype(str)
    return cleaned_data

def process_field_mapping(bronze_data, field_mapping, datetime_fields, numeric_fields, cleaned_data, context):
    """
    处理字段映射
    
    Args:
        bronze_data: 原始数据
        field_mapping: 字段映射字典
        datetime_fields: 日期时间字段列表
        numeric_fields: 数值字段列表
        cleaned_data: 清洗后的数据
        context: 上下文对象
    
    Returns:
        cleaned_data: 更新后的清洗数据
    """
    for source_field, target_field in field_mapping.items():
        if source_field in bronze_data.columns:
            if source_field in datetime_fields:
                cleaned_data = process_datetime_field(
                    bronze_data, source_field, target_field, cleaned_data, context
                )
            elif source_field in numeric_fields:
                cleaned_data = process_numeric_field(
                    bronze_data, source_field, target_field, numeric_fields, cleaned_data, context
                )
            else:
                cleaned_data = process_string_field(
                    bronze_data, source_field, target_field, cleaned_data
                )
        else:
            context.log.warning(f"⚠️ 源字段 '{source_field}' 在bronze数据中不存在")
    
    return cleaned_data

def process_special_bo_fields(bronze_data, field_mapping, cleaned_data, special_process_fields, context):
    for source_field, target_field in field_mapping.items():
        if source_field in special_process_fields:
            cleaned_data[target_field] = bronze_data[source_field].astype(str)
            # 替换空值、'*'、'nan'、'None'等为'NULL'
            cleaned_data[target_field] = cleaned_data[target_field].replace([
                '', 'nan', 'None', 'NULL', '*', 'null', 'NaN', 'NAN'
            ], 'NULL')
    context.log.info(f"✅ {source_field} 字段特殊处理完成：空值和'*'已替换为NULL")
    return cleaned_data

def process_add_field(bronze_data, field_mapping, cleaned_data, add_field, add_field_context, context):
    if add_field not in cleaned_data.columns:
        cleaned_data[add_field] = add_field_context
        context.log.info(f"✅ 添加 {add_field} 字段，默认值设置为 {add_field_context}")
    return cleaned_data

def time_logic_check(cleaned_data,start_time,end_time,context):
    create_times = pd.to_datetime(cleaned_data[start_time], errors='coerce')
    update_times = pd.to_datetime(cleaned_data[end_time], errors='coerce')
    invalid_time_logic = (create_times > update_times) & (create_times.notna()) & (update_times.notna())
    invalid_records_count = invalid_time_logic.sum()
    
    if invalid_records_count > 0:
        context.log.warning(f"⚠️ 发现 {invalid_records_count} 条记录的CREATE_TIME大于UPDATE_TIME，将被删除")
        
        # 显示被删除的记录信息
        invalid_indices = invalid_time_logic[invalid_time_logic].index
        for idx in invalid_indices[:5]:  # 只显示前5条
            context.log.warning(f"  删除记录 {idx}: CREATE_TIME={cleaned_data.loc[idx, 'created_at']}, UPDATE_TIME={cleaned_data.loc[idx, 'updated_at']}")
        
        if invalid_records_count > 5:
            context.log.warning(f"  ... 还有 {invalid_records_count - 5} 条类似记录")
        
        # 删除不符合时间逻辑的记录
        cleaned_data = cleaned_data[~invalid_time_logic]
        context.log.info(f"✅ 已删除 {invalid_records_count} 条时间逻辑错误的记录")
    else:
        context.log.info("✅ 所有记录的时间逻辑都正确")
    return cleaned_data

def test_null_count(cleaned_data,context):
    for col in cleaned_data.columns:
        null_count = cleaned_data[col].isnull().sum()
        if null_count > 0:
            context.log.warning(f"⚠️ 列 '{col}' 有 {null_count} 个空值")
        
        # 检查字符串长度
        if cleaned_data[col].dtype == 'object':
            max_length = cleaned_data[col].astype(str).str.len().max()
            context.log.info(f"📏 列 '{col}' 最大字符串长度: {max_length}")

def clean_dataframe_for_excel(clean_data, datetime_fields):
    """
    仅删除任意列中包含 Excel 非法控制字符或 '@' 的整行；不做其他任何收尾处理。
    """
    if clean_data is None or len(clean_data) == 0:
        return clean_data

    # Excel 禁止的控制字符（除 \t \n \r 外）+ '@'
    pattern = r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F@]'

    # 全表扫描（包含时间列），只删行不改值
    row_bad = clean_data.astype(str).apply(lambda c: c.str.contains(pattern, regex=True, na=False)).any(axis=1)
    bad_count = int(row_bad.sum())
    if bad_count:
        clean_data = clean_data.loc[~row_bad]
    return clean_data

def clean_invalid_process_field_characters(cleaned_data, process_field, context):
    """
    清除process_field中包含非法字符的记录
    
    Args:
        cleaned_data: 清洗后的数据DataFrame
        process_field: 清洗后的字段名
        context: Dagster执行上下文
    
    Returns:
        cleaned_data: 清除非法记录后的数据DataFrame
    """
    context.log.info(f"🔍 开始检查{process_field}中的非法字符...")
    
    if process_field not in cleaned_data.columns:
        context.log.warning(f"⚠️ {process_field} 列不存在，跳过非法字符检查")
        return cleaned_data
    
    original_count = len(cleaned_data)
    
    # 确保process_field为字符串类型
    cleaned_data[process_field] = cleaned_data[process_field].astype(str)
    
    # 定义非法字符模式
    illegal_patterns = [
        r'@',                     # @ 符号
        r'^\s*[A-Z]\s*$',        # 单独的大写字母（如单独的Z）
        r'[\x00-\x1f\x7f-\x9f]', # 控制字符
        r'[\t\n\r\f\v]',         # 制表符、换行符等
        r'[#$%^&*()+=\[\]{}|\\:";\'<>?,]', # 特殊符号
        r'^\s*$',                # 只包含空白字符
        r'^NULL$|^null$|^Null$', # NULL值
        r'^\*+$',                # 只包含星号
        r'^-+$'                  # 只包含破折号
    ]
    
    # 创建布尔掩码标识无效记录
    invalid_mask = pd.Series([False] * len(cleaned_data), index=cleaned_data.index)
    
    # 逐个检查每种非法模式
    for i, pattern in enumerate(illegal_patterns):
        try:
            pattern_mask = cleaned_data[process_field].str.contains(pattern, regex=True, na=False)
            pattern_count = pattern_mask.sum()
            
            if pattern_count > 0:
                context.log.warning(f"⚠️ 检测到 {pattern_count} 条包含模式 '{pattern}' 的记录")
                
                # 显示匹配的样本值
                sample_values = cleaned_data[pattern_mask][process_field].head(3).tolist()
                context.log.warning(f"  样本值: {sample_values}")
                
                invalid_mask |= pattern_mask
                
        except Exception as e:
            context.log.error(f"❌ 检查模式 '{pattern}' 时出错: {e}")
            continue
    
    # 额外检查：异常长度
    length_mask = (cleaned_data[process_field].str.len() < 2) | (cleaned_data[process_field].str.len() > 50)
    length_count = length_mask.sum()
    
    if length_count > 0:
        context.log.warning(f"⚠️ 发现 {length_count} 条{process_field}长度异常的记录（<2或>50字符）")
        invalid_mask |= length_mask
    
    # 统计并处理无效记录
    total_invalid_count = invalid_mask.sum()
    
    if total_invalid_count > 0:
        context.log.warning(f"⚠️ 总计发现 {total_invalid_count} 条包含非法{process_field}的记录")
        
        # 显示被删除记录的详细信息
        invalid_records = cleaned_data[invalid_mask]
        context.log.warning("📋 被删除的记录详情（前5条）:")
        
        for i, (idx, row) in enumerate(invalid_records.head(5).iterrows()):
            context.log.warning(f"  记录 {i+1}: 索引={idx}, {process_field}='{row[process_field]}'")
        
        if total_invalid_count > 5:
            context.log.warning(f"  ... 还有 {total_invalid_count - 5} 条类似记录")
        
        # 删除无效记录
        cleaned_data = cleaned_data[~invalid_mask].copy()
        
        final_count = len(cleaned_data)
        removed_count = original_count - final_count
        cleanup_rate = (removed_count / original_count) * 100 if original_count > 0 else 0
        
        context.log.info(f"✅ 已删除 {removed_count} 条非法{process_field}记录")
        context.log.info(f"📊 清理统计: 原始={original_count}条, 清理后={final_count}条, 清理率={cleanup_rate:.2f}%")
        
        # 数据质量警告
        if cleanup_rate > 15:
            context.log.warning(f"⚠️ 数据清理率较高 ({cleanup_rate:.2f}%)，建议检查上游数据质量")
        elif cleanup_rate > 5:
            context.log.info(f"ℹ️ 数据清理率正常 ({cleanup_rate:.2f}%)")
            
    else:
        context.log.info(f"✅ 所有{process_field}字段都是有效的，无需清理")
    
    return cleaned_data

def clean_short_process_field(cleaned_data, process_field, context, min_length=5):
    """
    清除process_field长度不超过指定字符数的记录
    
    Args:
        cleaned_data: 清洗后的数据DataFrame
        process_field: 清洗后的字段名
        context: Dagster执行上下文
        min_length: 最小长度要求，默认为5
    
    Returns:
        cleaned_data: 清除指定字段长度不符合要求的记录后的数据DataFrame
    """
    context.log.info(f"🔍 开始检查{process_field}长度（最小要求: {min_length}个字符）...")
    
    if process_field not in cleaned_data.columns:
        context.log.warning(f"⚠️ {process_field} 列不存在，跳过长度检查")
        return cleaned_data
    
    original_count = len(cleaned_data)
    
    # 确保process_field为字符串类型
    cleaned_data[process_field] = cleaned_data[process_field].astype(str)
    
    # 计算每个process_field的长度
    id_lengths = cleaned_data[process_field].str.len()
    
    # 找出长度不超过指定字符数的记录
    short_id_mask = id_lengths <= min_length
    short_id_count = short_id_mask.sum()
    
    if short_id_count > 0:
        context.log.warning(f"⚠️ 发现 {short_id_count} 条{process_field}长度不超过{min_length}个字符的记录")
        
        # 显示被删除记录的详细信息
        short_records = cleaned_data[short_id_mask]
        context.log.warning(f"📋 被删除的短{process_field}记录详情（前10条）:")
        
        for i, (idx, row) in enumerate(short_records.head(10).iterrows()):
            id_value = row[process_field]
            id_length = len(str(id_value))
            context.log.warning(f"  记录 {i+1}: 索引={idx}, {process_field}='{id_value}', 长度={id_length}")
        
        if short_id_count > 10:
            context.log.warning(f"  ... 还有 {short_id_count - 10} 条类似记录")
        
        # 统计长度分布（用于分析数据质量）
        length_distribution = id_lengths[short_id_mask].value_counts().sort_index()
        context.log.info(f"📊 短{process_field}长度分布:")
        for length, count in length_distribution.items():
            context.log.info(f"  长度 {length}: {count} 条记录")
        
        # 删除长度不符合要求的记录
        cleaned_data = cleaned_data[~short_id_mask].copy()
        
        final_count = len(cleaned_data)
        removed_count = original_count - final_count
        cleanup_rate = (removed_count / original_count) * 100 if original_count > 0 else 0
        
        context.log.info(f"✅ 已删除 {removed_count} 条长度不超过{min_length}个字符的{process_field}记录")
        context.log.info(f"📊 清理统计: 原始={original_count}条, 清理后={final_count}条, 清理率={cleanup_rate:.2f}%")
        
        # 数据质量分析
        if cleanup_rate > 20:
            context.log.warning(f"⚠️ 短{process_field}清理率很高 ({cleanup_rate:.2f}%)，请检查上游数据质量标准")
        elif cleanup_rate > 10:
            context.log.warning(f"⚠️ 短{process_field}清理率较高 ({cleanup_rate:.2f}%)，建议关注数据质量")
        else:
            context.log.info(f"ℹ️ 短{process_field}清理率正常 ({cleanup_rate:.2f}%)")
        
        # 显示清理后的长度统计
        if final_count > 0:
            remaining_lengths = cleaned_data[process_field].str.len()
            min_remaining = remaining_lengths.min()
            max_remaining = remaining_lengths.max()
            avg_remaining = remaining_lengths.mean()
            context.log.info(f"📏 清理后{process_field}长度统计: 最短={min_remaining}, 最长={max_remaining}, 平均={avg_remaining:.1f}")
        
    else:
        context.log.info(f"✅ 所有{process_field}长度都符合要求（>{min_length}个字符），无需清理")
        
        # 显示当前的长度统计
        if original_count > 0:
            min_length_current = id_lengths.min()
            max_length_current = id_lengths.max()
            avg_length_current = id_lengths.mean()
            context.log.info(f"📏 当前{process_field}长度统计: 最短={min_length_current}, 最长={max_length_current}, 平均={avg_length_current:.1f}")
    
    return cleaned_data