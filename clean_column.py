import pandas as pd


# 定义函数
def extract_table_info(table_name):
    # 获取 Excel 文件对象
    excel_file = pd.ExcelFile(r"C:\Users\ZengGuang\Desktop\Mes系统清洗规则记录.xlsx")

    # 查看所有工作表名称
    sheet_names = excel_file.sheet_names

    # 假设获取第一个工作表的数据，你可以根据实际情况修改
    df = excel_file.parse(sheet_names[0])

    # 删除`原表`列中包含缺失值的行
    df = df.dropna(subset=['原表'])

    table_info = df[df['原表'].str.contains(table_name)]

    # 提取原表头字段、清洗后表头字段和清洗后表头字段描述
    original_headers = table_info['原表头字段'].tolist()
    cleaned_headers = table_info['清洗后表头字段'].tolist()
    cleaned_descriptions = table_info['清洗后表头字段描述'].tolist()

    # 生成清洗后表头字段对应的原表头字段字典
    cleaned_to_original = dict(zip(original_headers, cleaned_headers))

    return original_headers, cleaned_headers, cleaned_descriptions, cleaned_to_original


# 预先设置表名
table_name = 'wr_dd_carrier_tool_log数采获取参数数据'

# 调用函数并获取结果
original_headers, cleaned_headers, cleaned_descriptions, cleaned_to_original = extract_table_info(table_name)

# 输出结果
print('原表头字段：\n', original_headers)
print('\n清洗后表头字段：\n', cleaned_headers)
print('\n原字段对应的清洗后表头字段：\n', cleaned_to_original)
print('\n清洗后表头字段描述：\n', cleaned_descriptions)