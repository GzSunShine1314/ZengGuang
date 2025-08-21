import pandas as pd
import argparse
import os
import pathlib
from typing import Optional

def validate_file_path(file_path: str, file_extension: str) -> Optional[str]:
    """
    验证文件路径是否有效
    
    参数:
        file_path (str): 待验证的文件路径
        file_extension (str): 期望的文件扩展名（不带点）
    
    返回:
        绝对路径字符串，如果有效；否则返回None
    """
    # 解析路径
    path = pathlib.Path(file_path).resolve()
    
    # 检查路径是否存在
    if not path.exists():
        print(f"错误: 路径不存在 - {path}")
        return None
    
    # 检查是否是文件
    if not path.is_file():
        print(f"错误: 不是有效的文件 - {path}")
        return None
    
    # 检查文件扩展名
    if path.suffix.lower() != f'.{file_extension}':
        print(f"错误: 文件不是{file_extension}格式 - {path}")
        return None
    
    return str(path)

def get_default_output_path(input_path: str) -> str:
    """生成默认的输出文件路径（与输入同目录，同名称，不同扩展名）"""
    input_path = pathlib.Path(input_path)
    return str(input_path.with_suffix('.csv'))

def validate_output_path(output_path: str, input_path: str) -> str:
    """验证或生成有效的输出路径"""
    if not output_path:
        return get_default_output_path(input_path)
    
    output_path = pathlib.Path(output_path).resolve()
    
    # 如果输出路径是目录，则在该目录下生成默认文件名
    if output_path.is_dir():
        input_filename = pathlib.Path(input_path).stem
        return str(output_path / f"{input_filename}.csv")
    
    # 如果输出文件已存在，给出提示
    if output_path.exists():
        print(f"警告: 输出文件已存在，将被覆盖 - {output_path}")
    
    # 确保输出目录存在
    output_dir = output_path.parent
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"提示: 创建输出目录 - {output_dir}")
    
    return str(output_path)

def parquet_to_csv(parquet_file_path: str, csv_file_path: Optional[str] = None,** kwargs) -> bool:
    """
    将snappy压缩的parquet文件转换为csv文件
    
    参数:
        parquet_file_path (str): parquet文件路径
        csv_file_path (str, 可选): 输出csv文件路径
        **kwargs: 传递给to_csv的额外参数，如sep, index等
    
    返回:
        转换成功返回True，否则返回False
    """
    # 验证输入文件路径
    validated_input = validate_file_path(parquet_file_path, 'parquet')
    if not validated_input:
        return False
    
    # 确定输出文件路径
    validated_output = validate_output_path(csv_file_path, validated_input)
    
    try:
        # 读取parquet文件，自动处理snappy压缩
        print(f"正在读取文件: {validated_input}")
        df = pd.read_parquet(validated_input)
        
        # 将数据写入csv文件
        print(f"正在写入文件: {validated_output}")
        df.to_csv(validated_output, index=False,** kwargs)
        
        print(f"转换成功！文件已保存至: {validated_output}")
        return True
        
    except Exception as e:
        print(f"转换失败: {str(e)}")
        return False

if __name__ == "__main__":
    # 设置命令行参数解析
    parser = argparse.ArgumentParser(description='将snappy.parquet文件转换为csv文件',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('input', help='输入的parquet文件路径（支持相对路径和绝对路径）')
    parser.add_argument('-o', '--output', help='输出的csv文件路径或目录')
    parser.add_argument('-s', '--sep', default=',', help='csv文件的分隔符')
    parser.add_argument('-e', '--encoding', default='utf-8', help='csv文件的编码格式')
    
    args = parser.parse_args()
    
    # 调用转换函数
    parquet_to_csv(
        args.input, 
        args.output, 
        sep=args.sep,
        encoding=args.encoding
    )
