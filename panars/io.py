import polars as pl


def read_excel(file_path: str, **kwargs) -> pl.DataFrame:
    """
    从 Excel 文件读取数据并返回 Polars DataFrame.

    :param file_path: Excel 文件路径
    :param kwargs: 传递给 Polars 的额外参数
    :return: Polars DataFrame
    """
    return pl.read_excel(file_path, **kwargs)


def read_csv(file_path: str, **kwargs) -> pl.DataFrame:
    return pl.read_csv(file_path, **kwargs)


def scan_csv(file_path: str, **kwargs) -> pl.DataFrame:
    return pl.scan_csv(file_path, **kwargs)
