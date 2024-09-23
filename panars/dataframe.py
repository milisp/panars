import polars as pl

from .series import Series


class DataFrame:
    def __init__(self, data):
        if isinstance(data, pl.DataFrame):
            self.data = data
        else:
            self.data = pl.DataFrame(data)

    # 返回DataFrame的前几行
    def head(self, n=5):
        return DataFrame(self.data.head(n))

    # 返回DataFrame的后几行
    def tail(self, n=5):
        return DataFrame(self.data.tail(n))

    # 按行或列求和
    def sum(self, axis=None):
        if axis == 1:
            # 按行求和
            return DataFrame(
                self.data.select(
                    [pl.sum_horizontal(pl.col(c)) for c in self.data.columns]
                )
            )
        # 默认按列求和
        return DataFrame(self.data.sum())

    # 按行或列求平均值
    def mean(self):
        return DataFrame(self.data.mean())

    # 删除列
    def drop(self, columns, axis=1):
        if axis == 1:
            return DataFrame(self.data.drop(columns))
        raise ValueError("Polars only supports dropping columns (axis=1)")

    """
    # 按键合并两个 DataFrame
    def merge(self, right, on=None, how="inner"):
        return DataFrame(self.data.join(right.data, on=on, how=how))
    """

    # 按列分组
    def groupby(self, by):
        return GroupBy(self.data.group_by(by))

    # 合并两个DataFrame（按行或列）
    @staticmethod
    def concat(dataframes: list, axis: int = 0, **kwargs) -> "DataFrame":
        if axis == 0:
            concatenated_data = pl.concat([df.data for df in dataframes], **kwargs)
        elif axis == 1:
            concatenated_data = pl.concat(
                [df.data for df in dataframes], how="horizontal", **kwargs
            )
        else:
            raise ValueError("Invalid axis: choose 0 for rows or 1 for columns.")

        return DataFrame(concatenated_data)

    # 按位置选择行 (模拟 Pandas 的 iloc)
    def iloc(self, idx):
        return DataFrame(self.data[idx : idx + 1])

    # 实现 loc 方法，支持使用布尔条件
    def loc(self, condition):
        print(condition)
        if isinstance(condition, pl.Expr):
            # 使用 Polars 的 filter 方法进行过滤
            return DataFrame(self.data.filter(condition))
        else:
            raise ValueError("Condition must be a Polars expression")

    # 设置列
    def __setitem__(self, key, value):
        if isinstance(value, Series):
            self.data = self.data.with_column(value._series.alias(key))
        else:
            self.data = self.data.with_column(pl.Series(key, value))

    # 通过列名获取列
    def __getitem__(self, key):
        if isinstance(key, str):
            return Series(self.data[key])
        elif isinstance(key, pl.Expr):
            return self.data.filter(key)
        elif isinstance(key, list):
            return DataFrame(self.data[key])
        else:
            raise KeyError(f"Unsupported key type: {type(key)}")

    def filter(self, mask):
        return DataFrame(self.data.filter(mask))

    # 打印DataFrame信息
    def __repr__(self):
        return self.data.__repr__()

    @staticmethod
    def merge(
        left: "DataFrame", right: "DataFrame", on=None, how="inner", **kwargs
    ) -> "DataFrame":
        merged_data = left.data.join(right.data, on=on, how=how, **kwargs)
        return DataFrame(merged_data)

    def map(self, func, axis=0):
        """
        Apply a function to each row (axis=0) or column (axis=1) of the DataFrame.
        Usage:

        wrapped_df = pa.DataFrame({"A": [2,5], "B": [4,7]})
        mapped_df_by_row = wrapped_df.map(lambda row: [x + 1 for x in row], axis=0)
        mapped_df_by_col = wrapped_df.map(lambda x: x * 2, axis=1)
        """
        if axis == 0:  # Apply function to each row
            mapped_rows = [func(row) for row in self.data.rows()]
            return pl.DataFrame(mapped_rows, schema=self.data.schema)
        elif axis == 1:  # Apply function to each column
            mapped_columns = {
                col: pl.Series(col, [func(x) for x in self.data[col].to_list()])
                for col in self.data.columns
            }
            return pl.DataFrame(mapped_columns)
        else:
            raise ValueError("Axis must be 0 (rows) or 1 (columns)")


class GroupBy:
    def __init__(self, groupby_obj):
        self._groupby = groupby_obj

    # 分组后求和
    def sum(self):
        return DataFrame(self._groupby.agg(pl.col("*").sum()))

    # 分组后求平均值
    def mean(self):
        return DataFrame(self._groupby.agg(pl.col("*").mean()))

    # 分组后应用自定义聚合函数
    def agg(self, agg_funcs):
        # agg_funcs是字典，key是列名，value是聚合函数
        return DataFrame(
            self._groupby.agg([agg_funcs[col](pl.col(col)) for col in agg_funcs])
        )
