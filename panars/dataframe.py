from typing import List, Union

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
        if isinstance(condition, pl.Expr):
            # 使用 Polars 的 filter 方法进行过滤
            return DataFrame(self.data.filter(condition))
        else:
            raise ValueError("Condition must be a Polars expression")

    # 设置列
    def __setitem__(self, key, value):
        if isinstance(value, Series):
            self.data = self.data.with_columns(value._series.alias(key))
        else:
            self.data = self.data.with_columns(pl.Series(key, value))

    def __getitem__(self, key):
        if isinstance(key, str):
            return Series(self.data[key])
        elif isinstance(key, pl.Expr):
            return self.data.select(key)
        elif isinstance(key, (list, dict, pl.Series)):
            # 处理布尔索引
            if isinstance(key, list):
                key = pl.Series(key)
            if isinstance(key, pl.Series) and key.dtype == pl.Boolean:
                return DataFrame(self.data.filter(key))
            else:
                raise NotImplementedError("目前只支持布尔 Series 作为索引")
        else:
            raise KeyError(f"Unsupported key type: {type(key)}")

    def groupby(self, by: Union[str, List[str]]) -> "GroupBy":
        if not isinstance(by, list):
            by = [by]
        return GroupBy(self.data, by)

    def __getattr__(self, name):
        # 其他 DataFrame 方法的简单传递
        if name in self.data.columns:
            return Series(self.data[name])
        return getattr(self.data, name)

    def __repr__(self):
        return self.data.__repr__()

    def __len__(self):
        return len(self.data)

    def filter(self, mask):
        return DataFrame(self.data.filter(mask))

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

    def isin(self, column, values):
        return self.data[column].is_in(values)

    def show(self):
        print(self.data)

    def to_pandas(self):
        return self.data.to_pandas()

    def to_csv(self, filepath: str, **kwargs):
        self.data.write_csv(filepath, **kwargs)

    def to_parquet(self, filepath: str, **kwargs):
        self.data.write_parquet(filepath, **kwargs)

    def to_excel(self, filepath: str, **kwargs):
        self.data.write_excel(filepath, **kwargs)


class GroupBy:
    def __init__(self, df, by):
        self._df = df
        self.by = by

    def sum(self):
        return DataFrame(self._df.group_by(self.by).agg(pl.col("*").sum()))

    def mean(self):
        return DataFrame(self._df.group_by(self.by).agg(pl.col("*").mean()))

    def max(self):
        return DataFrame(self._df.group_by(self.by).agg(pl.col("*").max()))

    def min(self):
        return DataFrame(self._df.group_by(self.by).agg(pl.col("*").min()))

    def count(self):
        return DataFrame(self._df.group_by(self.by).agg(pl.col("*").count()))

    def agg(self, agg_spec):
        aggs = []
        if isinstance(agg_spec, dict):
            for col, funcs in agg_spec.items():
                if not isinstance(funcs, list):
                    funcs = [funcs]
                for func in funcs:
                    aggs.append(self._get_agg_expr(col, func))
        elif isinstance(agg_spec, str):
            # 对所有非分组列应用同一聚合函数
            funcs = [agg_spec]
            data_cols = [col for col in self._df.columns if col not in self.by]
            for col in data_cols:
                for func in funcs:
                    aggs.append(self._get_agg_expr(col, func))
        else:
            raise ValueError("agg_spec must be either a dict or a str")

        result = self._df.group_by(self.by).agg(aggs)
        return DataFrame(result)

    def _get_agg_expr(self, col, func):
        func = func.lower()
        if func == "sum":
            return pl.col(col).sum().alias(f"{col}_{func}")
        elif func == "mean":
            return pl.col(col).mean().alias(f"{col}_{func}")
        elif func == "count":
            return pl.col(col).count().alias(f"{col}_{func}")
        elif func == "min":
            return pl.col(col).min().alias(f"{col}_{func}")
        elif func == "max":
            return pl.col(col).max().alias(f"{col}_{func}")
        else:
            raise NotImplementedError(
                f"Aggregation function '{func}' is not implemented"
            )
