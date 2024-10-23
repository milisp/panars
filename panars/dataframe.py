from typing import Any, List, Union

import polars as pl

from .series import Series
from .utils import validate_column


class DataFrame:
    def __init__(self, data: Union[pl.DataFrame, dict, list, "DataFrame"] = None):
        if isinstance(data, DataFrame):
            self.df = data.df.clone()
        elif isinstance(data, pl.DataFrame):
            self.df = data
        elif isinstance(data, dict) or isinstance(data, list):
            self.df = pl.DataFrame(data)
        elif data is None:
            self.df = pl.DataFrame()
        else:
            raise TypeError("Unsupported data type for DataFrame initialization.")

    def head(self, n=5):
        return DataFrame(self.df.head(n))

    def tail(self, n=5):
        return DataFrame(self.df.tail(n))

    def sum(self, axis=None):
        if axis == 1:
            # 按行求和
            return DataFrame(
                self.df.select(
                    [pl.sum_horizontal(pl.col(c)) for c in self.df.columns]
                )
            )
        # 默认按列求和
        return DataFrame(self.df.sum())

    def mean(self):
        return DataFrame(self.df.mean())

    # 删除列
    def drop(self, columns, axis=1):
        if axis == 1:
            return DataFrame(self.df.drop(columns))
        raise ValueError("Polars only supports dropping columns (axis=1)")

    # 合并两个DataFrame（按行或列）
    @staticmethod
    def concat(dataframes: list, axis: int = 0, **kwargs) -> "DataFrame":
        if axis == 0:
            concatenated_data = pl.concat([df.df for df in dataframes], **kwargs)
        elif axis == 1:
            concatenated_data = pl.concat(
                [df.df for df in dataframes], how="horizontal", **kwargs
            )
        else:
            raise ValueError("Invalid axis: choose 0 for rows or 1 for columns.")

        return DataFrame(concatenated_data)

    @staticmethod
    def merge(
        left: "DataFrame", right: "DataFrame", on=None, how="inner", **kwargs
    ) -> "DataFrame":
        merged_data = left.df.join(right.df, on=on, how=how, **kwargs)
        return DataFrame(merged_data)

    # 按位置选择行 (模拟 Pandas 的 iloc)
    def iloc(self, idx):
        return DataFrame(self.df[idx : idx + 1])
    
    def groupby(self, by: Union[str, List[str]]) -> "GroupBy":
        if not isinstance(by, list):
            by = [by]
        return GroupBy(self.df, by)

    def map(self, func, axis=0):
        """
        Apply a function to each row (axis=0) or column (axis=1) of the DataFrame.
        Usage:

        wrapped_df = pa.DataFrame({"A": [2,5], "B": [4,7]})
        mapped_df_by_row = wrapped_df.map(lambda row: [x + 1 for x in row], axis=0)
        mapped_df_by_col = wrapped_df.map(lambda x: x * 2, axis=1)
        """
        if axis == 0:  # Apply function to each row
            mapped_rows = [func(row) for row in self.df.rows()]
            return pl.DataFrame(mapped_rows, schema=self.df.schema)
        elif axis == 1:  # Apply function to each column
            mapped_columns = {
                col: pl.Series(col, [func(x) for x in self.df[col].to_list()])
                for col in self.df.columns
            }
            return pl.DataFrame(mapped_columns)
        else:
            raise ValueError("Axis must be 0 (rows) or 1 (columns)")

    def isin(self, column, values):
        return self.df[column].is_in(values)

    def info(self):
        print(self.df)

    def describe(self) -> "DataFrame":
        return DataFrame(self.df.describe())

    def show(self):
        print(self.df)

    def loc(self, condition: Any) -> "DataFrame":
        filtered_df = self.df.filter(condition)
        return DataFrame(filtered_df)

    def isnull(self) -> "DataFrame":
        return DataFrame(self.df.is_null())

    def dropna(
        self, subset: Union[str, List[str]] = None, how: str = "any"
    ) -> "DataFrame":
        return DataFrame(self.df.drop_nulls(subset=subset))  # , how=how))

    def fillna(self, value: Any, columns: Union[str, List[str]] = None) -> "DataFrame":
        if columns:
            if isinstance(columns, str):
                columns = [columns]
            validate_column(self.df, columns)
            for col in columns:
                self.df = self.df.with_columns(pl.col(col).fill_null(value))
        else:
            self.df = self.df.fill_null(value)
        return self

    def add(self, other, on: Union[str, List[str]] = None) -> "DataFrame":
        # Simple element-wise addition
        if isinstance(other, DataFrame):
            return DataFrame(self.df + other.df)
        else:
            return DataFrame(self.df + other)

    def multiply(self, other, on: Union[str, List[str]] = None) -> "DataFrame":
        # Simple element-wise multiplication
        if isinstance(other, DataFrame):
            return DataFrame(self.df * other.df)
        else:
            return DataFrame(self.df * other)

    def __add__(self, other):
        return self.add(other)

    def __sub__(self, other):
        if isinstance(other, DataFrame):
            return DataFrame(self.df - other.df)
        elif isinstance(other, (int, float)):
            return DataFrame(self.df - other)
        else:
            return NotImplemented

    def __mul__(self, other):
        return self.multiply(other)

    def pivot(
        self, index: str, columns: str, values: str, aggregate_function: str = "first"
    ) -> "DataFrame":
        pivoted = self.df.pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate_function=aggregate_function
        )
        return DataFrame(pivoted)

    def melt(
        self, id_vars: Union[str, List[str]], value_vars: Union[str, List[str]],
        variable_name: str = "variable", value_name: str = "value"
    ) -> "DataFrame":
        melted = self.df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name=variable_name,
            value_name=value_name
        )
        return DataFrame(melted)

    def to_datetime(self, column: str, fmt: str = None) -> "DataFrame":
        if fmt:
            self.df = self.df.with_columns(
                pl.col(column).str.strptime(pl.Datetime, fmt)
            )
        else:
            self.df = self.df.with_columns(
                pl.col(column).cast(pl.Datetime)
            )
        return self

    def set_index(self, column: str) -> "DataFrame":
        self.df = self.df.set_sorted(column)
        return self

    def to_categorical(self, columns: Union[str, List[str]]) -> "DataFrame":
        if isinstance(columns, str):
            columns = [columns]
        validate_column(self.df, columns)
        for col in columns:
            self.df = self.df.with_columns(pl.col(col).cast(pl.Categorical))
        return self

    # 其他常用方法
    def select(self, expressions):
        return DataFrame(self.df.select(expressions))

    def filter(self, expression):
        return DataFrame(self.df.filter(expression))

    def __gt__(self, other):
        return self.df > other

    def __lt__(self, other):
        return self.df < other

    def __eq__(self, other):
        return self.df == other

    def __ne__(self, other):
        return self.df != other

    def query(self, expression):
        return DataFrame(self.df.filter(expression))

    def __and__(self, other):
        return self.df & other.df

    def __or__(self, other):
        return self.df | other.df

    def __invert__(self):
        return ~self.df

    def collect(self):
        if isinstance(self.df, pl.LazyFrame):
            return self.df.collect()
        return self

    def to_pandas(self):
        return self.df.to_pandas()

    def to_csv(self, filepath: str, **kwargs):
        self.df.write_csv(filepath, **kwargs)

    def to_parquet(self, filepath: str, **kwargs):
        self.df.write_parquet(filepath, **kwargs)

    def to_excel(self, filepath: str, **kwargs):
        self.df.write_excel(filepath, **kwargs)

    def __getitem__(self, key):
        if isinstance(key, str):
            return Series(self.df[key])
        elif isinstance(key, pl.Expr):
            return self.df.select(key)
        elif isinstance(key, slice):
            return DataFrame(self.df.slice(key.start, key.stop))
        elif isinstance(key, (list, dict, pl.Series)):
            # 处理布尔索引
            if isinstance(key, list):
                key = pl.Series(key)
            if isinstance(key, pl.Series) and key.dtype == pl.Boolean:
                return DataFrame(self.df.filter(key))
            else:
                raise NotImplementedError("目前只支持布尔 Series 作为索引")
        else:
            raise KeyError(f"Unsupported key type: {type(key)}")

    def __setitem__(self, key, value):
        if isinstance(value, Series):
            self.df = self.df.with_columns(value._series.alias(key))
        else:
            self.df = self.df.with_columns(pl.Series(key, value))

    def __len__(self):
        return len(self.df)

    def __getattr__(self, name):
        if name in self.df.columns:
            return Series(self.df[name])
        # 其他 DataFrame 方法的简单传递
        return getattr(self.df, name)

    def __repr__(self):
        return self.df.__repr__()


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

