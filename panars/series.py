import polars as pl


class Series:
    def __init__(self, data):
        if isinstance(data, pl.Series):
            self._series = data
        else:
            self._series = pl.Series(data)

    def isin(self, values):
        return self._series.is_in(values)

    def isna(self):
        return self._series.is_null()

    def is_not_null(self):
        return self._series.is_not_null()

    def sum(self):
        return self._series.sum()

    def mean(self):
        return self._series.mean()

    def head(self, n: int = 5) -> "Series":
        return Series(self._series.head(n))

    def tail(self, n: int = 5) -> "Series":
        return Series(self._series.tail(n))

    # 按索引获取值
    def iloc(self, idx):
        return self._series[idx]

    # 按条件筛选
    def loc(self, condition):
        return Series(self._series.filter(condition))

    # 将 Series 转换为列表
    def to_list(self):
        return self._series.to_list()

    # 打印 Series 信息
    def __repr__(self):
        return self._series.__repr__()

    # 重载比较运算符以支持 df.loc(df['A'] > 2) 的条件
    def __eq__(self, other):
        return self._series == other

    def __ne__(self, other):
        return self._series != other

    def __gt__(self, other):
        return self._series > other

    def __lt__(self, other):
        return self._series < other

    def __ge__(self, other):
        return self._series >= other

    def __le__(self, other):
        return self._series <= other

    def apply(self, func):
        """使用 Python 的列表推导式模拟 apply"""
        return pl.Series([func(x) for x in self._series])

    def add(self, other) -> "Series":
        # Simple element-wise addition
        if isinstance(other, Series):
            return Series(self._series + other._series)
        else:
            return Series(self._series + other)

    def __add__(self, other):
        return self.add(other)

    def mul(self, other) -> "Series":
        # Simple element-wise addition
        if isinstance(other, Series):
            return Series(self._series * other._series)
        else:
            return Series(self._series * other)

    def __mul__(self, other):
        return self.mul(other)

    def sub(self, other) -> "Series":
        # Simple element-wise addition
        if isinstance(other, Series):
            return Series(self._series - other._series)
        else:
            return Series(self._series - other)

    def __sub__(self, other):
        return self.sub(other)

    def truediv(self, other) -> "Series":
        # Simple element-wise addition
        if isinstance(other, Series):
            return Series(self._series / other._series)
        else:
            return Series(self._series / other)

    def __truediv__(self, other):
        return self.truediv(other)

    def to_pandas(self):
        return self._series.to_pandas()
