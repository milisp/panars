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

    # 求和
    def sum(self):
        return self._series.sum()

    # 求平均值
    def mean(self):
        return self._series.mean()

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
