import panars as pa


def test_apply():
    series = pa.Series([1, 3, 8])
    print(series.apply(lambda x: x * 2))


def test_sum():
    series = pa.Series([1, 3, 8])
    print(series.sum())


def test_mean():
    series = pa.Series([1, 3, 8])
    print(series.mean())


def test_isna():
    series = pa.Series([1, 3, 8])
    print(series.isna())


def test_isin():
    series = pa.Series([1, 3, 8])
    print(series.isin([3, 8]))


def test_head():
    series = pa.Series([1, 3, 8])
    print(series.head())
