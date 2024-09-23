import panars as pa


def test_apply():
    series = pa.Series([1,3,8])
    print(series.apply(lambda x: x*2))


def test_sum():
    series = pa.Series([1,3,8])
    print(series.sum())
def test_mean():
    series = pa.Series([1,3,8])
    print(series.mean())
