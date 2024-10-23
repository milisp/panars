import panars as pa

series = pa.Series([1, 3, 8])


def test_apply():
    series.apply(lambda x: x * 2)


def test_sum():
    series.sum()


def test_mean():
    series.mean()


def test_isna():
    series.isna()


def test_isin():
    series.isin([3, 8])


def test_head():
    series.head()


def test_tail():
    series.tail()


def test_to_list():
    series.to_list()


def test_iloc():
    series.iloc(1)


def test_loc():
    series.loc(series > 3)


def test_gt():
    pa.Series(series>2)


def test_ge():
    pa.Series(series>=2)


def test_le():
    pa.Series(series<=2)


def test_add():
    series + series
    series + 2


def test_mul():
    series * series
    series * 2


def test_sub():
    series - series
    series - 2


def test_div():
    series / series
    series / 2


def test_show():
    print(series)


def test_to_pandas():
    series.to_pandas()
