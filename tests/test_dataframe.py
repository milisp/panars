import polars as pl
import pytest

import panars as pa

data = {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8], "C": [1, 1, 2, 2]}

# 创建DataFrame
df = pa.DataFrame(data)
df2 = pa.DataFrame({"A": [5, 6], "B": [9, 10], "C": [12, 13]})
df3 = pa.DataFrame(
    {"A": [5, 6], "B": [9, 10], "C": ["foo", "foo"], "city": ["London", "London"]}
)


def test_concat():
    # 创建两个简单的 DataFrame
    df1 = pa.DataFrame(pl.DataFrame({"A": [1, 2], "B": [3, 4]}))
    df2 = pa.DataFrame(pl.DataFrame({"A": [5, 6], "B": [7, 8]}))

    # 使用 pa.concat 按行合并
    combined = pa.concat([df1, df2], axis=0)

    # 断言合并后的结果
    expected_data = pl.DataFrame({"A": [1, 2, 5, 6], "B": [3, 4, 7, 8]})
    assert combined.data.equals(expected_data)


def test_concat1():
    df4 = pa.DataFrame({"D": ["foo", "bar"]})
    pa.concat([df4, df2], axis=1)


def test_merge():
    # 创建两个简单的 DataFrame
    df1 = pa.DataFrame(pl.DataFrame({"key": [1, 2], "A": [3, 4]}))
    df2 = pa.DataFrame(pl.DataFrame({"key": [1, 2], "B": [5, 6]}))

    # 使用 pa.merge 按照 "key" 列合并
    merged = pa.merge(df1, df2, on="key")

    # 断言合并后的结果
    expected_data = pl.DataFrame({"key": [1, 2], "A": [3, 4], "B": [5, 6]})
    assert merged.data.equals(expected_data)


def test_head():
    df.head()


def test_tail():
    print(df.tail())


def test_mean():
    print(df.mean())


def test_sum():
    df.sum()
    df.sum(axis=1)


def test_groupby():
    df3.groupby(["C", "city"]).agg({"A": "mean", "B": ["min", "count", "max", "sum"]})
    df.groupby("C").sum()

    df_grouped = df.groupby("C")

    df_grouped.mean()
    df_grouped.max()
    df_grouped.min()
    df_grouped.count()


def test_filter():
    # 通过loc和iloc选择数据
    print(df.filter(df["A"] > 2))
    print(df.filter(df["A"] < 2))
    print(df.filter(df["A"] == 2))
    print(df.iloc(1))


def test_filter2():
    print(df[df["A"] > 2])


def test_drop():
    df.drop(["A"])


def test_drop_axis0():
    with pytest.raises(
        ValueError, match="Polars only supports dropping columns \(axis=1\)"
    ):
        df.drop("A", axis=0)


def add(x):
    return x + 1


def test_map():
    df = pl.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})

    wrapped_df = pa.DataFrame(df)

    # 对每一行应用函数 (axis=0)
    mapped_df_by_row = wrapped_df.map(lambda row: [x + 1 for x in row], axis=0)
    print("Apply to rows:")
    print(mapped_df_by_row)

    # 对每一列应用函数 (axis=1)
    mapped_df_by_col = wrapped_df.map(lambda x: x * 2, axis=1)
    print("Apply to columns:")
    print(mapped_df_by_col)


def test_isin():
    print(df.filter(df["A"].isin([8, 9])))


def test_isin1():
    print(df.isin("A", [3, 9]))


def test_isin2():
    print(df[df["A"].isin([3, 9])])


def test_isna():
    print(df.filter(df["A"].isna()))


def test_is_not_null():
    print(df.filter(df["A"].is_not_null()))


def test_ne():
    df[df["A"] != 3]


def test_add_series():
    df["A"] + df["B"]


def test_to_pandas():
    df.to_pandas()


def test_len():
    len(df)


def test_show():
    df.show()


def test_to_csv():
    df.to_csv("/tmp/df.csv")
