# panars
Polars to Pandas Wrapper, 让 polars 像 pandas 一样的接口

```python
import polars as pl

import panars as pa

data = {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8], "C": [1, 1, 2, 2]}

# 创建DataFrame
df = pa.DataFrame(data)
df2 = pa.DataFrame({"A": [5, 6], "B": [9, 10], "C": [12, 13]})


def test_concat():
    # 创建两个简单的 DataFrame
    df1 = pa.DataFrame(pl.DataFrame({"A": [1, 2], "B": [3, 4]}))
    df2 = pa.DataFrame(pl.DataFrame({"A": [5, 6], "B": [7, 8]}))

    # 使用 pa.concat 按行合并
    combined = pa.concat([df1, df2], axis=0)

    # 断言合并后的结果
    expected_data = pl.DataFrame({"A": [1, 2, 5, 6], "B": [3, 4, 7, 8]})
    assert combined.data.equals(expected_data)


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


def test_groupby():
    grouped = df.groupby("C")
    print(grouped.sum())


def test_filter():
    # 通过loc和iloc选择数据
    print(df.filter(df["A"] > 2))
    print(df.filter(df["A"] < 2))
    print(df.filter(df["A"] == 2))
    print(df.iloc(1))


def test_drop():
    print(df.drop(["A"]))


def add(x):
    return x + 1


def test_map():
    df = pl.DataFrame({
        "a": [1, 2, 3, 4],
        "b": [5, 6, 7, 8]
    })

    wrapped_df = pa.DataFrame(df)

    # 对每一行应用函数 (axis=0)
    mapped_df_by_row = wrapped_df.map(lambda row: [x + 1 for x in row], axis=0)
    print("Apply to rows:")
    print(mapped_df_by_row)

    # 对每一列应用函数 (axis=1)
    mapped_df_by_col = wrapped_df.map(lambda x: x * 2, axis=1)
    print("Apply to columns:")
    print(mapped_df_by_col)
```
