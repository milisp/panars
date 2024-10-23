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
    df1 = pa.DataFrame({"A": [1, 2], "B": [3, 4]})
    df2 = pa.DataFrame({"A": [5, 6], "B": [7, 8]})

    # 使用 pa.concat 按行合并
    combined = pa.concat([df1, df2], axis=0)

    # 断言合并后的结果
    expected_data = pa.DataFrame({"A": [1, 2, 5, 6], "B": [3, 4, 7, 8]})
    assert combined.df.equals(expected_data)


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
    assert merged.df.equals(expected_data)


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

    # 每一行应用函数 (axis=0)
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



def test_dataframe_creation():
    data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
    df = pa.DataFrame(data)
    assert len(df) == 3
    assert df["A"].to_list() == [1, 2, 3]


def test_arithmetic_operations():
    data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
    df = pa.DataFrame(data)
    df2 = df + 1
    assert df2["A"].to_list() == [2, 3, 4]
    df3 = df * 2
    assert df3["B"].to_list() == [8, 10, 12]


def test_comparison_operations():
    data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
    df = pa.DataFrame(data)
    filtered_df = df[df["A"] > 1]
    assert len(filtered_df) == 2


def test_groupby_and_aggregation():
    data = {"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]}
    df = pa.DataFrame(data)
    grouped = df.groupby("A").agg({"B": "sum", "C": ["min", "max"]})
    assert len(grouped) == 3
    assert "B_sum" in grouped.df.columns
    assert "C_min" in grouped.df.columns
    assert "C_max" in grouped.df.columns


def test_to_datetime():
    date_data = {"date": ["2021-01-01", "2021-01-02", "2021-01-03"]}
    date_df = pa.DataFrame(date_data)
    date_df = date_df.to_datetime("date", fmt="%Y-%m-%d")
    assert pl.Datetime in date_df.df.dtypes


def test_to_categorical():
    cat_data = {"category": ["A", "B", "A", "C", "B"]}
    cat_df = pa.DataFrame(cat_data)
    cat_df = cat_df.to_categorical("category")
    assert pl.Categorical in cat_df.df.dtypes


def test_pivot():
    pivot_data = {"id": [1, 1, 2], "var": ["A", "B", "A"], "value": [10, 20, 30]}
    pivot_df = pa.DataFrame(pivot_data)
    pivoted = pivot_df.pivot(index="id", columns="var", values="value")
    assert "A" in pivoted.df.columns and "B" in pivoted.df.columns


def test_melt():
    melt_data = {"id": [1, 2], "A": [10, 30], "B": [20, 40]}
    melt_df = pa.DataFrame(melt_data)
    melted = melt_df.melt(id_vars=["id"], value_vars=["A", "B"])
    assert "variable" in melted.df.columns and "value" in melted.df.columns

def test_sort_values():
    data = {"A": [3, 1, 2], "B": [6, 4, 5]}
    df = pa.DataFrame(data)
    
    # Test sorting by a single column in ascending order
    sorted_df = df.sort_values("A")
    assert sorted_df["A"].to_list() == [1, 2, 3]
    assert sorted_df["B"].to_list() == [4, 5, 6]
    
    # Test sorting by a single column in descending order
    sorted_df_desc = df.sort_values("A", ascending=False)
    assert sorted_df_desc["A"].to_list() == [3, 2, 1]
    assert sorted_df_desc["B"].to_list() == [6, 5, 4]
    
    # Test sorting by multiple columns
    data_multi = {"A": [1, 1, 2], "B": [3, 2, 1], "C": [6, 5, 4]}
    df_multi = pa.DataFrame(data_multi)
    sorted_df_multi = df_multi.sort_values(["A", "B"])
    assert sorted_df_multi["A"].to_list() == [1, 1, 2]
    assert sorted_df_multi["B"].to_list() == [2, 3, 1]
    assert sorted_df_multi["C"].to_list() == [5, 6, 4]

    # Test sorting after groupby
    data_grouped = {"A": [1, 1, 2, 2], "B": [4, 3, 2, 1], "C": [10, 20, 30, 40]}
    df_grouped = pa.DataFrame(data_grouped)
    
    grouped_sorted = df_grouped.groupby("A").agg({"B": "sum", "C": "mean"}).sort_values("B_sum", ascending=False)
    
    assert grouped_sorted["A"].to_list() == [1, 2]
    assert grouped_sorted["B_sum"].to_list() == [7, 3]
    assert grouped_sorted["C_mean"].to_list() == [15.0, 35.0]
