import panars as pa
from panars import DataFrame

data = {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8], "C": [1, 1, 2, 2], 'd': [4,3,28,17]}

# 创建DataFrame
df = pa.DataFrame(data)
print('cols',df.columns)
print('is_in',df.filter(df['A'].isin([3,4])))
print('isin', df[df.d.isin([3,4])])
print('gt 5', df.filter(df.d>5))



def test_add_series():
    print(df["A"] + df["B"])


# 示例用法
if __name__ == "__main__":
    test_add_series()
    # 创建示例数据
    data = {
        'a': [1, 2, 3, 4],
        'b': [5, 6, 7, 8],
        'c': ['foo', 'bar', 'foo', 'baz']
    }
    
    df = DataFrame(data)
    print("原始 DataFrame:")
    df.show()
    
    # 1. df['a'] + df['b']
    df['a_plus_b'] = df['a'] + df['b']
    print("\n添加 a + b 列后:")
    df.show()
    df['a'] - df['b']
    df['a'] * df['b']
    df['a'] / df['b']
    
    # 2. df[df['a'] > 1]
    filtered_df = df[df['a'] > 1]
    print("\n筛选 a > 1 后的 DataFrame:")
    filtered_df.show()
    
    # 3. df[~(df['a'] == 2)]
    filtered_df_neg = df[~(df['a'] == 2)]
    print("\n筛选 a != 2 后的 DataFrame:")
    filtered_df_neg.show()
    
    # 4. df[df['a'].isin(['foo'])]  # 这里假设 'a' 是字符串类型
    # 修改数据使 'a' 为字符串以示例 isin
    df_str = DataFrame({
        'a': ['foo', 'bar', 'foo', 'baz'],
        'b': [5, 6, 7, 8],
        'c': ['foo', 'bar', 'foo', 'baz']
    })
    isin_df = df_str[df_str.isin('a', ['foo'])]
    print("\n筛选 a 在 ['foo'] 中的 DataFrame:")
    isin_df.show()
