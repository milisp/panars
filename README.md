# panars: Polars with Pandas-like Interface

Panars is a powerful wrapper that brings the familiar Pandas API to Polars, combining the best of both worlds: Polars' speed and efficiency with Pandas' user-friendly interface.

## Key Features

- **Pandas-like API**: Use Polars with syntax you already know from Pandas.
- **High Performance**: Leverage Polars' speed while writing Pandas-style code.
- **Easy Migration**: Seamlessly transition existing Pandas code to Polars.
- **Best of Both Worlds**: Combine Pandas' ease of use with Polars' efficiency.

## Installation

```bash
pip install panars
```

## Quick Start

```python
import panars as pa

# Create a DataFrame
df = pa.DataFrame({
    "A": [1, 2, 3, 4],
    "B": [5, 6, 7, 8],
    "C": [1, 1, 2, 2]
})

# Familiar Pandas operations
print(df.head())
print(df.groupby("C").sum())
print(df.filter(df["A"] > 2))

# Efficient data manipulation
result = (df.groupby(["C"])
            .agg({"A": ["mean", "sum"], "B": ["min", "max"]})
            .sort_values("C"))
print(result)
```

## Why panars?

1. **Familiar Syntax**: Write Polars code using Pandas conventions you already know.
2. **Performance Boost**: Gain Polars' speed advantages without learning a new API.
3. **Gradual Migration**: Easily port existing Pandas projects to Polars over time.
4. **Community-Driven**: Open-source project welcoming contributions and feedback.

## Documentation

For detailed usage instructions and API reference, visit our [documentation](https://github.com/milisp/panars/wiki).

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for more details.

## License

Panars is released under the MIT License. See the [LICENSE](LICENSE) file for details.
