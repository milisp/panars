def validate_column(df, columns):
    missing = set(columns) - set(df.columns)
    if missing:
        raise KeyError(f"Columns not found: {missing}")
