import polars as pl

def is_numeric(expr: pl.Expr) -> pl.Expr:
    # Robust numeric detection
    return expr.cast(pl.Float64, strict=False).is_not_null()

# Read parquet
df = pl.read_parquet("sanru-iaso-to-dhis2/workspace/pipelines/sanru-iaso-to-dhis2/transformed/ssc_supervision_v3_file_202601.parquet")

# Filter out numeric values (keep only alpha / non-numeric)
df_alpha = df.filter(~is_numeric(pl.col("VALUE")))

# Group by DX_UID and ORG_UNIT
result = (
    df_alpha
    .group_by(["DX_UID"])
    .agg([
        pl.count().alias("count"),                     # count of rows
        pl.first("VALUE").alias("sample_alpha_value")  # pick one non-numeric value
    ])
)

# Export to CSV
result.write_csv("output.csv")