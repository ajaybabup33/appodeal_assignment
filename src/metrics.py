from pyspark.sql.functions import col, count, sum

def calculate_metrics(impressions_df, clicks_df):
    joined = impressions_df.join(
        clicks_df,
        impressions_df.id == clicks_df.impression_id,
        "left"
    )

    result = (
        joined.groupBy("app_id", "country_code")
        .agg(
            count(impressions_df.id).alias("impressions"),
            count(clicks_df.id).alias("clicks"),
            sum("revenue").alias("revenue")
        )
        .fillna({"revenue": 0.0})
    )
    return result