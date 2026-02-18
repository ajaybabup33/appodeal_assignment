from pyspark.sql.functions import col, sum , expr

def calculate_median_user_spend(impressions_df, clicks_df):

    joined = impressions_df.join(
        clicks_df,
        impressions_df.id == clicks_df.impression_id,
        "inner"
    )

    user_spend = (
        joined.groupBy("country_code", "user_id")
        .agg(sum("revenue").alias("total_spend"))
    )

    median_df = (
        user_spend.groupBy("country_code")
        .agg(expr("percentile_approx(total_spend, 0.5)").alias("median_spend"))
    )
    return median_df