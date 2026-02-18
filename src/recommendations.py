from pyspark.sql.functions import col, count, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import collect_list

def top_advertisers(impressions_df, clicks_df):

    joined = impressions_df.join(
        clicks_df,
        impressions_df.id == clicks_df.impression_id,
        "left"
    )

    advertiser_perf = (
        joined.groupBy("app_id", "country_code", "advertiser_id")
        .agg(
            count(impressions_df.id).alias("impressions"),
            sum("revenue").alias("revenue")
        )
        .fillna({"revenue": 0.0})
    )

    advertiser_perf = advertiser_perf.filter(col("impressions") >= 5)

    advertiser_perf = advertiser_perf.withColumn(
        "revenue_per_impression",
        col("revenue") / col("impressions")
    )

    window_spec = Window.partitionBy("app_id", "country_code") \
                        .orderBy(col("revenue_per_impression").desc())

    ranked = advertiser_perf.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 5)

    result = (
        ranked.groupBy("app_id", "country_code")
        .agg(
            collect_list("advertiser_id").alias("recommended_advertiser_ids")
        )
    )
    return result