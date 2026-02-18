import sys
from utils import create_spark_session, read_json_files
from metrics import calculate_metrics
from recommendations import top_advertisers
from median import calculate_median_user_spend


def main(impressions_paths, clicks_paths):

    spark = create_spark_session()

    impressions_df = read_json_files(spark, impressions_paths)
    clicks_df = read_json_files(spark, clicks_paths)

    metrics_df = calculate_metrics(impressions_df, clicks_df)
    # metrics_df.coalesce(1).write.mode("overwrite").json("output/")
    print("----------------metrics_df -----------------------")
    metrics_df.show(truncate=False)

    recommendations_df = top_advertisers(impressions_df, clicks_df)
    print("----------------recommendations_df -----------------------")
    recommendations_df.show(truncate=False)
    # recommendations_df.coalesce(1).write.mode("overwrite").json("output/recommendations.json")
    

    median_df = calculate_median_user_spend(impressions_df, clicks_df)
    print("----------------median_df -----------------------")
    median_df.show(truncate=False)
    # median_df.coalesce(1).write.mode("overwrite").json("output/median_spend.json")
    
    spark.stop()


if __name__ == "__main__":
    impressions_files = sys.argv[1].split(",")
    clicks_files = sys.argv[2].split(",")

    main(impressions_files, clicks_files)