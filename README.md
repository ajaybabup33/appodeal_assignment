# Appodeal Assignment

## Run

python src/main.py data/impressions.json data/clicks.json

If multiple files:
python src/main.py "data/imp1.json,data/imp2.json" "data/click1.json,data/click2.json"

OR

run via spark-submit

spark-submit --master local[*] src/main.py data/impressions.json data/clicks.json

## Outputs
## 

----------------metrics_df -----------------------
+------+------------+-----------+------+------------------+
|app_id|country_code|impressions|clicks|revenue           |
+------+------------+-----------+------+------------------+
|1     |US          |12         |12    |28.94             |
|2     |IN          |2          |2     |1.3399999999999999|
+------+------------+-----------+------+------------------+

----------------recommendations_df -----------------------
+------+------------+--------------------------+
|app_id|country_code|recommended_advertiser_ids|
+------+------------+--------------------------+
|1     |US          |[12]                      |
+------+------------+--------------------------+

----------------median_df -----------------------
+------------+------------+
|country_code|median_spend|
+------------+------------+
|IN          |0.58        |
|US          |3.19        |
+------------+------------+