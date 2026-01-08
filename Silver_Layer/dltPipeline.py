from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


#Bookings Data
@dp.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df

@dp.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifyDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df


rules = {
    "rule1" : "booking_id is not null",
    "rule2" : "passenger_id is not null"
}

@dp.table(
    name = "silver_bookings"
)
@dp.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    #alternate code, df = dlt.read("trans_booking")#
    return df




#########################################################
#Flights Data


@dp.view(
    name = "trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data/")
    df = df.withColumn("modifyDate", current_timestamp())\
           .withColumn("flight_date", to_date(col("flight_date")))\
           .drop("_rescued_data")
    
    return df


dp.create_streaming_table(
    "silver_flights"
)

dp.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("modifyDate"),
  stored_as_scd_type = "1"
)


########################################################
#Passengers_data

@dp.view(
    name = "trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    df = df.withColumn("modifyDate", current_timestamp())\
           .drop("_rescued_data")
    return df

dp.create_streaming_table(
    "silver_passengers"
)

dp.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modifyDate"),
    stored_as_scd_type = "1"
)


########################################################
#Airports_data


@dp.view(
    name = "trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")
    df = df.withColumn("modifyDate", current_timestamp())\
           .drop("_rescued_data")
    return df

dp.create_streaming_table(
    "silver_airports"
)

dp.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("modifyDate"),
    stored_as_scd_type = "1"
)


##################################################
#Silver business view

# @dp.table(
#     name = "silver_business"
# )
# def silver_business():
#     df = dp.readStream("silver_bookings")\
#         .join(dp.readStream("silver_flights"), ["flight_id"])\
#         .join(dp.readStream("silver_passengers"), ["passenger_id"])\
#         .join(dp.readStream("silver_airports"), ["airport_id"])\
#         .drop("modifyDate")
#     return df


# ########## EXTRA ######
# @dp.table(
#     name = "silver_business_mat"
# )    
# def silver_business_mat():
#     df = dp.read("silver_business")
#     return df
    