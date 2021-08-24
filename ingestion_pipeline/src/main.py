# TODO: Add a proper error checking/logging

import os, sys
import datetime, time
import logging
import shutil
 
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)
    logging.getLogger('pyspark').setLevel(logging.ERROR)
    logging.getLogger('py4j').setLevel(logging.ERROR)

    dataDir = os.getenv("DATA_DIR")
    sparkMasterURL = os.getenv("SPARK_MASTER")

    if None in [dataDir, sparkMasterURL] :
        sys.exit("Please specify environment variables:\nLANDING_ZONE_DIR, ARCHIVE_DIR, PARQUET_DIR")

    # Check for new CSV files in the landing zone directory
    landingZoneDir = os.path.join(dataDir, "landing_zone")
    newFiles = list(Path(landingZoneDir).rglob("*.[cC][sS][vV]"))
    if len(newFiles) < 1:    
        logging.info("No new files to process.")
        sys.exit(0)

    logging.info("Ingesting " + str(len(newFiles)) + " new file(s).")

    # Create a temporary processing directory, named with the timestamp to be unique.
    # This is so we don't accidentally process data more than once by accident. Which also enables us to
    #  run the script on demand.
    unixTime = int(time.time())
    processingDir = os.path.join(dataDir, str(unixTime))
    if not os.path.exists(processingDir):
        os.mkdir(processingDir)

    for file in newFiles:
        shutil.move(os.path.join(landingZoneDir, file), processingDir)

    # Note: Here I would also archive the original data  somewhere- just in case :) 

    # Create Spark session & context
    spark = SparkSession.builder.master(sparkMasterURL).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("DEBUG")

    # Manually define the schema - so there are no surprises during production
    schema = StructType([
                StructField("Year",StringType(),True) ,
                StructField("Month",StringType(),True) ,
                StructField("DayofMonth",StringType(),True) ,
                StructField("DayOfWeek",StringType(),True) ,
                StructField("DepTime",StringType(),True) ,
                StructField("CRSDepTime",StringType(),True) ,
                StructField("ArrTime",StringType(),True) ,
                StructField("CRSArrTime",StringType(),True) ,
                StructField("UniqueCarrier",StringType(),True) ,
                StructField("FlightNum",StringType(),True) ,
                StructField("TailNum",StringType(),True) ,
                StructField("ActualElapsedTime",StringType(),True) ,
                StructField("CRSElapsedTime",StringType(),True) ,
                StructField("AirTime",StringType(),True) ,
                StructField("ArrDelay",IntegerType(),True) ,
                StructField("DepDelay",StringType(),True) ,
                StructField("Origin",StringType(),True) ,
                StructField("Dest",StringType(),True) ,
                StructField("Distance",StringType(),True) ,
                StructField("TaxiIn",StringType(),True) ,
                StructField("TaxiOut",StringType(),True) ,
                StructField("Cancelled",StringType(),True) ,
                StructField("CancellationCode",StringType(),True) ,
                StructField("Diverted",StringType(),True) ,
                StructField("CarrierDelay",StringType(),True) ,
                StructField("WeatherDelay",StringType(),True) ,
                StructField("NASDelay",StringType(),True) ,
                StructField("SecurityDelay",StringType(),True) ,
                StructField("LateAircraftDelay",StringType(),True)
    ])

    flightDF = spark.read.csv(landingZoneDir, header="True", sep=",", inferSchema="False", nullValue="NA", schema=schema)
    columnList = ["Year", "Month", "DayofMonth", "DepTime", "UniqueCarrier", "FlightNum", "ArrDelay", "Origin", "Dest"]
    flightDF = flightDF.select(columnList)

    # Write data to disk
    parquetPath = os.path.join(dataDir, "parquet","flightData.parquet")
    # My instinct is to usually partition by date, but I chose to partition by destination because the analytics questions asked all involved the destination airport and didn't really invovle dates.
    # Could partition data here, but instructions say last step
    flightDF.write.partitionBy("Dest").mode('append').parquet(parquetPath)

    # Remove the processing directory and its files
    shutil.rmtree(processingDir)
    logging.info(str(len(newFiles)) + " files successfully ingested")

    # === Analytics Portion === #
    # Normaly this would be a separate project/repo
    logging.info("Computing analytics...")

    schema = StructType([
                StructField("Year",StringType(),True) ,
                StructField("Month",StringType(),True) ,
                StructField("DayofMonth",StringType(),True) ,
                StructField("DepTime",StringType(),True) ,
                StructField("UniqueCarrier",StringType(),True) ,
                StructField("FlightNum",StringType(),True) ,
                StructField("ArrDelay",IntegerType(),True) ,
                StructField("Origin",StringType(),True) ,
                StructField("Dest",StringType(),True)
    ])

    # Could use partitions here...
    parquetDF = spark.read.parquet(parquetPath, schema=schema)
    spark.sql("CREATE TABLE IF NOT EXISTS flights USING parquet OPTIONS (path \"flights.parquet\")")

    # 1. Compute the average arrdelay of flights landing in LAX
    logging.info("Average arrdelay of flights landing in LAX\n" + 
        spark.sql("SELECT AVG(ArrDelay) FROM flights GROUP BY Dest HAVING upper(Dest) LIKE 'LAX'").show())

    # 2. Compute the average arrdelay of flights where the origin is DEN and the dest is SFO
    logging.info("Average arrdelay of flights where the origin is DEN and the dest is SFO\n" + 
    spark.sql("SELECT AVG(ArrDelay) FROM flights WHERE upper(Origin) LIKE 'DEN' AND upper(Dest) LIKE 'SFO' LIMIT 20").show())
    
    # 3. Determine which dest airport had the highest average arrdelay
    logging.info("Which dest airport had the highest average arrdelay\n" + 
    spark.sql("SELECT Dest FROM (SELECT Dest, RANK() OVER (ORDER BY AVG(ArrDelay) DESC) AS rank FROM flights GROUP BY Dest) WHERE rank=1").show())
    
    # Assuming that we get a new file every hour. Write a script which would run and on hourly basis to calculate and store the average delay data.
    parquetPath = os.path.join(dataDir, "parquet","avgFlightDelay.parquet")
    
    avgDelayDF = spark.sql("SELECT AVG(ArrDelay) AS AvgDelay FROM flights")
    
    flightDF.write.partitionBy("AvgDelay").mode('append').parquet(parquetPath) 

    logging.info("Analytics completed successfully")

