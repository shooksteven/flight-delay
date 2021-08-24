import os, sys
import datetime, time
import logging
import shutil
 
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import types 

if __name__ == "__main__":
    logger = logging.getLogger('flight_application')
    logger.setLevel(logging.DEBUG)

    dataDir = os.getenv("DATA_DIR")
    sparkMasterURL = os.getenv("SPARK_MASTER")

    if None in [dataDir, sparkMasterURL] :
        sys.exit("Please specify environment variables:\nLANDING_ZONE_DIR, ARCHIVE_DIR, PARQUET_DIR")

    # Check for new CSV files in the landing zone directory
    landingZoneDir = os.path.join(dataDir, "landing_zone")
    newFiles = list(Path(landingZoneDir).rglob("*.[cC][sS][vV]"))
    if len(newFiles) < 1:    
        logger.info("No new files to process.")
        sys.exit(0)

    logger.info("Ingesting " + str(len(newFiles)) + " new file(s).")

    # Create a temporary processing directory, named with the timestamp to be unique.
    # This is so we don't accidentally process data more than once by accident, but also enables us to
    #  run the script manually.
    unixTime = int(time.time())
    processingDir = os.path.join(dataDir, str(unixTime))
    if not os.path.exists(processingDir):
        os.mkdir(processingDir)

    for file in newFiles:
        shutil.move(os.path.join(landingZoneDir, file), processingDir)

    # Note: Here I would also archive the original data - just in case :) 

    # Create Spark session & context
    spark = SparkSession.builder.master(sparkMasterURL).getOrCreate()
    sc = spark.sparkContext

    flightDF = spark.read.csv("data/sample (1).csv", header="True", sep=",", inferSchema="False", nullValue="NA")
    columnList = ["Year", "Month", "DayofMonth", "DepTime", "UniqueCarrier", "FlightNum", "ArrDelay", "Origin", "Dest"]
    flightDF = flightDF.select(columnList)
    flightDF = flightDF.withColumn("ArrDelay", flightDF.ArrDelay.cast("int"))
    flightDF.write.parquet(os.path.join(dataDir, "parquet", str(unixTime), ".parquet"))

    # Remove the processing directory and its files
    shutil.rmtree(processingDir)
    logger.info(str(len(newFiles)) + " files successfully ingested")