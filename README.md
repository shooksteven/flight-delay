# flight-delay

## How to run

### MacOS
In terminal:
`make run`

### Other OS
Install docker and run the following 
<!-- TODO - basically put the contents of makefile here -->

## Assignment:
The files contain comma-separated values of the flight delays files contain historical data for airline flight delays. The columns in the files match the following schema:

Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum, ActualElapsedTime, CRSElapsedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode, Diverted, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay

The goal of this exercise is to show proficient knowledge of Spark and surrounding technologies. Being able to build a simple data pipeline

1. Put all the input files in a folder on your local computer or S3 and load all of the data in.
2. Clean up the data, remove all rows in the flightdelays data where the columns equals the string "NA" or data is missing.
3. The output should only contain the Year, Month, DayofMonth, DepTime, UniqueCarrier, FlightNum, ArrDelay, Origin and Dest
4. Store the output in a big data storage format of your choice (parquet, orc, avro)
5. Load saved data
6. Write code to answer the following questions:
i. Compute the average arrdelay of flights landing in LAX
ii. Compute the average arrdelay of flights where the origin is DEN and the dest is SFO
iii. Determine which dest airport had the highest average arrdelay
7. Assuming that we get a new file every hour. Write a script which would run and on hourly basis to calculate and store the average delay data.
8. Store the results in in a new table (choose the partitioning strategy of your choice).