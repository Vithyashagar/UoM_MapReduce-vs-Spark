-- Hive Codes
-- Table creation
CREATE EXTERNAL TABLE flights_delay (
    Record INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay INT,
    DepDelay INT,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted INT,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://s3-emr-248382a-bigdata/data/'
TBLPROPERTIES("skip.header.line.count"="1");


-- CarrierDelay
"SELECT Year, avg((CarrierDelay /ArrDelay)*100) from flights_delay GROUP BY Year").show()
-- 
"SELECT Year, avg((NASDelay /ArrDelay)*100) from flights_delay GROUP BY Year").show()
-- WeatherDelay
"SELECT Year, avg((WeatherDelay /ArrDelay)*100) from flights_delay GROUP BY Year").show()
-- LateAircraftDelay
"SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from flights_delay GROUP BY Year").show()
-- SecurityDelay
"SELECT Year, avg((SecurityDelay /ArrDelay)*100) from flights_delay GROUP BY Year").show()
