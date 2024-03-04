import argparse
import time
from pyspark.sql import SparkSession

def calculate_flight_delay(data_source, output_uri):
    """
    Processes delayed flights data and queries the data to find the delay year wise

    :param data_source: The URI of  flights_delay data CSV, such as 's3://DOC-EXAMPLE-BUCKET/DelayedFlights-updated.csvoutputuri'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/DelayedFlights_{task}_results'.
    """
    with SparkSession.builder.appName("Calculate flight delays").getOrCreate() as spark:
        # Load the flights_delay CSV data
        if data_source is not None:
            flights_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flights_df.createOrReplaceTempView("flight_delays")

        delay_type = ["CarrierDelay","NASDelay", "WeatherDelay", "LateAircraftDelay", "SecurityDelay"]
        iteration = 0

        while iteration < 5:
            execution_time = {}
            for delays in delay_type:
                #start time of the process
                start = time.perf_counter()
                
                # Create a DataFrame of the flight delay according to the delay type
                delay = spark.sql(f"""SELECT Year, avg(({delays} /ArrDelay)*100) AS {delays} 
                                        FROM flight_delays  
                                        GROUP BY Year 
                                        ORDER BY Year ASC""")

                
                #end time of the process
                end = time.perf_counter()

                # Write the results to the specified output URI
                delay.write.option("header", "true").mode("overwrite").csv(output_uri+"/"+str({delays}))
                
                #execution time
                execution_time[delays] = (end-start)


            #write time to csv
            execution_time_df = spark.createDataFrame([(delay, time) for delay, time in execution_time.items()])
            execution_time_df.repartition(1).write.option("header", "true").mode("overwrite").csv(output_uri+"/ExecutionTime "+str(iteration+1))

            iteration +=1

        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV restaurant data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_flight_delay(args.data_source, args.output_uri)
			