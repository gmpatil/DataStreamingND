# San Framsisco Crime Statistics

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
Below are some of the SparkSession properties that effect the throughput and latency of the data.
, and writeStream.trigger parameters:
- maxOffsetsPerTrigger (readStream.format("kafka").option) 
- minPartitions (readStream.format("kafka").option) 
- processingTime (writeStream.trigger)
- continous (writeStream.trigger)

SparkSession's Kafka stream read option "maxOffsetsPerTrigger" is one of the main parameter which allows us to throttle up and down the rate at which stream messages are processed. If enough Executor resources are available and Trigger execution duration is consistently less than Stream trigger "processingTime", we can increase the "maxOffsetsPerTrigger" to increase the throughput. Trigger by "processingTime" is used for Micro-Batch Stream Processing, and this may cause some of records to be  processed with latency of 100s of ms. If latency is critical, one might use "continuous" parameter with appropriate checkpoint interval to get significantly improved latency times [1].


## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

For this project "maxOffsetsPerTrigger" and "processingTime" parameters were tuned to get better throughput. Starting with  "maxOffsetsPerTrigger" around 100 and  "processingTime" configured around 3 seconds, Trigger execution and add-batch times exceeded the "processingTime". Increased the processing "processingTime" to 15 secs and also able to increase  "maxOffsetsPerTrigger" to 200, and then upto 300 without Trigger execution time exceeding the "processingTime" configured. 


[1] https://databricks.com/blog/2018/03/20/low-latency-continuous-processing-mode-in-structured-streaming-in-apache-spark-2-3-0.html