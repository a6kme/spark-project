## Questions and Answers

Q1: How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Answer: Changing values for maxOffsetsPerTrigger allowed better throughput of data getting processed by streaming application. Also configuring pollTimeoutMs allowed to reduce round trip while consuming data from Kafka.

Q2: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

Answer: I used maxOffsetsPerTrigger: 200 which helped me provide good throughput for jobs.