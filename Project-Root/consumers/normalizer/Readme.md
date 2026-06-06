


### Steps to execute the consumer


1.) Navigate to the path
...market-data-pipeline/Market-Data-Pipeline-Project/Project-Root


2.) Make sure to enable the virtual env
source venv_3.14/bin/activate


3.) Execute the following command from the Project-Root directory 
python3 -m consumers.normalizer.consumer



### About Kafka polling mechanism

Kafka consumers read data from Kafka topics by establishing an active, pull-based connection to the brokers. They work in parallel using Consumer Groups, coordinate work through partition assignments, and use Offsets to track their reading progress. 



#### Key Mechanisms: 
```
• Pull Model: Consumers actively "pull" or fetch batches of messages from the brokers rather than waiting for brokers to push them, which allows them to process data at their own optimal speed. 
• Consumer Groups: Consumers belong to a consumer group (identified by a ). Consumers in the same group divide the work, allowing for parallel consumption without message duplication. 
• Partition Assignment: Kafka distributes the partitions of a subscribed topic among the consumers in a group. Each partition is consumed by exactly one consumer in a group. 
• Offsets (Bookmarks): Kafka maintains a numerical offset (position) for records in a partition. Consumers track their progress by "committing" these offsets to a special internal Kafka topic, ensuring that if a consumer crashes, it can resume reading exactly where it left off. [3, 8]  
```

#### The Consumption Lifecycle: 
```
1. Subscription & Connect: The consumer connects to the Kafka cluster using the  and subscribes to one or more topics. 
2. The Poll Loop: The application calls the  API. The consumer acts as a long-running loop that periodically sends fetch requests to the cluster, pulling batches of messages into memory for processing. 
3. Group Rebalancing: If a consumer is added, removed, or crashes, Kafka triggers a "rebalance". The partitions are automatically redistributed among the active consumers to ensure high availability and continuous processing. 
4. Offset Committing: Once the consumer successfully processes a message, it commits the offset so Kafka knows the message was handled. Commits can be automated or handled manually.
```
