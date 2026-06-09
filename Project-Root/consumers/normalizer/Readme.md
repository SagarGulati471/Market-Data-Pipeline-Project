


### Steps to execute the consumer


1.) Navigate to the path
...market-data-pipeline/Market-Data-Pipeline-Project/Project-Root


2.) Make sure to enable the virtual env
source venv_3.14/bin/activate


3.) Execute the following command from the Project-Root directory 
python3 -m consumers.normalizer.consumer




---

### Dead Letter Topic (DLT)

#### What problem does a DLT solve?

In a Kafka-based pipeline, messages within a partition are processed sequentially. If a consumer encounters a malformed, invalid, or unexpected message and repeatedly fails to process it, the consumer cannot advance beyond that offset.

This creates a **"poison pill" message** situation, where a single bad record blocks all subsequent messages in the same partition from being processed.

A "poison pill message" typically refers to a malformed or corrupt data packet in event-driven systems (like Apache Kafka or RabbitMQ) that consistently crashes the receiving application.

#### How can a bad message stall the pipeline?

Consider the following sequence:

* Offset 100 → Processed successfully
* Offset 101 → Processed successfully
* Offset 102 → Processing fails
* Offset 103 → Valid message
* Offset 104 → Valid message

If offset 102 is never acknowledged (committed), Kafka continues to deliver the same failed message whenever the consumer retries or restarts. As a result:

* *Offset* 102 keeps failing.
* Offsets 103 and 104 are never reached.
* The partition becomes effectively stalled.

A single bad message can therefore block thousands or millions of valid messages behind it.

#### What is a Dead Letter Topic?

A Dead Letter Topic (DLT) is a dedicated Kafka topic used to store messages that cannot be processed successfully.

Instead of repeatedly retrying the same failing message, the consumer:

1. Copies the original message to the DLT.
2. Records failure details such as the error, source topic, partition, and offset.
3. Commits the original offset.
4. Continues processing the next message.

This allows the pipeline to continue operating while preserving the failed record for later investigation.

#### Why is a DLT useful?

* Prevents a single bad message from blocking an entire partition.
* Preserves failed messages instead of silently dropping them.
* Enables debugging and root-cause analysis.
* Allows failed records to be replayed after the underlying issue is fixed.
* Improves overall reliability and availability of the streaming system.

#### Design Principle

The DLT mechanism prioritizes continuous pipeline operation. Failed messages are isolated and stored for later analysis, while valid messages continue to flow through the system without interruption.



---


# About Kafka
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
