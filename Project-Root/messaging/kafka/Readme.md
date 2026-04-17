## References 


Apache Kafka - https://hub.docker.com/r/apache/kafka

AioKafka - https://aiokafka.readthedocs.io/en/stable/

Tutorial - https://medium.com/@sirajul.anik/apache-kafka-understanding-how-to-produce-and-consume-messages-9744c612f40f



### Commands

path inside container - /opt/kafka/bin

#### Creating Topic
./kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

#### Creating Listing Topics
./kafka-topics.sh --bootstrap-server localhost:9092 --list

#### Consuming Messages from Topic
./kafka-console-consumer.sh  --bootstrap-server localhost:9092 --topic test-topic --from-beginning

#### Producing the messages
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic




## Explanation
A Kafka Broker is a server in a Kafka cluster that stores data and serves client requests, acting as a node in a distributed system. A Bootstrap Server is a configuration property (bootstrap.servers) providing a list of broker addresses that clients (producers/consumers) use to initially connect to and discover all other active brokers in the cluster. 



<b>Listeners</b> is what the broker will use to create server sockets.
[where broker accepts connections]

<b>Advertised.listeners</b> is what clients will use to connect to the brokers.


<b>Bootstrap server</b> is only for discovery, not for actual data transfer.

<b>Client → bootstrap server → metadata → reconnect to correct broker</b>


Client connects → Broker returns advertised address → Client follows it


#### What is PLAINTEXT?

A transport protocol configuration, not a listener name
| Protocol       | Meaning           |
| -------------- | ----------------- |
| PLAINTEXT      | No encryption     |
| SSL            | TLS encryption    |
| SASL_PLAINTEXT | Auth only         |
| SASL_SSL       | Auth + encryption |



### About the consensus
Kafka (without Zookeeper) uses Raft consensus
Quorum voters = controllers participating in consensus
Kafka needs to manage:

Topic metadata
Partition leaders
ISR (in-sync replicas)

Earlier → Zookeeper
Now → KRaft (internal Raft cluster)


----------------------------------------------------------------------------------------------------------------------------------------------------------------

## Understanding with Analogy

Imagine Kafka is a huge post office that can deliver millions of letters (messages) very fast.

Broker = One actual post office building.
Cluster = Many post office buildings working together.
Client (your Python code) = A person who wants to send or receive letters.

Now, the tricky part is: How does the person (client) find the correct post office and start talking to it?
This is where bootstrap_servers, KAFKA_LISTENERS, and KAFKA_ADVERTISED_LISTENERS come in.



#### Part 2: Individual Concepts (Simple + Low-level)

1. <b>Broker </b>

Simple meaning: The actual Kafka server that stores your data and handles read/write requests.
Low-level: It is a JVM process running on a machine (or Docker container). It has its own ID (broker ID), listens on certain ports, and manages topics, partitions, and replication.
In your case, you have one broker running inside Docker.

2. <b>Bootstrap Server(s) </b>

Simple meaning: The first known address your client uses to say "Hello, I'm here".
Low-level: It is just a list of broker addresses (host:port) that the client connects to initially.
Once connected, the broker sends back the full cluster metadata (list of all brokers, their addresses, etc.).
Example: bootstrap_servers='localhost:9092'

Important: Even if you have 10 brokers in a big cluster, you usually give only 2–3 bootstrap servers. The client then discovers the rest automatically.


3. <b>KAFKA_LISTENERS</b>

Simple meaning: "Hey Kafka, please open these doors (ports) and listen for people knocking."
Low-level: This setting tells the broker which network interfaces and ports it should bind to.Syntax: ListenerName://Host:PortExample:YAMLKAFKA_LISTENERS: EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
0.0.0.0 = listen on all network interfaces
EXTERNAL and CONTROLLER are just names you give to these listeners (you can call them anything: PLAINTEXT, EXTERNAL, INTERNAL, etc.)


4. <b>KAFKA_ADVERTISED_LISTENERS</b>

Simple meaning: "After someone connects to me, tell them to use this address for future communication."
Low-level: This is the most important and confusing part.When a client connects using the bootstrap server, Kafka replies with:"Okay, from now on, talk to me using this address: localhost:9092"This address is taken from KAFKA_ADVERTISED_LISTENERS.Why is this needed?
Because the broker may be behind NAT, Docker, cloud, etc. The address the client used to connect initially may not be the best address for ongoing communication.

5. <b>INTERNAL:// vs EXTERNAL:// (Listener Names)</b>
These are just labels you give to different ways of accessing the same broker:

EXTERNAL:// → For clients outside Docker (your Python code on your laptop)
INTERNAL:// → For clients inside the Docker network (Kafka UI, other services running in Docker)

You can name them anything (PLAINTEXT, CLIENT, DOCKER, etc.). The name just has to match between KAFKA_LISTENERS and KAFKA_ADVERTISED_LISTENERS.




## How Everything Works Together


<b>Let’s imagine the complete flow when your Python consumer starts:</b>

* You write in your code:Pythonbootstrap_servers='localhost:9092'
* Your consumer connects to localhost:9092 (this is the bootstrap server).
* Inside Docker, the broker receives this connection because of:YAMLKAFKA_LISTENERS: EXTERNAL://0.0.0.0:9092→ It was listening on port 9092.
* After the initial handshake, the broker looks at:YAMLKAFKA_ADVERTISED_LISTENERS: EXTERNAL://localhost:9092and tells your consumer:"From now on, keep talking to me at localhost:9092"
* Your consumer says "Okay" and continues using localhost:9092.
* If you had Kafka UI running inside Docker, you would want Kafka to advertise kafka:9092 (the Docker service name) instead. That’s why we sometimes need multiple advertised listeners.


## Some Extra Points
* <b> To check if the port is not blocked by any firewall</b>  - nc -zv \<IP ADDR> 9092
* <b>Check IP Address in your MAC</b> ipconfig getifaddr en0

## Contributor
Sagar Gulati