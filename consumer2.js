// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs");

// the client ID lets kafka know who's producing the messages
const clientId = "consumer-group2";
// we can define the list of brokers in the cluster
const brokers = ["192.168.1.3:9092"];
// this is the topic to which we want to write messages
const topic = "halim";

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers });
// create a new consumer from the kafka client, and set its group ID
// the group ID helps Kafka keep track of the messages that this client
// is yet to receive
const consumer2 = kafka.consumer({ groupId: clientId });

const consume2 = async () => {
  // first, we wait for the client to connect and subscribe to the given topic
  await consumer2.connect();
  await consumer2.subscribe({ topic, fromBeginning: true });
  await consumer2.run({
    // this function is called every time the consumer gets a new message
    eachMessage: ({ message, partition }) => {
      // here, we just log the message to the standard output
      console.log(
        `consumer 2 received message: ${message.value} from partition ${partition}`
      );
    },
  });
};

consume2().catch((err) => {
  console.error("error in consumer: ", err);
});
