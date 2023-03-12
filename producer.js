// import the `Kafka` instance from the kafkajs library
const { Kafka } = require("kafkajs");

// the client ID lets kafka know who's producing the messages
const clientId = "producer-group1";
// we can define the list of brokers in the cluster
const brokers = ["192.168.1.3:9092"];
// this is the topic to which we want to write messages
const topic = "halim";

// initialize a new kafka client and initialize a producer from it
const kafka = new Kafka({ clientId, brokers });
const producer = kafka.producer();

// we define an async function that writes a new message each second
const produce = async () => {
  await producer.connect();
  let i = 0;

  // after the produce has connected, we start an interval timer
  setInterval(async () => {
    try {
      // send a message to the configured topic with
      // the key and value formed from the current value of `i`
      let [res] = await producer.send({
        topic,
        messages: [
          {
            key: String(i),
            value: "this is message " + i,
          },
        ],
      });

      // if the message is written successfully, log it and increment `i`
      console.log(
        `Producer has produced : ${i} to topic ${res.topicName} on partition ${res.partition} and baseOffset ${res.baseOffset}`
      );
      i++;
    } catch (err) {
      console.error("could not write message " + err);
    }
  }, 3000);
};

produce().catch((err) => {
  console.error("error in producer: ", err);
});
