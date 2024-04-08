import { readFileSync } from "node:fs";
import type { KafkaConfig, KafkaMessage, ProducerConfig } from "kafkajs";
import { Kafka } from "kafkajs";
import {
  Config,
  ReaderConstructor,
  SimpleStream,
  WriterConstructor,
} from "../connectors";

export interface SASLOptions {
  mechanism: "plain";
  username: string;
  password: string;
}

export interface BrokerConfig {
  hosts: string[];
  ssl?: boolean;
  sasl?: SASLOptions;
}

export interface ConsumerConfig {
  groupId: string;
  metadataMaxAge?: number;
  sessionTimeout?: number;
  rebalanceTimeout?: number;
  heartbeatInterval?: number;
  maxBytesPerPartition?: number;
  minBytes?: number;
  maxBytes?: number;
  maxWaitTimeInMs?: number;
  allowAutoTopicCreation?: boolean;
  maxInFlightRequests?: number;
  readUncommitted?: boolean;
  rackId?: string;
}

export interface CSTopic {
  topic: string;
  fromBeginning?: boolean;
}
export interface KafkaReaderConfig {
  topic: {
    name: string;
    fromBeginning?: boolean;
  };
  consumer: ConsumerConfig;
  broker: string | BrokerConfig;
}

export const startKafkaStreamReader: ReaderConstructor<KafkaReaderConfig> = (
  config,
) => {
  const brokerConfig: unknown = {};
  if (typeof config.broker === "string" || config.broker instanceof String) {
    Object.assign(
      <BrokerConfig>brokerConfig,
      JSON.parse(readFileSync(<string>config.broker, "utf-8")),
    );
  } else {
    Object.assign(<BrokerConfig>brokerConfig, config.broker);
  }
  if (brokerConfig && (<BrokerConfig>brokerConfig).hosts) {
    (<KafkaConfig>brokerConfig).brokers = (<BrokerConfig>brokerConfig).hosts;
  }

  const kafka = new Kafka(<KafkaConfig>brokerConfig);

  const consumer = kafka.consumer(config.consumer);

  const stream = new SimpleStream<string>(async () => {
    await consumer.disconnect();
    await consumer.stop();
  });

  const init = async () => {
    await consumer.connect();
    await consumer.subscribe({
      topic: config.topic.name,
      fromBeginning: config.topic.fromBeginning,
    });

    consumer
      .run({
        async eachMessage({
          topic,
          message,
        }: {
          topic: string;
          message: KafkaMessage;
        }) {
          if (topic === config.topic.name) {
            const element = message.value?.toString() ?? "";
            stream.push(element).catch((error) => {
              throw error;
            });
          }
        },
      })
      .catch((error) => {
        throw error;
      });
  };

  return { reader: stream, init };
};

export interface KafkaWriterConfig {
  topic: {
    name: string;
  };
  producer: ProducerConfig;
  broker: BrokerConfig | string;
}

export const startKafkaStreamWriter: WriterConstructor<KafkaWriterConfig> = (
  config,
) => {
  const topic = config.topic.name;

  const brokerConfig: unknown = {};
  if (typeof config.broker === "string" || config.broker instanceof String) {
    Object.assign(
      <BrokerConfig>brokerConfig,
      JSON.parse(readFileSync(<string>config.broker, "utf-8")),
    );
  } else {
    Object.assign(<BrokerConfig>brokerConfig, config.broker);
  }
  if (brokerConfig && (<BrokerConfig>brokerConfig).hosts) {
    (<KafkaConfig>brokerConfig).brokers = (<BrokerConfig>brokerConfig).hosts;
  }

  const kafka = new Kafka(<KafkaConfig>brokerConfig);

  const producer = kafka.producer(config.producer);
  const init = () => producer.connect();

  const push = async (item: string): Promise<void> => {
    await producer.send({ topic, messages: [{ value: item }] });
  };

  const end = async (): Promise<void> => {
    await producer.disconnect();
  };

  return { writer: { push, end }, init };
};
