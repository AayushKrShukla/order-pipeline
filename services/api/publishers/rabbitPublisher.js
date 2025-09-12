import amqp from "amqplib";

const EXCHANGE = "order.events";
const TYPE = "topic";
let connection;
let channel;

export async function initRabbit(url) {
  connection = await amqp.connect(url);

  channel = await connection.createConfirmChannel();
  await channel.assertExchange(EXCHANGE, TYPE, { durable: true });

  channel.on("error", (e) => console.error("AMQP channel error", e));
  channel.on("close", () => console.warn("AMQP channel closed"));
}

export async function publishOrderCreated(event) {
  if (!channel) throw new Error("RabbitMQ channel not initialized");

  const key = "order.created";
  const payload = Buffer.from(JSON.stringify(event));
  return new Promise((resolve, reject) => {
    channel.publish(
      EXCHANGE,
      key,
      payload,
      { contentType: "application/json", persistent: true },
      (err) => {
        if (err) return reject(err);
        resolve(true);
      }
    );
  });
}

export async function closeRabbit() {
  try {
    await channel?.close();
  } catch {}
  try {
    await connection?.close();
  } catch {}
}
