import amqp from "amqplib";
import nodemailer from "nodemailer";

const EXCHANGE = "order.events";
const QUEUE = "notify.q";
const RK = "order.created";

const smtp = {
  host: process.env.SMTP_HOST || "mailpit",
  port: Number(process.env.SMTP_PORT || 1025),
  secure: false,
};

async function setupTopology(ch) {
  // Main Queue
  await ch.assertExchange(EXCHANGE, "topic", { durable: true });

  // DLQ for failed messages
  await ch.assertExchange("order.dlx", "direct", { durable: true });

  await ch.assertQueue(QUEUE, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": "order.dlx",
      "x-dead-letter-routing-key": "notify.retry",
    },
  });

  await ch.bindQueue(QUEUE, EXCHANGE, RK);

  await ch.assertQueue("notify.retry.q", {
    durable: true,
    arguments: {
      "x-message-ttl": 3000,
      "x-dead-letter-exchange": "order.events",
      "x-dead-letter-routing-key": "order.created",
    },
  });

  await ch.bindQueue("notify.retry.q", "order.dlx", "notify.retry");

  await ch.assertQueue("notify.dlq", { durable: true });
  await ch.bindQueue("notify.dlq", "order.dlx", "notify.dead");
}

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const ch = await conn.createChannel();

  await setupTopology(ch);

  ch.prefetch(10);
  const transporter = nodemailer.createTransport(smtp);

  await ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;
      try {
        const evt = JSON.parse(msg.content.toString());

        const info = await transporter.sendMail({
          from: "no-reply@example.dev",
          to: "user@example.dev",
          subject: `Order Received ${evt.data.orderId}`,
          text: `Thanks! Order ${evt.data.orderId}`,
          html: `<p>Thanks! Order <b>${evt.data.orderId}</b></p>`,
        });

        ch.ack(msg);
      } catch (e) {
        const headers = msg.properties.headers || {};
        const attempts = Number(headers["x-attempts"] || 0);

        if (attempts >= 3) {
          console.log(`Message failed ${attempts} times, sending to DLQ`);

          ch.publish("order.dlx", "notify.dead", msg.content, {
            contentType: "application/json",
            persistent: true,
            headers: {
              ...headers,
              "x-attempts": attempts + 1,
              "x-final-error": e,
            },
          });
          ch.ack(msg);
        } else {
          console.log(`Attempt ${attempts + 1}/3 failed, retrying in 1s`);
          ch.publish("order.dlx", "notify.retry", msg.content, {
            contentType: "application/json",
            persistent: true,
            headers: { ...headers, "x-attempts": attempts + 1 },
          });

          ch.ack(msg);
        }
      }
    },
    { noAck: false }
  );

  console.log("Consumer started and listening...");

  process.on("SIGINT", async () => {
    ch.close();
    await conn.close();
    process.exit(0);
  });
}

start().catch((e) => {
  console.error(e);
  process.exit(1);
});
