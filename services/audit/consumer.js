import amqp from "amqplib";
import { writeFileSync, appendFileSync } from "node:fs";

const EXHANGE = "order.events";
const QUEUE = "audit.q";
const RK = "#";

const AUDIT_FILE = "audit.log";

try {
  writeFileSync(AUDIT_FILE, "", { flag: "wx" });
} catch (e) {}

const processedMessages = new Set();

async function setupTopology(ch) {
  await ch.assertExchange(EXHANGE, "topic", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.bindQueue(QUEUE, EXHANGE, RK);
}

async function auditEvent(evt) {
  const eventId = evt.id;

  if (processedMessages.has(eventId)) {
    console.log(`Event ${eventId} already audited - skipping`);
    return;
  }

  const auditEntry = {
    timeStamp: new Date().toISOString(),
    eventId,
    eventType: evt.type,
    data: evt.data,
    originalTimestamp: evt.occurredAt,
  };

  appendFileSync(AUDIT_FILE, JSON.stringify(auditEntry) + "\n");
  processedMessages.add(eventId);

  console.log(`Audited event: ${evt.type} (${eventId})`);
}

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const ch = await conn.createChannel();

  await setupTopology(ch);
  ch.prefetch(10);

  await ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      try {
        const evt = JSON.parse(msg.content.toString());
        await auditEvent(evt);
        ch.ack(msg);
      } catch (e) {
        console.error(`Audit error:`, e);
        ch.nack(msg, false, true);
      }
    },
    { noAck: false }
  );

  process.on("SIGINT", async () => {
    await ch.close();
    await conn.close();
    process.exit(0);
  });
}

start().catch(console.error);
