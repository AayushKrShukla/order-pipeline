import amqp from "amqplib";
import Database from "better-sqlite3";
import { EXCHANGE, ROUTING_KEYS, SAGA_STATUS } from "./contracts.js";

const db = new Database("sagas.db");
db.pragma("journal_mode = VAL");

db.exec(
  `
    CREATE TABLE IF NOT EXISTS sagas (
      saga_id TEXT PRIMARY KEY,
      idempotency_key TEXT UNIQUE NOT NULL,
      order_id TEXT NOT NULL,
      customer_id TEXT NOT NULL,
      status TEXT NOT NULL,
      current_step TEXT NOT NULL,
      order_data TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS processed_messages (
      message_id TEXT PRIMARY_KEY,
      idempotency_key TEXT NOT NULL,
      event_type TEXT NOT NULL,
      processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `
);

const QUEUE = "orchestrator.q";
let channel;

async function setupTopology(ch) {
  await ch.assertExchange(EXCHANGE, "topic", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });

  await ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEYS.ORDER_CREATED);
  await ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEYS.RESERVE_SUCCEEDED);
  await ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEYS.RESERVE_FAILED);
  await ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEYS.PAYMENT_SUCCEEDED);
  await ch.bindQueue(QUEUE, EXCHANGE, ROUTING_KEYS.PAYMENT_FAILED);
}

async function publishEvent(routingKey, data, idempotencyKey) {
  const event = {
    type: routingKey,
    id: `evt_${Date.now()}_${Math.random().toString(36).substring(2, 11)}`,
    idempotencyKey,
    occurredAt: new Date().toISOString(),
    data,
  };

  const payload = Buffer.from(JSON.stringify(event));
  await channel.publish(EXCHANGE, routingKey, payload, {
    contentType: "application/json",
    persistent: true,
  });

  console.log(`Published ${routingKey} for saga ${idempotencyKey}`);
}

const createSaga = db.prepare(
  `INSERT INTO sagas (saga_id, idempotency_key, order_id, customer_id, status, current_step, order_data) VALUES (?, ?, ?, ?, ?, ?, ?)
  `
);

const updateSaga = db.prepare(`
  UPDATE sagas
  SET status = ?, current_step = ?, updated_at = CURRENT_TIMESTAMP
  WHERE idempotency_key = ?
  `);

const getSaga = db.prepare(`
    SELECT * FROM sagas WHERE idempotency_key = ?
  `);

const markProcessed = db.prepare(
  `
  INSERT OR IGNORE INTO processed_messages (message_id, idempotency_key, event_type) VALUES (?, ?, ?)
  `
);

const isProcessed = db.prepare(`
  SELECT 1 FROM processed_messages WHERE idempotency_key = ? AND event_type = ?
`);

async function handleOrderCreated(event) {
  const { idempotencyKey, data } = event;

  if (isProcessed.get(idempotencyKey, "order.created")) {
    console.log(`Order ${idempotencyKey} already processed- skipping`);
    return;
  }

  const sagaId = `saga_${Date.now()}`;

  try {
    createSaga.run(
      sagaId,
      idempotencyKey,
      data.orderId,
      data.customerId,
      SAGA_STATUS.STARTED,
      "reserve_inventory",
      JSON.stringify(data)
    );

    await publishEvent(
      ROUTING_KEYS.RESERVE_REQUEST,
      {
        orderId: data.orderId,
        items: data.items,
      },
      idempotencyKey
    );
    markProcessed.run(sagaId, idempotencyKey, "order.created");
    console.log(`Started saga ${sagaId} for order ${data.orderId}`);
  } catch (error) {
    console.error("Failed to start saga:", error);
  }
}

async function handleReserveSucceeded(event) {
  const { idempotencyKey, data } = event;

  if (isProcessed.get(idempotencyKey, "reserve.succeeded")) {
    console.log(
      `Reserve success ${idempotencyKey} already processed - skipping`
    );
    return;
  }

  const saga = getSaga.get(idempotencyKey);
  if (!saga) {
    console.error(`No saga found for ${idempotencyKey}`);
    return;
  }

  updateSaga.run(
    SAGA_STATUS.INVENTORY_RESERVED,
    "process.payment",
    idempotencyKey
  );

  await publishEvent(
    ROUTING_KEYS.PAYMENT_REQUEST,
    {
      orderId: data.orderId,
      amount: data.totalAmount || 99.99,
    },
    idempotencyKey
  );

  markProcessed.run(saga.saga_id, idempotencyKey, "reserve.succeeded");
  console.log(`Saga ${saga.saga_id}: inventory reserved, requesting payment`);
}

async function handleReserveFailure(event) {
  const { idempotencyKey } = event;

  const saga = getSaga.get(idempotencyKey);
  if (!saga) return;

  updateSaga.run(SAGA_STATUS.FAILED, "failed", idempotencyKey);

  await publishEvent(
    ROUTING_KEYS.SAGA_FAILED,
    {
      reason: "inventory_unavailable",
      orderId: JSON.parse(saga.order_data).orderId,
    },
    idempotencyKey
  );

  console.log(`Saga ${saga.saga_id}: failed due to inventory`);
}

async function handlePaymentSucceeded(event) {
  const { idempotencyKey, data } = event;

  if (isProcessed.get(idempotencyKey, "payment.succeeded")) {
    return;
  }

  const saga = getSaga.get(idempotencyKey);
  if (!saga) return;

  updateSaga.run(SAGA_STATUS.COMPLETED, "completed", idempotencyKey);

  await publishEvent(
    ROUTING_KEYS.NOTIFY_REQUEST,
    {
      orderId: data.orderId,
      message: "Order confirmed and payment processed",
    },
    idempotencyKey
  );

  await publishEvent(
    ROUTING_KEYS.SAGA_COMPLETED,
    {
      orderId: data.orderId,
    },
    idempotencyKey
  );

  markProcessed.run(saga.saga_id, idempotencyKey, "payment.succeeded");
  console.log(`Saga ${saga.saga_id}: completed successfully`);
}

async function handlePaymentFailed(event) {
  const { idempotencyKey, data } = event;

  const saga = getSaga.get(idempotencyKey);
  if (!saga) return;

  updateSaga.run(SAGA_STATUS.COMPENSATING, "compensating", idempotencyKey);

  await publishEvent(
    ROUTING_KEYS.INVENTORY_RELEASE,
    {
      orderId: data.orderId,
      reason: "payment_failed",
    },
    idempotencyKey
  );

  updateSaga.run(SAGA_STATUS.FAILED, "failed", idempotencyKey);

  await publishEvent(
    ROUTING_KEYS.SAGA_FAILED,
    {
      reason: "payment_failed",
      orderId: data.orderId,
    },
    idempotencyKey
  );

  console.log(`Saga ${saga.saga_id}: compensating and failing due to payment`);
}

async function processMessage(msg) {
  const event = JSON.parse(msg.content.toString());
  const routingKey = msg.fields.routingKey;

  console.log(`Processing ${routingKey} for ${event.idempotencyKey}`);

  try {
    switch (routingKey) {
      case ROUTING_KEYS.ORDER_CREATED:
        await handleOrderCreated(event);
        break;
      case ROUTING_KEYS.RESERVE_SUCCEEDED:
        await handleReserveSucceeded(event);
        break;
      case ROUTING_KEYS.RESERVE_FAILED:
        await handleReserveFailure(event);
        break;
      case ROUTING_KEYS.PAYMENT_SUCCEEDED:
        await handlePaymentSucceeded(event);
        break;
      case ROUTING_KEYS.PAYMENT_FAILED:
        await handlePaymentFailed(event);
        break;
      default:
        console.log(`Unknown routing key: ${routingKey}`);
    }
  } catch (error) {
    console.error(`Error processing ${routingKey}:`, e);
    channel.nack(msg, false, false);
  }
}

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  channel = await conn.createChannel();

  await setupTopology(channel);
  channel.prefetch(1);

  await channel.consume(QUEUE, processMessage, { noAck: false });

  console.log("Orchestrator service started");

  process.on("SIGINT", async () => {
    db.close();
    await channel.close();
    await conn.close();
    process.exit(0);
  });
}

start().catch(console.error);
