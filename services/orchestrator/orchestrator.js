import amqp from "amqplib";
import Database from "better-sqlite3";
import { EXCHANGE, ROUTING_KEYS, SAGA_STATUS } from "./contracts";

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
    occuredAt: new Date().toISOString(),
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
  INSERT OR IGNORE INTO processed_messaged (message_id, idempotency_key, event_type) VALUES (?, ?, ?)
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