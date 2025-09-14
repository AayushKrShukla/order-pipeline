import amqp from "amqplib";
import Database from "better-sqlite3";

const EXCHANGE = "order.events";
const QUEUE = "inventory.q";

const db = new Database("inventory.db");
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    total_stock INTEGER DEFAULT 0
    reserved_stock INTEGER DEFAULT 0
    available_stock INTEGER GENERATED ALWAYS AS (total_stock - reserved_stock) STORED
  );
  
  CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL,
    order_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved'
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(idempotency_key, sku)
  );

  CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(idempotency_key, event_type)
  );
`);

const insertProduct = db.prepare(
  "INSERT OR REPLACE INTO products (sku, name, total_stock, reserved_stock) values (?, ?, ?, COALESCE((SELECT reserved_stock FROM products WHERE sku = ?), 0))"
);
insertProduct.run("SKU-1", "Product A", 100, "SKU-1");
insertProduct.run("SKU-2", "Product B", 50, "SKU-2");
insertProduct.run("SKU-3", "Product C", 25, "SKU-3");
console.log("Sample data seeded");

let channel;

async function setupTopology(ch) {
  await ch.assertExchange(EXCHANGE, "topic", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });

  await ch.bindQueue(QUEUE, EXCHANGE, "order.created");
  await ch.bindQueue(QUEUE, EXCHANGE, "reserve.request");
  await ch.bindQueue(QUEUE, EXCHANGE, "inventory.release");
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
    peristent: true,
  });

  console.log(`Published ${routingKey} for ${idempotencyKey}`);
}

const isProcessed = db.prepare(
  `SELECT 1 from processed_messages WHERE idempotency_key = ? AND event_type = ?`
);

const markProcessed = db.prepare(
  "INSERT OR IGNORE INTO processed_messages (message_id, idempotency_key, event_type) VALUES (?, ?, ?)"
);

const checkAvailability = db.prepare(`
  SELECT sku, available_stock FROM products WHERE sku = ?  
`);

const reserveStock = db.prepare(`
  UPDATE products SET reversed_stock = reserved_stock + ? WHERE sku = ?
  `);

const releaseStock = db.prepare(`
  UPDATE products SET reversed_stock = reserved_stock - ? WHERE sku = ? and reserved_stock >= ?
  `);

const createReservation = db.prepare(
  "INSERT INTO reservations (idempotency_key, order_id, sku, quantity, status) VALUES (?, ?, ?, ?, 'reserved)"
);

const releaseReservation = db.prepare(
  `UPDATE reservations
  SET status = 'released
  WHERE idempotency_key = ? AND sku = ? AND status = 'reserved'`
);

const getReservations = db.prepare(
  `SELECT * FROM reservations WHERE idempotency_key = ? AND status = 'reserved'`
);

async function handleReserveRequest(event) {
  const { idempotencyKey, data } = event;
  if (isProcessed.get(idempotencyKey, "reserve.request")) {
    console.log(`Reserve request ${idempotencyKey} already processed`);
    return;
  }

  const { orderId, items } = data;
  console.log(`Processing reserve request for order ${orderId}`);

  const unavailableItems = [];
  for (const item of items) {
    const product = checkAvailability.get(item.sku);
    if (!product) {
      unavailableItems.push({ sku: item.sku, reason: "product_not_found" });
    } else if (product.available_stock < item.qty) {
      unavailableItems.push({
        sku: item.sku,
        reason: "insufficient_stock",
        available_stock: product.available_stock,
        requested: item.qty,
      });
    }
  }

  if (unavailableItems.length > 0) {
    await publishEvent(
      "reserve.failed",
      {
        orderId,
        unavailableItems,
      },
      idempotencyKey
    );

    markProcessed.run(`msg_${Date.now()}`, idempotencyKey, "reserve.request");
    return;
  }

  const transaction = db.transaction(() => {
    let totalAmount = 0;

    for (const item of items) {
      reserveStock.run(item.qty, item.sku);
      createReservation.run(idempotencyKey, orderId, item.sku, item.qty);
      totalAmount += item.qty * 29.99;
    }

    markProcessed.run(`msg_${Date.now()}`, idempotencyKey, "reserve.request");
    return totalAmount;
  });

  const totalAmount = transaction();

  await publishEvent("reserve.succeeded", {
    orderId,
    reservedItems: items,
    totalAmount,
  }, idempotencyKey);

  console.log(`Reserved inventory for order ${orderId}: ${items.length} items`);
}

async function handleInventoryRelease(event) {
  const { idempotencyKey, data } = event;

  if (isProcessed.get(idempotencyKey, "inventory.release")) {
    console.log(`Release request ${idempotencyKey} already processed`);
    return;
  }

  const { orderId, items } = data;
  console.log(`Releasing inventory for order ${orderId}, reason: ${reason}`);

  const reservations = getReservations.all(idempotencyKey);

  if (reservations.length === 0) {
    console.log(`No active reservation for ${idempotencyKey}`);
    markProcessed.run(`msg_${Date.now()}`, idempotencyKey, "inventory.release");
    return;
  }

  const transaction = db.transaction(() => {
    for (const reservation of reservations) {
      releaseStock.run(
        reservation.quantity,
        reservation.sku,
        reservation.quantity
      );
      releaseReservation.run(idempotencyKey, reservation.sku);
    }
    markProcessed.run(`msg_${Date.now()}`, idempotencyKey, "inventory.release");
  });

  transaction();

  console.log(
    `Released ${reservations.length} reservations for order ${orderId}`
  );
}

async function handleOrderCreated(event) {
  const { idempotencyKey, data } = event;

  if (isProcessed.get(idempotencyKey, "order.created")) {
    console.log(`Order ${idempotencyKey} already processed`);
    return;
  }

  console.log(`Direct order processing for ${data.orderId} (legacy mode)`);

  await handleReserveRequest({
    idempotencyKey,
    data: { orderId: data.orderId, items: data.items },
  });
}

async function processMessage(msg) {
  const event = JSON.parse(msg.content.toString());
  const routingKey = msg.fields.routingKey;

  console.log(
    `Processing ${routingKey} for ${event.idempotencyKey || "no-key"}`
  );

  try {
    switch (routingKey) {
      case "reserve.request":
        await handleReserveRequest(event);
        break;
      case "inventory.release":
        await handleInventoryRelease(event);
        break;
      case "order.created":
        await handleOrderCreated(event);
        break;
      default:
        console.log(`Unknown routing key: ${routingKey}`);
    }

    channel.ack(msg);
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

  console.log("Inventory service started");

  setInterval(() => {
    const products = db.prepare("SELECT * FROM products").all();
    console.log("=== INVENTORY STATUS ===");
    products.forEach((p) => {
      console.log(
        `${p.sku}: ${p.available_stock}/${p.totla_stock} available (${p.reserved_stock} reserved)`
      );
    });
    console.log("========================");
  }, 30000);

  process.on("SIGINT", async () => {
    db.close();
    await channel.close();
    await conn.close();
    process.exit(0);
  });
}

start().catch(console.error);
