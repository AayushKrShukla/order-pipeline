import amqp from "amqplib";
import Database from "better-sqlite3";

const EXCHANGE = "order.events";
const QUEUE = "inventory.q";
const RK = "order.created";

const db = new Database("inventory.db");
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    stock INTEGER DEFAULT 0
  );
  
  CREATE TABLE IF NOT EXISTS reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(order_id, sku)
  );

  CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    processod_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);

const insertProduct = db.prepare(
  "INSERT OR IGNORE INTO products (sku, name, stock) values (?, ?, ?)"
);
insertProduct.run("SKU-1", "Product A", 100);
insertProduct.run("SKU-2", "Product B", 50);
console.log("Sample data seeded");

async function setupTopology(ch) {
  await ch.assertExchange(EXCHANGE, "topic", { durable: true });
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.bindQueue(QUEUE, EXCHANGE, RK);
}
async function processOrder(orderData) {
  const { orderId, items } = orderData;

  const checkProcessed = db.prepare(
    "SELECT 1 from processed_messages WHERE message_id = ?"
  );
  if (checkProcessed.get(orderId)) {
    console.log(`Order ${orderId} already processed - skipping`);
    return true;
  }

  const transaction = db.transaction(() => {
    for (const item of items) {
      const { sku, qty } = item;

      const getStock = db.prepare("SELECT stock FROM products WHERE sku = ?");
      const product = getStock.get(sku);

      if (!product) {
        throw new Error(`Product ${sku} not found`);
      }

      if (product.stock < qty) {
        throw new Error(
          `Insufficient stock for ${sku}. Available ${product.stock}, Requested: ${qty}`
        );
      }

      const reserve = db.prepare(
        "INSERT INTO reservations (order_id, sku, quantity) VALUES (?, ?, ?)"
      );
      reserve.run(orderId, sku, qty);

      const updatedStock = db.prepare(
        "UPDATE products SET stock = stock - ? WHERE sku = ?"
      );
      updatedStock.run(qty, sku);

      console.log(`Reserved ${qty} units of ${sku} for order ${orderId}`);
    }

    const markProcessed = db.prepare(
      "INSERT INTO processed_messages (message_id) VALUES (?)"
    );
    markProcessed.run(orderId);
  });

  transaction();
  return true;
}

async function start() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL);
  const ch = await conn.createChannel();

  await setupTopology(ch);
  ch.prefetch(1);

  await ch.consume(QUEUE, async (msg) => {
    if (!msg) return;
    console.log(msg.content.toString());
    
    try {
      const evt = JSON.parse(msg.content.toString());
      console.log(`Processing inventory for order: ${evt.data.orderId}`);

      await processOrder(evt.data);
      ch.ack(msg);
    } catch (e) {
      console.error(`Inventory processing error:`, e.message);
      ch.nack(msg, false, false);
    }
  });

  console.log("Inventory service started");

  process.on("SIGINT", async () => {
    db.close();
    await ch.close();
    await conn.close();
    process.exit(0);
  });
}

start().catch(console.error);
