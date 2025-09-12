import express from "express";
import dotenv from "dotenv";
import {
  initRabbit,
  publishOrderCreated,
} from "./publishers/rabbitPublisher.js";

dotenv.config();

const RABBIT_URL = process.env.RABBITMQ_URL;
await initRabbit(RABBIT_URL);

const app = express();
app.use(express.json());

const port = process.env.PORT || 3000;

app.get("/health", (req, res) => {
  res.status(200).json({ status: "ok" });
});

app.post("/orders", async (req, res) => {
  const { customerId, items, note } = req.body || {};
  if (!customerId || !Array.isArray(items)) {
    return res
      .status(400)
      .json({ error: "customerId and items[] are required" });
  }

  const orderId = `ord_${Date.now()}`;
  const event = {
    type: "order_created",
    id: `evt_${Date.now()}`,
    occuredAt: new Date().toISOString(),
    data: { orderId, customerId, items, note: note || null },
  };

  await publishOrderCreated(event);

  res
    .status(202)
    .set("x-idempotency-key", orderId)
    .json({ accepted: true, orderId });
});

app.listen(port, () => {
  console.log(`API listening on http://localhost:${port}`);
});
