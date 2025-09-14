export const EXCHANGE = "order.events";

export const ROUTING_KEYS = {
  ORDER_CREATED: "order.created",

  RESERVE_REQUEST: "reserve.request",
  RESERVE_SUCCEEDED: "reserve.succeeded",
  RESERVE_FAILED: "reserve.failed",
  INVENTORY_RELEASE: "invetory.release",

  PAYMENT_REQUEST: "payment.request",
  PAYMENT_SUCCEEDED: "payment.succeeded",
  PAYMENT_FAILED: "payment.failed",

  NOTIFY_REQUEST: "notify.request",
  SAGA_COMPLETED: "saga.completed",
  SAGA_FAILED: "saga.failed",
};

export const SAGA_STATUS = {
  STARTED: "started",
  INVENTORY_RESERVED: "inventory_reserved",
  PAYMENT_COMPLETED: "payment_completed",
  COMPLETED: "completed",
  FAILED: "failed",
  COMPENSATING: "compensating",
};
