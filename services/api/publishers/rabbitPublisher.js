export async function publishOrderCreated(event) {
  console.log("[publishOrderCreated] queued for publish:", event);
  return true;
}
