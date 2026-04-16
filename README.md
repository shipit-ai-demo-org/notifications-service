# notifications-service

Multi-channel notification service for CargoCloud — email, SMS and push. Consumes order and shipment lifecycle events and notifies customers on every meaningful status change (tier-2).

## Role at CargoCloud

- Consumes shipment events from [`orders-api`](https://github.com/shipit-ai-demo-org/orders-api) on the `orders.shipment-events` Kafka topic
- Delivers push notifications to the [`mobile-app`](https://github.com/shipit-ai-demo-org/mobile-app) via APNs/FCM
- Email links point customers at tracking pages on [`web-storefront`](https://github.com/shipit-ai-demo-org/web-storefront)
- Deployed to Kubernetes via [`platform-helm-charts`](https://github.com/shipit-ai-demo-org/platform-helm-charts)

## Architecture

```
orders-api ──(Kafka: orders.shipment-events)──> notifications-service
                                                  ├── email (SES)
                                                  ├── sms   (Twilio)
                                                  └── push  (APNs / FCM)
```

- `src/consumers/orderEvents.ts` — Kafka consumer, fans events out to channels
- `src/channels/{email,sms,push}.ts` — provider-specific delivery
- `src/templates/shipment-update.ts` — customer-facing copy

## Usage

```bash
npm install
npm run dev          # local dev with tsx watch
npm run typecheck
```

| Endpoint   | Purpose            |
| ---------- | ------------------ |
| `/healthz` | Liveness probe     |
| `/readyz`  | Readiness probe    |

Configuration via env: `PORT`, `KAFKA_BROKERS`, `SES_ENDPOINT`.

## Deployment

Pushes to `main` run the `Deploy` workflow, which performs a `helm upgrade` using the chart in `platform-helm-charts`.
