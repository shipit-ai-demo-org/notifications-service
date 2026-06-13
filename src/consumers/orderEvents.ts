import { Kafka, type EachMessagePayload, type Producer } from 'kafkajs';
import pino from 'pino';

import { sendEmail, type ChannelResult } from '../channels/email.js';
import { sendPush } from '../channels/push.js';
import { sendSms } from '../channels/sms.js';
import { renderShipmentUpdate } from '../templates/shipment-update.js';

const logger = pino({ name: 'consumer:order-events' });

/**
 * Consumes order and shipment lifecycle events published by orders-api
 * (https://github.com/shipit-ai-demo-org/orders-api) on the
 * `orders.shipment-events` topic and fans them out to channels.
 *
 * Events that cannot be processed (malformed payloads, or every targeted
 * channel failing) are parked on a dead-letter topic instead of being
 * silently dropped, so they can be inspected and replayed by ops tooling.
 */

const SOURCE_TOPIC = 'orders.shipment-events';
const DLQ_TOPIC = process.env.ORDER_EVENTS_DLQ_TOPIC ?? 'orders.shipment-events.dlq';

export interface ShipmentEventMessage {
  eventType: 'shipment.created' | 'shipment.in_transit' | 'shipment.out_for_delivery' | 'shipment.delivered';
  shipmentId: string;
  trackingNumber: string;
  customer: {
    email?: string;
    phone?: string;
    deviceToken?: string;
    platform?: 'ios' | 'android';
  };
}

const kafka = new Kafka({
  clientId: 'notifications-service',
  brokers: (process.env.KAFKA_BROKERS ?? 'kafka-0.internal:9092').split(','),
});

const consumer = kafka.consumer({
  groupId: 'notifications-service',
  retry: { initialRetryTime: 300, retries: 8, multiplier: 2, maxRetryTime: 30_000 },
});

let dlqProducer: Producer | undefined;

async function getDlqProducer(): Promise<Producer> {
  if (!dlqProducer) {
    dlqProducer = kafka.producer();
    await dlqProducer.connect();
  }
  return dlqProducer;
}

async function sendToDlq(payload: EachMessagePayload, reason: string): Promise<void> {
  try {
    const producer = await getDlqProducer();
    await producer.send({
      topic: DLQ_TOPIC,
      messages: [
        {
          key: payload.message.key,
          value: payload.message.value,
          headers: {
            ...payload.message.headers,
            'x-dlq-reason': reason,
            'x-dlq-source-topic': SOURCE_TOPIC,
            'x-dlq-source-partition': String(payload.partition),
            'x-dlq-source-offset': payload.message.offset,
            'x-dlq-failed-at': new Date().toISOString(),
          },
        },
      ],
    });
    logger.warn({ reason, offset: payload.message.offset, dlqTopic: DLQ_TOPIC }, 'event parked on DLQ');
  } catch (err) {
    // Never let DLQ publishing take the consumer down; the original error
    // is already logged and the message will surface in lag metrics.
    logger.error({ err, reason }, 'failed to publish event to DLQ');
  }
}

export async function startOrderEventsConsumer(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: SOURCE_TOPIC, fromBeginning: false });
  await consumer.run({ eachMessage: handleMessage });
  logger.info('order events consumer running');
}

async function handleMessage(payload: EachMessagePayload): Promise<void> {
  const { message } = payload;
  if (!message.value) return;

  let event: ShipmentEventMessage;
  try {
    event = JSON.parse(message.value.toString()) as ShipmentEventMessage;
  } catch (err) {
    logger.error({ err }, 'malformed event payload, parking on DLQ');
    await sendToDlq(payload, 'malformed_payload');
    return;
  }
  logger.info({ eventType: event.eventType, shipmentId: event.shipmentId }, 'received event');

  const tasks: Array<Promise<ChannelResult>> = [];

  if (event.customer.email) {
    const rendered = renderShipmentUpdate(event);
    tasks.push(
      sendEmail({
        to: event.customer.email,
        subject: rendered.subject,
        htmlBody: rendered.htmlBody,
        textBody: rendered.textBody,
      }),
    );
  }

  if (event.customer.phone && event.eventType === 'shipment.out_for_delivery') {
    tasks.push(
      sendSms({
        to: event.customer.phone,
        body: `CargoCloud: parcel ${event.trackingNumber} is out for delivery today.`,
      }),
    );
  }

  if (event.customer.deviceToken && event.customer.platform) {
    tasks.push(
      sendPush({
        deviceToken: event.customer.deviceToken,
        platform: event.customer.platform,
        title: 'Shipment update',
        body: `${event.trackingNumber}: ${event.eventType.replace('shipment.', '').replace('_', ' ')}`,
        data: { shipmentId: event.shipmentId },
      }),
    );
  }

  if (tasks.length === 0) return;

  const results = await Promise.allSettled(tasks);
  const delivered = results.some(
    (result) => result.status === 'fulfilled' && result.value.accepted,
  );

  // Partial success is fine (e.g. push landed but SMS did not); only park
  // the event when no channel got the update to the customer at all.
  if (!delivered) {
    logger.error(
      { eventType: event.eventType, shipmentId: event.shipmentId, channels: results.length },
      'all channels failed, parking event on DLQ',
    );
    await sendToDlq(payload, 'all_channels_failed');
  }
}
