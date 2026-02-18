import { Kafka, type EachMessagePayload } from 'kafkajs';
import pino from 'pino';

import { sendEmail } from '../channels/email.js';
import { sendPush } from '../channels/push.js';
import { sendSms } from '../channels/sms.js';

const logger = pino({ name: 'consumer:order-events' });

/**
 * Consumes order and shipment lifecycle events published by orders-api
 * (https://github.com/shipit-ai-demo-org/orders-api) on the
 * `orders.shipment-events` topic and fans them out to channels.
 */

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

const consumer = kafka.consumer({ groupId: 'notifications-service' });

export async function startOrderEventsConsumer(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders.shipment-events', fromBeginning: false });
  await consumer.run({ eachMessage: handleMessage });
  logger.info('order events consumer running');
}

async function handleMessage({ message }: EachMessagePayload): Promise<void> {
  if (!message.value) return;

  const event = JSON.parse(message.value.toString()) as ShipmentEventMessage;
  logger.info({ eventType: event.eventType, shipmentId: event.shipmentId }, 'received event');

  const tasks: Array<Promise<unknown>> = [];

  if (event.customer.email) {
    tasks.push(
      sendEmail({
        to: event.customer.email,
        subject: `Update on shipment ${event.trackingNumber}`,
        htmlBody: `<p>Your shipment is now: ${event.eventType}</p>`,
        textBody: `Your shipment is now: ${event.eventType}`,
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

  await Promise.allSettled(tasks);
}
