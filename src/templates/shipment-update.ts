import type { ShipmentEventMessage } from '../consumers/orderEvents.js';

interface RenderedTemplate {
  subject: string;
  htmlBody: string;
  textBody: string;
}

const HEADLINES: Record<ShipmentEventMessage['eventType'], string> = {
  'shipment.created': 'Your shipment is booked',
  'shipment.in_transit': 'Your parcel is on the move',
  'shipment.out_for_delivery': 'Out for delivery today',
  'shipment.delivered': 'Delivered!',
};

export function renderShipmentUpdate(event: ShipmentEventMessage): RenderedTemplate {
  const headline = HEADLINES[event.eventType];
  const trackingUrl = `https://www.cargocloud.dev/tracking/${event.trackingNumber}`;

  const textBody = [
    headline,
    '',
    `Tracking number: ${event.trackingNumber}`,
    `Follow your parcel: ${trackingUrl}`,
    '',
    '— The CargoCloud team',
  ].join('\n');

  const htmlBody = `
    <div style="font-family: Inter, sans-serif; color: #0b2545;">
      <h2>${headline}</h2>
      <p>Tracking number: <strong>${event.trackingNumber}</strong></p>
      <p><a href="${trackingUrl}">Follow your parcel</a></p>
      <p style="color: #5c6b7a;">— The CargoCloud team</p>
    </div>
  `;

  return { subject: `${headline} — ${event.trackingNumber}`, htmlBody, textBody };
}
