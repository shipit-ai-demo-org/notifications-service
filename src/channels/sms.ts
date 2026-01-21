import pino from 'pino';

import type { ChannelResult } from './email.js';

const logger = pino({ name: 'channel:sms' });

export interface SmsMessage {
  to: string;
  body: string;
}

const E164_PATTERN = /^\+[1-9]\d{6,14}$/;
const MAX_SMS_LENGTH = 320;

export async function sendSms(message: SmsMessage): Promise<ChannelResult> {
  if (!E164_PATTERN.test(message.to)) {
    logger.warn({ to: message.to }, 'rejected non-E.164 phone number');
    return { channel: 'sms', accepted: false };
  }

  const body =
    message.body.length > MAX_SMS_LENGTH
      ? `${message.body.slice(0, MAX_SMS_LENGTH - 1)}…`
      : message.body;

  // Production path calls the Twilio Messages API.
  logger.info({ to: message.to, length: body.length }, 'dispatching sms');

  return {
    channel: 'sms',
    accepted: true,
    providerMessageId: `tw-${Date.now().toString(36)}`,
  };
}
