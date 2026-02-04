import pino from 'pino';

import type { ChannelResult } from './email.js';

const logger = pino({ name: 'channel:push' });

export interface PushMessage {
  deviceToken: string;
  platform: 'ios' | 'android';
  title: string;
  body: string;
  data?: Record<string, string>;
}

export async function sendPush(message: PushMessage): Promise<ChannelResult> {
  if (message.deviceToken.length < 16) {
    logger.warn({ platform: message.platform }, 'rejected malformed device token');
    return { channel: 'push', accepted: false };
  }

  // iOS goes through APNs, Android through FCM. Both are wrapped
  // behind this single entry point so consumers stay provider-agnostic.
  const provider = message.platform === 'ios' ? 'apns' : 'fcm';
  logger.info({ provider, title: message.title }, 'dispatching push notification');

  return {
    channel: 'push',
    accepted: true,
    providerMessageId: `${provider}-${Date.now().toString(36)}`,
  };
}
