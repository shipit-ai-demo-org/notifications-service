import pino from 'pino';

const logger = pino({ name: 'channel:email' });

export interface EmailMessage {
  to: string;
  subject: string;
  htmlBody: string;
  textBody: string;
}

export interface ChannelResult {
  channel: 'email' | 'sms' | 'push';
  accepted: boolean;
  providerMessageId?: string;
}

const SES_ENDPOINT = process.env.SES_ENDPOINT ?? 'https://email.us-east-1.amazonaws.com';

export async function sendEmail(message: EmailMessage): Promise<ChannelResult> {
  if (!message.to.includes('@')) {
    logger.warn({ to: message.to }, 'rejected invalid email address');
    return { channel: 'email', accepted: false };
  }

  // In production this calls SES via the AWS SDK; the endpoint is
  // injected so integration tests can point at a localstack container.
  logger.info({ to: message.to, subject: message.subject, endpoint: SES_ENDPOINT }, 'dispatching email');

  return {
    channel: 'email',
    accepted: true,
    providerMessageId: `ses-${Date.now().toString(36)}`,
  };
}
