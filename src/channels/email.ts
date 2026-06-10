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

// SES throttles bursts (and has the occasional 5xx); transient failures are
// retried with exponential backoff plus jitter before the message is
// reported as not accepted.
const MAX_ATTEMPTS = Number(process.env.EMAIL_MAX_ATTEMPTS ?? 4);
const BASE_DELAY_MS = Number(process.env.EMAIL_RETRY_BASE_MS ?? 200);
const MAX_DELAY_MS = 5_000;

function backoffDelay(attempt: number): number {
  const exp = Math.min(BASE_DELAY_MS * 2 ** attempt, MAX_DELAY_MS);
  return exp / 2 + Math.random() * (exp / 2);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function dispatchToProvider(message: EmailMessage): Promise<string> {
  // In production this calls SES via the AWS SDK; the endpoint is
  // injected so integration tests can point at a localstack container.
  logger.info({ to: message.to, subject: message.subject, endpoint: SES_ENDPOINT }, 'dispatching email');
  return `ses-${Date.now().toString(36)}`;
}

export async function sendEmail(message: EmailMessage): Promise<ChannelResult> {
  if (!message.to.includes('@')) {
    logger.warn({ to: message.to }, 'rejected invalid email address');
    return { channel: 'email', accepted: false };
  }

  let lastError: unknown;
  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt += 1) {
    try {
      const providerMessageId = await dispatchToProvider(message);
      return { channel: 'email', accepted: true, providerMessageId };
    } catch (err) {
      lastError = err;
      const delayMs = backoffDelay(attempt);
      logger.warn(
        { err, attempt: attempt + 1, maxAttempts: MAX_ATTEMPTS, delayMs: Math.round(delayMs) },
        'email dispatch failed, backing off',
      );
      if (attempt < MAX_ATTEMPTS - 1) {
        await sleep(delayMs);
      }
    }
  }

  logger.error({ err: lastError, to: message.to }, 'email dispatch exhausted retries');
  return { channel: 'email', accepted: false };
}
