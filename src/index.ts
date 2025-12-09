import Fastify from 'fastify';
import pino from 'pino';

const logger = pino({ name: 'notifications-service' });

const app = Fastify({ logger: false });

app.get('/healthz', async () => ({ status: 'ok' }));
app.get('/readyz', async () => ({ status: 'ready' }));

const port = Number(process.env.PORT ?? 8084);

async function main(): Promise<void> {
  await app.listen({ port, host: '0.0.0.0' });
  logger.info({ port }, 'notifications-service listening');
}

main().catch((err) => {
  logger.error(err, 'fatal startup error');
  process.exit(1);
});
