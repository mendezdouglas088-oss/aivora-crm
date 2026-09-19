import { NestFactory } from '@nestjs/core';
import { RuntimeModule } from './runtime.module';
import { Logger } from '@nestjs/common';

process.on('unhandledRejection', (reason) => {
  console.error('unhandledRejection en whatsapp-runtime:', reason);
});

async function bootstrap() {
  const logger = new Logger('whatsapp-Runtime');
  // Este proceso no sirve HTTP — no hay controllers ni frontend que lo
  // consuma directamente, solo @Processor de BullMQ. createApplicationContext
  // levanta el contenedor de DI de Nest sin Express/Fastify ni puerto.
  const app = await NestFactory.createApplicationContext(RuntimeModule);

  // Imprescindible para que los OnModuleDestroy (cerrar Chrome de cada
  // sesión, cerrar el pool de Postgres, desconectar Redis) se disparen al
  // recibir SIGTERM — así es como Docker detiene un container al
  // reiniciarlo (Fase 5, reinicio programado contra el memory leak).
  app.enableShutdownHooks();

  logger.log('whatsapp-runtime listo, esperando comandos en la cola...');
}

bootstrap();
