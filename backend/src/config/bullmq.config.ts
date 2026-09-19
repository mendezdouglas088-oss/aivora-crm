import { BullRootModuleOptions } from '@nestjs/bullmq';
import { RedisOptions } from 'ioredis';

export const redisOptions: RedisOptions = {
  host: process.env.REDIS_HOST || 'localhost',
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD,
};

export const bullConfig: BullRootModuleOptions = {
  connection: redisOptions,
};
