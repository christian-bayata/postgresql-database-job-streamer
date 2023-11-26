import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { JobService } from './job.service';

@Module({
  imports: [ConfigModule],
  providers: [JobService],
})
export class JobModule {}
