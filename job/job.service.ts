import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Cron, CronExpression } from '@nestjs/schedule';
import { Client } from 'pg';

@Injectable()
export class JobService {
  constructor(private readonly configService: ConfigService) {}

  private logger = new Logger('Job');
  /**
   * @Responsibility: Cron job that runs every day by 12am to stream database
   *
   * @returns {}
   */

  @Cron(CronExpression.EVERY_DAY_AT_MIDNIGHT)
  async databaseStreamer() {
    try {
      /* Connect to both the staging and local databases */
      const [stagingDbClient, localDbClient] = await Promise.all([
        new Client(this.databaseConfig('staging')).connect(),
        new Client(this.databaseConfig('local')).connect(),
      ]);

      /* Retrieve all table rows from the staging database */
      const allStagingDbTableNames = await stagingDbClient
        .query(
          'SELECT table_name FROM information_schema.tables WHERE table_schema = $1',
          ['public'], // This is assuming you are using the default public schema
        )
        .rows.map((row: any) => row.table_name);

      /* Loop through the staging tables and perform the migration */
      const theLength: number = allStagingDbTableNames.length;
      for (let i = 0; i < theLength; i++) {
        const tableName = allStagingDbTableNames[i];

        const theStagingQuery = `SELECT * FROM ${tableName}`;
        const theLocalQuery = `INSERT INTO ${tableName} SELECT * FROM ${tableName}`;

        /* Drop local database for fresh streaming */
        await localDbClient.query(`DELETE FROM ${tableName}`);

        /* Move data from staging to local collection in streams */
        const stream = stagingDbClient.query(theStagingQuery).stream();

        stream.on('data', async (row) => {
          await localDbClient.query(theLocalQuery);
        });

        stream.on('end', () => {
          this.logger.log(`Data migration completed for table ${tableName}`);
        });

        stream.on('error', (error) => {
          this.logger.log(`Data stream error: ${error}`);
        });

        /* After completion of data stream, move to the next collection */
        await new Promise((resolve) => stream.on('end', resolve));
      }

      await stagingDbClient.end();
      await localDbClient.end();
    } catch (error) {
      throw error;
    }
  }

  private databaseConfig(key: string) {
    return {
      local: {
        user: `${this.configService.get('LOCAL_DB_USER')}`,
        host: `${this.configService.get('LOCAL_DB_HOST')}`,
        database: `${this.configService.get('LOCAL_DB_URL')}`,
        password: `${this.configService.get('LOCAL_DB_PASSWORD')}`,
        port: `${this.configService.get('LOCAL_DB_PORT')}`,
      },
      staging: {
        user: `${this.configService.get('STAGING_DB_USER')}`,
        host: `${this.configService.get('STAGING_DB_HOST')}`,
        database: `${this.configService.get('STAGING_DB_URL')}`,
        password: `${this.configService.get('STAGING_DB_PASSWORD')}`,
        port: `${this.configService.get('STAGING_DB_PORT')}`,
      },
    }[key];
  }
}
