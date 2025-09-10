import {
  CoreApp,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceInstanceSettings,
  LiveChannelScope,
  ScopedVars,
} from '@grafana/data';
import { DataSourceWithBackend, getGrafanaLiveSrv, getTemplateSrv } from '@grafana/runtime';
import { Observable, merge, throwError, of } from 'rxjs';
import { catchError, startWith } from 'rxjs/operators';
import { KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, defaultQuery } from './types';

export class DataSource extends DataSourceWithBackend<KafkaQuery, KafkaDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<KafkaDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<KafkaQuery> {
    return {
      topicName: '',
      ...defaultQuery,
    };
  }

  filterQuery(query: KafkaQuery): boolean {
    if (!query?.topicName) {
      return false;
    }
    return query.partition === 'all' || (typeof query.partition === 'number' && query.partition >= 0);
  }

  applyTemplateVariables(query: KafkaQuery, scopedVars: ScopedVars) {
    const templateSrv = getTemplateSrv();
    const topicName = templateSrv.replace(query.topicName, scopedVars);
    let partition: number | 'all' = query.partition;
    // Only apply template replacement if partition is a string that looks like a template variable
    if (typeof partition === 'string' && partition !== 'all') {
      const replaced = templateSrv.replace(partition, scopedVars);
      if (replaced === 'all') {
        partition = 'all';
      } else {
        const parsed = Number.parseInt(replaced, 10);
        partition = Number.isFinite(parsed) && parsed >= 0 ? parsed : 'all';
      }
    } else if (typeof partition === 'number') {
      // Apply template replacement to numeric partitions (might be from saved queries)
      const replaced = templateSrv.replace(String(partition), scopedVars);
      const parsed = Number.parseInt(replaced, 10);
      partition = Number.isFinite(parsed) && parsed >= 0 ? parsed : partition;
    }
    // Sanitize lastN only when mode is LAST_N; otherwise unset it
    let lastN: number | undefined = undefined;
    if (query.autoOffsetReset === AutoOffsetReset.LAST_N) {
      const replacedLastN = templateSrv.replace(String(query.lastN ?? ''), scopedVars);
      const parsed = Number.parseInt(replacedLastN, 10);
      const n = Number.isFinite(parsed) && parsed > 0 ? parsed : 100;
      lastN = n;
    }
    return { ...query, topicName, partition, lastN };
  }

  query(request: DataQueryRequest<KafkaQuery>): Observable<DataQueryResponse> {
    const observables = request.targets
      .filter((q): q is KafkaQuery => this.filterQuery(q as KafkaQuery))
      .map((q) => {
        const interpolatedQuery = this.applyTemplateVariables(q as KafkaQuery, request.scopedVars);
        // Build path from encoded segments without dangling dashes
        // Include all configuration parameters that should trigger stream restart
        const segments: string[] = [];
        segments.push(encodeURIComponent(String(interpolatedQuery.topicName)));
        segments.push(encodeURIComponent(String(interpolatedQuery.partition)));
        segments.push(encodeURIComponent(String(interpolatedQuery.autoOffsetReset)));
        segments.push(encodeURIComponent(String(interpolatedQuery.messageFormat || 'json')));
        segments.push(encodeURIComponent(String(interpolatedQuery.avroSchemaSource || 'schemaRegistry')));
        // Include a hash of the Avro schema to detect changes
        const schemaHash = interpolatedQuery.avroSchema ? 
          String(interpolatedQuery.avroSchema.length) + '_' + interpolatedQuery.avroSchema.slice(0, 10).replace(/[^a-zA-Z0-9]/g, '') : 'none';
        segments.push(encodeURIComponent(schemaHash));
        if (
          interpolatedQuery.autoOffsetReset === AutoOffsetReset.LAST_N &&
          typeof interpolatedQuery.lastN !== 'undefined'
        ) {
          segments.push(encodeURIComponent(String(interpolatedQuery.lastN)));
        }
        const path = segments.join('-');

        return getGrafanaLiveSrv()
          .getDataStream({
            addr: {
              scope: LiveChannelScope.DataSource,
              namespace: this.uid,
              path,
              data: interpolatedQuery,
            },
          })
          .pipe(
            startWith({ data: [] }),
            catchError((err) => {
              console.error('Stream error:', err);
              return throwError(() => ({
                message: `Error connecting to Kafka topic ${interpolatedQuery.topicName}: ${err.message}`,
                status: 'error',
              }));
            })
          );
      });

    return observables.length ? merge(...observables) : of({ data: [] });
  }

  async getTopicPartitions(topicName: string): Promise<number[]> {
    try {
      const response = await this.getResource('partitions', { topic: topicName });
      return response.partitions || [];
    } catch (err: any) {
      // Re-throw to let Grafana surface the error toast, preserving 404 messages
      throw err;
    }
  }

  async searchTopics(prefix: string, limit = 5): Promise<string[]> {
    const response = await this.getResource('topics', { prefix, limit: String(limit) });
    return response.topics || [];
  }

  async validateSchemaRegistry(): Promise<{ status: string; message: string }> {
    try {
      const response = await this.getResource('validate-schema-registry') as any;
      return {
        status: response.status || 'ok',
        message: response.message || 'Schema registry is accessible',
      };
    } catch (err: any) {
      return {
        status: 'error',
        message: err?.message || 'Failed to validate schema registry',
      };
    }
  }

  async validateAvroSchema(schema: string): Promise<{ status: string; message: string }> {
    try {
      const response = await this.postResource('validate-avro-schema', { schema }) as any;
      return {
        status: response.status || 'ok',
        message: response.message || 'Schema is valid',
      };
    } catch (err: any) {
      return {
        status: 'error',
        message: err?.message || 'Failed to validate schema',
      };
    }
  }
}
