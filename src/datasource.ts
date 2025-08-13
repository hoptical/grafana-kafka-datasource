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
import { KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode } from './types';

export class DataSource extends DataSourceWithBackend<KafkaQuery, KafkaDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<KafkaDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_: CoreApp): Partial<KafkaQuery> {
    return {
      topicName: '',
      partition: 'all',
      autoOffsetReset: AutoOffsetReset.LATEST,
      timestampMode: TimestampMode.Now,
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
    return { ...query, topicName, partition };
  }

  query(request: DataQueryRequest<KafkaQuery>): Observable<DataQueryResponse> {
    const observables = request.targets
      .filter((q): q is KafkaQuery => this.filterQuery(q as KafkaQuery))
      .map((q) => {
  const interpolatedQuery = this.applyTemplateVariables(q as KafkaQuery, request.scopedVars);
  const path = `${interpolatedQuery.topicName}-${interpolatedQuery.partition}-${interpolatedQuery.autoOffsetReset}-${interpolatedQuery.lastN ?? ''}`;

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
}
