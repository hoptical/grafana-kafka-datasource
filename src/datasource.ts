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
import { catchError } from 'rxjs/operators';
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
    if (typeof partition === 'number') {
      const replaced = templateSrv.replace(String(partition), scopedVars);
      const parsed = Number.parseInt(replaced, 10);
      partition = Number.isFinite(parsed) ? parsed : 0;
    } else {
      const replaced = templateSrv.replace('all', scopedVars);
      if (replaced !== 'all') {
        const parsed = Number.parseInt(replaced, 10);
        partition = Number.isFinite(parsed) ? parsed : 0;
      }
    }

    return { ...query, topicName, partition };
  }

  query(request: DataQueryRequest<KafkaQuery>): Observable<DataQueryResponse> {
    const observables = request.targets
      .filter((q): q is KafkaQuery => this.filterQuery(q as KafkaQuery))
      .map((q) => {
        const interpolatedQuery = this.applyTemplateVariables(q as KafkaQuery, request.scopedVars);
        const path = `${interpolatedQuery.topicName}-${interpolatedQuery.partition}-${interpolatedQuery.autoOffsetReset}`;

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
    const response = await this.getResource('partitions', { topic: topicName });
    return response.partitions || [];
  }

  async searchTopics(prefix: string, limit = 5): Promise<string[]> {
    const response = await this.getResource('topics', { prefix, limit: String(limit) });
    return response.topics || [];
  }
}
