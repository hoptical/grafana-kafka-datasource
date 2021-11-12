import { DataSourceInstanceSettings } from '@grafana/data';
import { DataSourceWithBackend } from '@grafana/runtime';
import { KafkaOptions, KafkaQuery } from './types';

export class DataSource extends DataSourceWithBackend<KafkaQuery, KafkaOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<KafkaOptions>) {
    super(instanceSettings);
  }
}
