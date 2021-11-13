import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface KafkaQuery extends DataQuery {
  topicName: string;
  partition: number;
  withStreaming: boolean;
}

export const defaultQuery: Partial<KafkaQuery> = {
  partition: 0,
  withStreaming: false,
};

export interface KafkaDataSourceOptions extends DataSourceJsonData {
  bootstrapServers: string;
}

export interface KafkaSecureJsonData {
  apiKey?: string;
}
