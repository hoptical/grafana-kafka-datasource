import { DataQuery, DataSourceJsonData } from '@grafana/data';

export enum AutoOffsetReset {
  EARLIEST = 'earliest',
  LATEST = 'latest',
}
export interface KafkaQuery extends DataQuery {
  topicName: string;
  partition: number;
  withStreaming: boolean;
  autoOffsetReset: AutoOffsetReset;
}

export const defaultQuery: Partial<KafkaQuery> = {
  partition: 0,
  withStreaming: true,
  autoOffsetReset: AutoOffsetReset.LATEST,
};

export type AutoOffsetResetInterface = {
  [key in AutoOffsetReset]: string;
};

export interface KafkaDataSourceOptions extends DataSourceJsonData {
  bootstrapServers: string;
}

export interface KafkaSecureJsonData {
  apiKey?: string;
}
