import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface KafkaQuery extends DataQuery {
  topicName: string;
  partition: number;
  withStreaming?: boolean;
};

export const defaultQuery: Partial<KafkaQuery> = {
  partition: 0,
  withStreaming: true,
};

export enum AutoOffsetReset {
  EARLIEST = 'earliest',
  LATEST = 'latest',
  NONE = 'none',
};

export type AutoOffsetResetInterface = {
  [key in AutoOffsetReset]: string;
};

export interface KafkaOptions extends DataSourceJsonData {
  bootstrapServers: string;
  autoOffsetReset: AutoOffsetReset;
};

export interface KafkaSecureJsonData {
  apiKey?: string;
};
