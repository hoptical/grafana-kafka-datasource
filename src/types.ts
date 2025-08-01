import { DataQuery, DataSourceJsonData } from '@grafana/data';

export enum AutoOffsetReset {
  EARLIEST = 'earliest',
  LATEST = 'latest',
}

export enum TimestampMode {
  Now = 'now',
  Message = 'message',
}

export type AutoOffsetResetInterface = {
  [key in AutoOffsetReset]: string;
};

export type TimestampModeInterface = {
  [key in TimestampMode]: string;
};

export interface KafkaDataSourceOptions extends DataSourceJsonData {
  bootstrapServers: string;
  clientId?: string;
  securityProtocol: string;
  saslMechanisms: string;
  saslUsername: string;
  logLevel: string;
  healthcheckTimeout: number;
  // TLS Configuration
  tlsAuthWithCACert?: boolean;
  tlsAuth?: boolean;
  tlsSkipVerify?: boolean;
  serverName?: string;
  // Advanced HTTP settings
  timeout?: number;
}

// Default options used when creating a new Kafka datasource
export const defaultDataSourceOptions: Partial<KafkaDataSourceOptions> = {
  bootstrapServers: '',
  clientId: '',
  securityProtocol: 'PLAINTEXT',
  saslMechanisms: '',
  saslUsername: '',
  logLevel: '',
  healthcheckTimeout: 2000,
  tlsAuthWithCACert: false,
  tlsAuth: false,
  tlsSkipVerify: false,
  serverName: '',
  timeout: 0
};

export interface KafkaSecureJsonData {
  apiKey?: string; // Deprecated
  saslPassword?: string;
  // TLS Certificates
  tlsCACert?: string;
  tlsClientCert?: string;
  tlsClientKey?: string;
}

export interface KafkaQuery extends DataQuery {
  topicName: string;
  partition: number;
  autoOffsetReset: AutoOffsetReset;
  timestampMode: TimestampMode;
}

export const defaultQuery: Partial<KafkaQuery> = {
  partition: 0,
  autoOffsetReset: AutoOffsetReset.LATEST,
  timestampMode: TimestampMode.Now,
};
