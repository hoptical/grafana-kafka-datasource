import { DataQuery, DataSourceJsonData } from '@grafana/data';

export enum AutoOffsetReset {
  EARLIEST = 'earliest',
  LATEST = 'latest',
  LAST_N = 'lastN',
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

export enum MessageFormat {
  JSON = 'json',
  AVRO = 'avro',
}

export enum AvroSchemaSource {
  SCHEMA_REGISTRY = 'schemaRegistry',
  INLINE_SCHEMA = 'inlineSchema',
}

export type MessageFormatInterface = {
  [key in MessageFormat]: string;
};

export type AvroSchemaSourceInterface = {
  [key in AvroSchemaSource]: string;
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
  // Avro Configuration (moved to query level)
  schemaRegistryUrl?: string;
  schemaRegistryUsername?: string;
  // Advanced Json flattening settings
  flattenMaxDepth?: number;
  flattenFieldCap?: number;
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
  timeout: 0,
  flattenMaxDepth: 5,
  flattenFieldCap: 1000,
};

export interface KafkaSecureJsonData {
  apiKey?: string; // Deprecated
  saslPassword?: string;
  // TLS Certificates
  tlsCACert?: string;
  tlsClientCert?: string;
  tlsClientKey?: string;
  // Schema Registry Authentication
  schemaRegistryPassword?: string;
}

export interface KafkaQuery extends DataQuery {
  topicName: string;
  partition: number | 'all';
  autoOffsetReset: AutoOffsetReset;
  timestampMode: TimestampMode;
  lastN?: number;
  // Message Format Configuration
  messageFormat: MessageFormat;
  // Avro Configuration
  avroSchemaSource?: AvroSchemaSource;
  avroSchema?: string;
}

export const defaultQuery: Partial<KafkaQuery> = {
  partition: 'all',
  autoOffsetReset: AutoOffsetReset.LATEST,
  timestampMode: TimestampMode.Message, // Kafka Event Time is now default
  lastN: 100,
  messageFormat: MessageFormat.JSON,
  avroSchemaSource: AvroSchemaSource.SCHEMA_REGISTRY,
};
