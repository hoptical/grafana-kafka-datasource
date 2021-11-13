import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { KafkaQuery, KafkaDataSourceOptions } from './types';

export const plugin = new DataSourcePlugin<DataSource, KafkaQuery, KafkaDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
