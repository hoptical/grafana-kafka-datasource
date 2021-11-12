import { DataSourcePlugin } from '@grafana/data';
import { DataSource } from './datasource';
import { ConfigEditor } from './ConfigEditor';
import { QueryEditor } from './QueryEditor';
import { KafkaQuery, KafkaOptions } from './types';

export const plugin = new DataSourcePlugin<DataSource, KafkaQuery, KafkaOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
