import { defaults } from 'lodash';

import React, { ChangeEvent, PureComponent, SyntheticEvent } from 'react';
import { LegacyForms } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, MyDataSourceOptions, MyQuery } from './types';

const { FormField, Switch } = LegacyForms;

type Props = QueryEditorProps<DataSource, MyQuery, MyDataSourceOptions>;

export class QueryEditor extends PureComponent<Props> {
  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: event.target.value });
    onRunQuery();
  };

  onPartitionChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, partition: parseFloat(event.target.value) });
    onRunQuery();
  };

  onWithStreamingChange = (event: SyntheticEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, withStreaming: event.currentTarget.checked });
    onRunQuery();
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { topicName, partition, withStreaming } = query;

    return (
      <div className="gf-form">
        <FormField
          labelWidth={7}
          width={8}
          value={topicName || ''}
          onChange={this.onTopicNameChange}
          label="Topic Name"
          type="text"
        />

        <FormField
          labelWidth={5}
          width={4}
          value={partition}
          onChange={this.onPartitionChange}
          label="partition"
          type="number"
          step="1"
          min="0"
        />

        <Switch checked={withStreaming || false} label="Enable streaming (v8+)" onChange={this.onWithStreamingChange} />
      </div>
    );
  }
}
