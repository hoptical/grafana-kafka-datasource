import { defaults } from 'lodash';
import React, { ChangeEvent, PureComponent, SyntheticEvent } from 'react';
import { LegacyForms, InlineFormLabel } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode } from './types';

const { FormField, Switch, Select } = LegacyForms;

const autoResetOffsets = [
  {
    label: 'From the last 100',
    value: AutoOffsetReset.EARLIEST,
    description: 'Consume from the last 100 offset',
  },
  {
    label: 'Latest',
    value: AutoOffsetReset.LATEST,
    description: 'Consume from the latest offset',
  },
] as Array<SelectableValue<AutoOffsetReset>>;

const timestampModes = [
  {
    label: 'Now',
    value: TimestampMode.Now,
    description: 'Current time while consuming the message',
  },
  {
    label: 'Message Timestamp',
    value: TimestampMode.Message,
    description: 'The message timestamp while producing into topic',
  },
] as Array<SelectableValue<TimestampMode>>;

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

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

  onAutoResetOffsetChanged = (selected: SelectableValue<AutoOffsetReset>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, autoOffsetReset: selected.value || AutoOffsetReset.LATEST });
    onRunQuery();
  };

  resolveAutoResetOffset = (value: string | undefined) => {
    if (value === AutoOffsetReset.LATEST) {
      return autoResetOffsets[1];
    }
    return autoResetOffsets[0];
  };

  onTimestampModeChanged = (selected: SelectableValue<TimestampMode>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, timestampMode: selected.value || TimestampMode.Now });
    onRunQuery();
  };

  resolveTimestampMode = (value: string | undefined) => {
    if (value === TimestampMode.Now) {
      return timestampModes[0];
    }
    return timestampModes[1];
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { topicName, partition, withStreaming, autoOffsetReset, timestampMode } = query;

    return (
      <div className="gf-form">
        <FormField
          labelWidth={7}
          width={8}
          value={topicName || ''}
          onChange={this.onTopicNameChange}
          label="Topic"
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

        <div className="gf-form">
          <InlineFormLabel
            className="width-5"
            tooltip="Starting offset to consume that can be from latest or last 100."
          >
            Auto offset reset
          </InlineFormLabel>
          <Select
            className="width-10"
            value={this.resolveAutoResetOffset(autoOffsetReset)}
            options={autoResetOffsets}
            defaultValue={autoResetOffsets[0]}
            onChange={this.onAutoResetOffsetChanged}
          />
        </div>
        <div className="gf-form">
          <InlineFormLabel className="width-5" tooltip="Timestamp of the kafka value to visualize.">
            Timestamp Mode
          </InlineFormLabel>
          <Select
            className="width-10"
            value={this.resolveTimestampMode(timestampMode)}
            options={timestampModes}
            defaultValue={timestampModes[0]}
            onChange={this.onTimestampModeChanged}
          />
        </div>
        <Switch checked={withStreaming || false} label="Enable streaming (v8+)" onChange={this.onWithStreamingChange} />
      </div>
    );
  }
}
