import { defaults } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { InlineField, InlineFieldRow, Input, Combobox, ComboboxOption } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode } from './types';

const autoResetOffsets: Array<{ label: string; value: AutoOffsetReset }> = [
  {
    label: 'From the last 100',
    value: AutoOffsetReset.EARLIEST,
  },
  {
    label: 'Latest',
    value: AutoOffsetReset.LATEST,
  },
];

const timestampModes: Array<{ label: string; value: TimestampMode }> = [
  {
    label: 'Now',
    value: TimestampMode.Now,
  },
  {
    label: 'Message Timestamp',
    value: TimestampMode.Message,
  },
];

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

export class QueryEditor extends PureComponent<Props> {
  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: event.target.value });
    onRunQuery();
  };

  onPartitionChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = parseInt(event.target.value, 10);
    // Ensure partition is a valid non-negative integer
    const partition = isNaN(value) || value < 0 ? 0 : value;
    onChange({ ...query, partition });
    onRunQuery();
  };

  onAutoResetOffsetChanged = (option: ComboboxOption<AutoOffsetReset> | null) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = option?.value || AutoOffsetReset.LATEST;
    onChange({ ...query, autoOffsetReset: value });
    onRunQuery();
  };

  resolveAutoResetOffset = (value: string | undefined): ComboboxOption<AutoOffsetReset> => {
    const found = autoResetOffsets.find(option => option.value === value);
    return found ? { label: found.label, value: found.value } : { label: autoResetOffsets[1].label, value: autoResetOffsets[1].value };
  };

  onTimestampModeChanged = (option: ComboboxOption<TimestampMode> | null) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = option?.value || TimestampMode.Now;
    onChange({ ...query, timestampMode: value });
    onRunQuery();
  };

  resolveTimestampMode = (value: string | undefined): ComboboxOption<TimestampMode> => {
    const found = timestampModes.find(option => option.value === value);
    return found ? { label: found.label, value: found.value } : { label: timestampModes[0].label, value: timestampModes[0].value };
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { topicName, partition, autoOffsetReset, timestampMode } = query;

    return (
      <>
        <InlineFieldRow>
          <InlineField label="Topic" labelWidth={10} tooltip="Kafka topic name">
            <Input
              id="query-editor-topic"
              value={topicName || ''}
              onChange={this.onTopicNameChange}
              type="text"
              width={20}
              placeholder="Enter topic name"
            />
          </InlineField>
          <InlineField label="Partition" labelWidth={10} tooltip="Kafka partition number">
            <Input
              id="query-editor-partition"
              value={partition}
              onChange={this.onPartitionChange}
              type="number"
              width={10}
              min={0}
              step={1}
              placeholder="0"
            />
          </InlineField>
        </InlineFieldRow>
        <InlineFieldRow>
          <InlineField label="Auto offset reset" labelWidth={20} tooltip="Starting offset to consume that can be from latest or last 100.">
            <Combobox
              width={22}
              value={this.resolveAutoResetOffset(autoOffsetReset)}
              options={autoResetOffsets}
              onChange={this.onAutoResetOffsetChanged}
            />
          </InlineField>
          <InlineField label="Timestamp Mode" labelWidth={20} tooltip="Timestamp of the kafka value to visualize.">
            <Combobox
              width={25}
              value={this.resolveTimestampMode(timestampMode)}
              options={timestampModes}
              onChange={this.onTimestampModeChanged}
            />
          </InlineField>
        </InlineFieldRow>
      </>
    );
  }
}
