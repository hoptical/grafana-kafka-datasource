import { defaults } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { InlineField, InlineFieldRow, Input, Select } from '@grafana/ui';
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

const partitionOptions: Array<{ label: string; value: number | 'all' }> = [
  {
    label: 'All partitions',
    value: 'all',
  },
];

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

interface State {
  availablePartitions: number[];
}

export class QueryEditor extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      availablePartitions: [],
    };
  }

  componentDidMount() {
    this.fetchPartitionsIfNeeded();
  }

  componentDidUpdate(prevProps: Props) {
    const currentTopic = this.props.query.topicName;
    const prevTopic = prevProps.query.topicName;
    
    if (currentTopic && currentTopic !== prevTopic) {
      this.fetchPartitionsIfNeeded();
    }
  }

  fetchPartitionsIfNeeded = async () => {
    const { datasource, query } = this.props;
    if (!query.topicName) {
      this.setState({ availablePartitions: [] });
      return;
    }

    try {
      const partitions = await datasource.getTopicPartitions(query.topicName);
      this.setState({ availablePartitions: partitions });
    } catch (error) {
      console.error('Failed to fetch partitions:', error);
      this.setState({ availablePartitions: [] });
    }
  };

  getPartitionOptions = (): Array<{ label: string; value: number | 'all' }> => {
    const options = [...partitionOptions]; // Start with "All partitions"
    
    // Add specific partitions based on available partitions
    this.state.availablePartitions.forEach((partition) => {
      options.push({
        label: `Partition ${partition}`,
        value: partition,
      });
    });

    return options;
  };
  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: event.target.value });
    onRunQuery();
  };

  onPartitionChange = (value: number | 'all') => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, partition: value });
    onRunQuery();
  };

  onAutoResetOffsetChanged = (value: AutoOffsetReset) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, autoOffsetReset: value });
    onRunQuery();
  };

  onTimestampModeChanged = (value: TimestampMode) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, timestampMode: value });
    onRunQuery();
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
          <InlineField label="Partition" labelWidth={10} tooltip="Kafka partition selection">
            <Select
              id="query-editor-partition"
              value={partition}
              options={this.getPartitionOptions()}
              onChange={(value) => this.onPartitionChange(value.value!)}
              width={15}
              placeholder="Select partition"
            />
          </InlineField>
        </InlineFieldRow>
        <InlineFieldRow>
          <InlineField label="Auto offset reset" labelWidth={20} tooltip="Starting offset to consume that can be from latest or last 100.">
            <Select
              width={22}
              value={autoOffsetReset}
              options={autoResetOffsets}
              onChange={(value) => this.onAutoResetOffsetChanged(value.value!)}
            />
          </InlineField>
          <InlineField label="Timestamp Mode" labelWidth={20} tooltip="Timestamp of the kafka value to visualize.">
            <Select
              width={25}
              value={timestampMode}
              options={timestampModes}
              onChange={(value) => this.onTimestampModeChanged(value.value!)}
            />
          </InlineField>
        </InlineFieldRow>
      </>
    );
  }
}
