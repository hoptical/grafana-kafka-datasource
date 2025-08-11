import { defaults, debounce } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { InlineField, InlineFieldRow, Input, Select, Button, Spinner } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode } from './types';

const autoResetOffsets: Array<{ label: string; value: AutoOffsetReset }> = [
  { label: 'From the last 100', value: AutoOffsetReset.EARLIEST },
  { label: 'Latest', value: AutoOffsetReset.LATEST },
];

const timestampModes: Array<{ label: string; value: TimestampMode }> = [
  { label: 'Now', value: TimestampMode.Now },
  { label: 'Message Timestamp', value: TimestampMode.Message },
];

const partitionOptions: Array<{ label: string; value: number | 'all' }> = [
  { label: 'All partitions', value: 'all' },
];

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

interface State {
  availablePartitions: number[];
  topicSuggestions: string[];
  showingSuggestions: boolean;
  loadingPartitions: boolean;
}

export class QueryEditor extends PureComponent<Props, State> {
  private debouncedSearchTopics: (input: string) => void;
  private lastCommittedTopic = '';

  constructor(props: Props) {
    super(props);
    this.state = {
      availablePartitions: [],
      topicSuggestions: [],
      showingSuggestions: false,
  loadingPartitions: false,
    };

  this.debouncedSearchTopics = debounce(this.searchTopics, 300);
  }

  componentDidMount() {
    // If a saved panel already has a topic, allow manual fetch via button; do not auto fetch.
  }

  componentDidUpdate() {
    // No automatic partition fetching to avoid noisy errors while typing.
  }

  componentWillUnmount() {
    // Cancel debounced topic search to avoid setState after unmount
    if ((this.debouncedSearchTopics as any)?.cancel) {
      (this.debouncedSearchTopics as any).cancel();
    }
  }

  fetchPartitions = async () => {
    const { datasource, query } = this.props;
    if (!query.topicName) {
      this.setState({ availablePartitions: [] });
      return;
    }
    this.setState({ loadingPartitions: true });
    try {
      const partitions = await datasource.getTopicPartitions(query.topicName);
      this.setState({ availablePartitions: partitions || [], loadingPartitions: false });
    } catch (error) {
      console.error('Failed to fetch partitions:', error);
      this.setState({ availablePartitions: [], loadingPartitions: false });
    }
  };

  getPartitionOptions = (): Array<{ label: string; value: number | 'all' }> => {
    const options = [...partitionOptions];
    (this.state.availablePartitions || []).forEach((partition) => {
      options.push({ label: `Partition ${partition}`, value: partition });
    });
    return options;
  };

  searchTopics = async (input: string) => {
    const { datasource } = this.props;
    const trimmed = input.trim();
    if (trimmed.length < 2) {
      this.setState({ topicSuggestions: [], showingSuggestions: false });
      return;
    }

    try {
      const topics = await datasource.searchTopics(trimmed, 10);
      const filteredTopics = (topics || [])
        .filter((topic) => topic.toLowerCase().startsWith(trimmed.toLowerCase()))
        .filter((topic) => {
          const hasStructure = /[-_.]/.test(topic);
          const isSignificantlyLonger = topic.length >= trimmed.length + 3;
          const isExactMatch = topic.toLowerCase() === trimmed.toLowerCase();
          return isExactMatch || isSignificantlyLonger || hasStructure;
        })
        .slice(0, 5);

      this.setState({ topicSuggestions: filteredTopics, showingSuggestions: filteredTopics.length > 0 });
    } catch (error) {
      console.error('Failed to search topics:', error);
      this.setState({ topicSuggestions: [], showingSuggestions: false });
    }
  };

  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
  const { onChange, query } = this.props;
    const value = event.target.value;
    onChange({ ...query, topicName: value });
    // Do NOT run query yet; wait for Enter or blur to reduce noisy errors
    this.debouncedSearchTopics(value);
  };

  commitTopicIfChanged = () => {
    const { topicName } = defaults(this.props.query, defaultQuery);
    if (topicName && topicName !== this.lastCommittedTopic) {
      this.lastCommittedTopic = topicName;
      this.props.onRunQuery();
    }
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

  onTopicSuggestionClick = (topic: string) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: topic });
    this.setState({ showingSuggestions: false, topicSuggestions: [] });
  this.lastCommittedTopic = topic;
  onRunQuery();
  };

  onTopicInputBlur = () => {
    // Commit the topic after suggestions close (slight delay so click works)
    setTimeout(() => {
      this.setState({ showingSuggestions: false });
      this.commitTopicIfChanged();
    }, 150);
  };

  onTopicInputFocus = () => {
    if (this.state.topicSuggestions.length > 0) {
      this.setState({ showingSuggestions: true });
    }
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { topicName, partition, autoOffsetReset, timestampMode } = query;

    return (
      <>
        <InlineFieldRow>
          <InlineField label="Topic" labelWidth={10} tooltip="Kafka topic name + click Fetch to load partitions">
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', position: 'relative' }}>
              <div style={{ position: 'relative' }}>
                <Input
                  id="query-editor-topic"
                  value={topicName || ''}
                  onChange={this.onTopicNameChange}
                  onBlur={this.onTopicInputBlur}
                  onFocus={this.onTopicInputFocus}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      this.commitTopicIfChanged();
                      this.fetchPartitions();
                    }
                  }}
                  type="text"
                  width={20}
                  placeholder="Enter topic name"
                />
                {this.state.showingSuggestions && this.state.topicSuggestions.length > 0 && (
                  <div
                    style={{
                      position: 'absolute',
                      top: '100%',
                      left: 0,
                      right: 0,
                      backgroundColor: 'var(--gf-color-background-primary, #1f1f20)',
                      border: '1px solid var(--gf-color-border-medium, #444)',
                      borderRadius: '2px',
                      boxShadow: '0 2px 8px rgba(0,0,0,0.2)',
                      zIndex: 1000,
                      maxHeight: '200px',
                      overflowY: 'auto',
                    }}
                  >
                    {this.state.topicSuggestions.map((topic, index) => (
                      <div
                        key={`${topic}-${index}`}
                        style={{
                          padding: '8px 12px',
                          cursor: 'pointer',
                          borderBottom:
                            index < this.state.topicSuggestions.length - 1
                              ? '1px solid var(--gf-color-border-weak, #333)'
                              : 'none',
                          fontSize: '13px',
                          color: 'var(--gf-color-text-primary, #ffffff)',
                          backgroundColor: 'transparent',
                        }}
                        onClick={() => this.onTopicSuggestionClick(topic)}
                        onMouseEnter={(e) => {
                          e.currentTarget.style.backgroundColor = 'var(--gf-color-background-secondary, #262628)';
                        }}
                        onMouseLeave={(e) => {
                          e.currentTarget.style.backgroundColor = 'transparent';
                        }}
                      >
                        {topic}
                      </div>
                    ))}
                  </div>
                )}
              </div>
              <Button
                size="sm"
                variant="secondary"
                onClick={() => {
                  this.commitTopicIfChanged();
                  this.fetchPartitions();
                }}
                disabled={!topicName || this.state.loadingPartitions}
              >
                {this.state.loadingPartitions ? <Spinner size={14} /> : 'Fetch'}
              </Button>
            </div>
          </InlineField>
          <InlineField label="Partition" labelWidth={10} tooltip="Kafka partition selection">
            <Select
              id="query-editor-partition"
              value={partition}
              options={this.getPartitionOptions()}
              onChange={(value) => this.onPartitionChange(value.value as number | 'all')}
              width={15}
              placeholder="Select partition"
              noOptionsMessage="Fetch topic partitions first"
            />
          </InlineField>
        </InlineFieldRow>
        <InlineFieldRow>
          <InlineField label="Auto offset reset" labelWidth={20} tooltip="Starting offset to consume that can be from latest or last 100.">
            <Select
              width={22}
              value={autoOffsetReset}
              options={autoResetOffsets}
              onChange={(value) => this.onAutoResetOffsetChanged(value.value as AutoOffsetReset)}
            />
          </InlineField>
          <InlineField label="Timestamp Mode" labelWidth={20} tooltip="Timestamp of the kafka value to visualize.">
            <Select
              width={25}
              value={timestampMode}
              options={timestampModes}
              onChange={(value) => this.onTimestampModeChanged(value.value as TimestampMode)}
            />
          </InlineField>
        </InlineFieldRow>
      </>
    );
  }
}
