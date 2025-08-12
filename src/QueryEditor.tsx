import { defaults, debounce, type DebouncedFunc } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { InlineField, InlineFieldRow, Input, Select, Button, Spinner, Alert, InlineLabel } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode } from './types';

const autoResetOffsets: Array<{ label: string; value: AutoOffsetReset }> = [
  { label: 'Latest', value: AutoOffsetReset.LATEST },
  { label: 'Last N messages', value: AutoOffsetReset.LAST_N },
  { label: 'Earliest', value: AutoOffsetReset.EARLIEST },
];

const timestampModes: Array<{ label: string; value: TimestampMode }> = [
  { label: 'Now', value: TimestampMode.Now },
  { label: 'Message Timestamp', value: TimestampMode.Message },
];

const partitionOptions: Array<{ label: string; value: number | 'all' }> = [{ label: 'All partitions', value: 'all' }];

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

interface State {
  availablePartitions: number[];
  topicSuggestions: string[];
  showingSuggestions: boolean;
  loadingPartitions: boolean;
  partitionSuccess?: string;
}

export class QueryEditor extends PureComponent<Props, State> {
  private debouncedSearchTopics: DebouncedFunc<(input: string) => void>;
  private lastCommittedTopic = '';
  private fetchPartitionsTimeoutId?: ReturnType<typeof setTimeout>;
  private topicBlurTimeout?: ReturnType<typeof setTimeout>;

  constructor(props: Props) {
    super(props);
    this.state = {
      availablePartitions: [],
      topicSuggestions: [],
      showingSuggestions: false,
      loadingPartitions: false,
      partitionSuccess: undefined,
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
    this.debouncedSearchTopics.cancel();
    if (this.fetchPartitionsTimeoutId) {
      clearTimeout(this.fetchPartitionsTimeoutId);
      this.fetchPartitionsTimeoutId = undefined;
    }
    if (this.topicBlurTimeout) {
      clearTimeout(this.topicBlurTimeout);
      this.topicBlurTimeout = undefined;
    }
  }

  fetchPartitions = async () => {
    const { datasource, query } = this.props;
    if (!query.topicName) {
      this.setState({ availablePartitions: [] });
      return;
    }
    this.setState({ loadingPartitions: true, partitionSuccess: undefined });
    try {
      const partitions = await datasource.getTopicPartitions(query.topicName);
      const partCount = (partitions || []).length;
      this.setState({
        availablePartitions: partitions || [],
        loadingPartitions: false,
        partitionSuccess: `Fetched ${partCount} partition${partCount === 1 ? '' : 's'}`,
      });
      // Clear success after 5s
      if (this.fetchPartitionsTimeoutId) {
        clearTimeout(this.fetchPartitionsTimeoutId);
        this.fetchPartitionsTimeoutId = undefined;
      }
      this.fetchPartitionsTimeoutId = setTimeout(() => {
        // Only clear if still mounted and timeout not replaced
        if (this.fetchPartitionsTimeoutId) {
          this.setState({ partitionSuccess: undefined });
          this.fetchPartitionsTimeoutId = undefined;
        }
      }, 5000);
    } catch (error) {
      console.error('Failed to fetch partitions:', error);
      // Rely on Grafana global error; do not show a duplicate inline error.
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
    // If switching to Last N and no lastN set, default to 100
    const next: KafkaQuery = { ...query, autoOffsetReset: value } as KafkaQuery;
    if (value === AutoOffsetReset.LAST_N && (!next.lastN || next.lastN <= 0)) {
      (next as any).lastN = 100;
    }
    onChange(next);
    onRunQuery();
  };

  onLastNChanged = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = Number(event.target.value);
    const n = Number.isFinite(value) ? Math.max(1, Math.min(1000000, Math.round(value))) : 100; // clamp 1..1e6
    onChange({ ...query, lastN: n });
    // Don't auto-run on every keystroke if empty; only when valid number present
    if (!Number.isNaN(n)) {
      onRunQuery();
    }
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
    if (this.topicBlurTimeout) {
      clearTimeout(this.topicBlurTimeout);
      this.topicBlurTimeout = undefined;
    }
    this.topicBlurTimeout = setTimeout(() => {
      this.setState({ showingSuggestions: false });
      this.commitTopicIfChanged();
      this.topicBlurTimeout = undefined;
    }, 150);
  };

  onTopicInputFocus = () => {
    if (this.state.topicSuggestions.length > 0) {
      this.setState({ showingSuggestions: true });
    }
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
  const { topicName, partition, autoOffsetReset, timestampMode, lastN } = query;

    return (
      <>
        <InlineFieldRow>
          <InlineField label="Topic" labelWidth={12} tooltip="Kafka topic name + click Fetch to load partitions" style={{ minWidth: 260 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', position: 'relative', minWidth: 260 }}>
              <div style={{ position: 'relative', minWidth: 180 }}>
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
                  width={24}
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
                style={{ minWidth: 60 }}
              >
                {this.state.loadingPartitions ? <Spinner size={14} /> : 'Fetch'}
              </Button>
              {this.state.partitionSuccess && (
                <div style={{ color: 'var(--gf-color-success-text, #73BF69)', fontSize: '12px' }}>
                  {this.state.partitionSuccess}
                </div>
              )}
            </div>
          </InlineField>
          <InlineField label="Partition" labelWidth={13} tooltip="Kafka partition selection" style={{ minWidth: 200 }}>
            <Select
              id="query-editor-partition"
              value={partition}
              options={this.getPartitionOptions()}
              onChange={(value) => this.onPartitionChange(value.value as number | 'all')}
              width={22}
              placeholder="Select partition"
              noOptionsMessage="Fetch topic partitions first"
              styles={{ container: (base: any) => ({ ...base, minWidth: 140 }) }}
            />
          </InlineField>
        </InlineFieldRow>
        <InlineFieldRow>
          <InlineField label="Offset" labelWidth={12} tooltip="Where to start consuming from for this query" style={{ minWidth: 260 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Select
                width={22}
                value={autoOffsetReset}
                options={autoResetOffsets}
                onChange={(value) => this.onAutoResetOffsetChanged(value.value as AutoOffsetReset)}
              />
              {autoOffsetReset === AutoOffsetReset.LAST_N && (
                <>
                  <InlineLabel width={12}>N</InlineLabel>
                  <Input
                    id="query-editor-last-n"
                    value={String(lastN ?? 100)}
                    type="number"
                    min={1}
                    step={1}
                    width={12}
                    onChange={this.onLastNChanged}
                  />
                </>
              )}
            </div>
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
        {autoOffsetReset !== AutoOffsetReset.LATEST && (
          <div style={{ marginTop: 8 }}>
            <Alert severity="warning" title="Potential higher load">
              Starting from Earliest or reading the last N messages can increase load on Kafka and the backend.
            </Alert>
          </div>
        )}
      </>
    );
  }
}
