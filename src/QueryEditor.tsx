import { defaults, debounce } from 'lodash';
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
  topicSuggestions: string[];
  showingSuggestions: boolean;
}

export class QueryEditor extends PureComponent<Props, State> {
  private debouncedSearchTopics: (input: string) => void;

  constructor(props: Props) {
    super(props);
    this.state = {
      availablePartitions: [],
      topicSuggestions: [],
      showingSuggestions: false,
    };
    
    // Debounce topic search to avoid too many API calls
    this.debouncedSearchTopics = debounce(this.searchTopics, 300);
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

  searchTopics = async (input: string) => {
    const { datasource } = this.props;
    if (!input || input.length < 2) {
      this.setState({ topicSuggestions: [], showingSuggestions: false });
      return;
    }

    try {
      console.log('Searching topics with prefix:', input);
      const topics = await datasource.searchTopics(input, 10); // Get more topics to filter smartly
      console.log('Received topics:', topics);
      
      // Smart filtering to avoid showing partial topic names
      const filteredTopics = topics
        .filter(topic => topic.toLowerCase().startsWith(input.toLowerCase()))
        .filter(topic => {
          // Filter out topics that look like partial typing
          // Keep topics that are:
          // 1. Exactly the input
          // 2. Significantly longer than input (likely complete topic names)
          // 3. Contain separators like -, _, . which indicate structure
          const hasStructure = /[-_.]/.test(topic);
          const isSignificantlyLonger = topic.length >= input.length + 3;
          const isExactMatch = topic.toLowerCase() === input.toLowerCase();
          
          return isExactMatch || isSignificantlyLonger || hasStructure;
        })
        .slice(0, 5); // Limit to 5 suggestions
      
      console.log('Filtered topics:', filteredTopics);
      
      this.setState({ 
        topicSuggestions: filteredTopics,
        showingSuggestions: filteredTopics.length > 0 
      });
    } catch (error) {
      console.error('Failed to search topics:', error);
      this.setState({ topicSuggestions: [], showingSuggestions: false });
    }
  };

  onTopicNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, topicName: event.target.value });
    onRunQuery();
    
    // Trigger topic search for autocomplete
    this.debouncedSearchTopics(event.target.value);
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
    onRunQuery();
  };

  onTopicInputBlur = () => {
    // Delay hiding suggestions to allow clicking on them
    setTimeout(() => {
      this.setState({ showingSuggestions: false });
    }, 200);
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
          <InlineField label="Topic" labelWidth={10} tooltip="Kafka topic name">
            <div style={{ position: 'relative', width: '20ch' }}>
              <Input
                id="query-editor-topic"
                value={topicName || ''}
                onChange={this.onTopicNameChange}
                onBlur={this.onTopicInputBlur}
                onFocus={this.onTopicInputFocus}
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
                    overflowY: 'auto'
                  }}
                >
                  {this.state.topicSuggestions.map((topic, index) => (
                    <div
                      key={index}
                      style={{
                        padding: '8px 12px',
                        cursor: 'pointer',
                        borderBottom: index < this.state.topicSuggestions.length - 1 ? '1px solid var(--gf-color-border-weak, #333)' : 'none',
                        fontSize: '13px',
                        color: 'var(--gf-color-text-primary, #ffffff)',
                        backgroundColor: 'transparent'
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
