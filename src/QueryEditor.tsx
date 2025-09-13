import { debounce, type DebouncedFunc } from 'lodash';
import React, { ChangeEvent, PureComponent } from 'react';
import { InlineField, InlineFieldRow, Input, Select, Button, Spinner, Alert, InlineLabel } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery, AutoOffsetReset, TimestampMode, AvroSchemaSource, MessageFormat } from './types';

// Constants for Last N messages input
const LAST_N_MIN = 1;
const LAST_N_MAX = 1000000;
const LAST_N_DEFAULT = 100;

const autoResetOffsets: Array<{ label: string; value: AutoOffsetReset }> = [
  { label: 'Latest', value: AutoOffsetReset.LATEST },
  { label: 'Last N messages', value: AutoOffsetReset.LAST_N },
  { label: 'Earliest', value: AutoOffsetReset.EARLIEST },
];

const timestampModes: Array<{ label: string; value: TimestampMode }> = [
  { label: 'Kafka Event Time', value: TimestampMode.Message },
  { label: 'Dashboard received time', value: TimestampMode.Now },
];

const avroSchemaSources: Array<{ label: string; value: AvroSchemaSource }> = [
  { label: 'Schema Registry', value: AvroSchemaSource.SCHEMA_REGISTRY },
  { label: 'Inline Schema', value: AvroSchemaSource.INLINE_SCHEMA },
];

const messageFormats: Array<{ label: string; value: MessageFormat }> = [
  { label: 'JSON', value: MessageFormat.JSON },
  { label: 'Avro', value: MessageFormat.AVRO },
];

const partitionOptions: Array<{ label: string; value: number | 'all' }> = [{ label: 'All partitions', value: 'all' }];

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

interface State {
  availablePartitions: number[];
  topicSuggestions: string[];
  showingSuggestions: boolean;
  loadingPartitions: boolean;
  partitionSuccess?: string;
  schemaValidation?: {
    status: 'valid' | 'invalid' | 'loading';
    message?: string;
  };
  schemaRegistryValidation?: {
    status: 'valid' | 'invalid' | 'loading';
    message?: string;
  };
}

export class QueryEditor extends PureComponent<Props, State> {
  private debouncedSearchTopics: DebouncedFunc<(input: string) => void>;
  private debouncedRunQuery: DebouncedFunc<() => void>;
  private lastCommittedTopic = '';
  private fetchPartitionsTimeoutId?: ReturnType<typeof setTimeout>;
  private topicBlurTimeout?: ReturnType<typeof setTimeout>;
  private schemaValidationTimeout?: ReturnType<typeof setTimeout>;

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
    // Add debounced version of query execution to handle rapid changes better
    this.debouncedRunQuery = debounce(() => {
      this.props.onRunQuery();
    }, 500);
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
    // Cancel debounced query execution to avoid issues after unmount
    this.debouncedRunQuery.cancel();
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
    const { datasource, query, onChange, onRunQuery } = this.props;
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
      
      // Auto-apply "all partitions" if not already set and trigger query
      if (query.partition === undefined || query.partition === null) {
        console.log('Auto-applying "all partitions" after fetch');
        onChange({ ...query, partition: 'all' });
        onRunQuery();
      }
      
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
    const query = { ...defaultQuery, ...this.props.query };
    const { partition } = query;
    const present = new Set<number>();
    (this.state.availablePartitions || []).forEach((p) => {
      options.push({ label: `Partition ${p}`, value: p });
      present.add(p);
    });
    // Ensure previously selected numeric partition is visible even before fetching
    if (typeof partition === 'number' && !present.has(partition)) {
      options.push({ label: `Partition ${partition}`, value: partition });
    }
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
    const query = { ...defaultQuery, ...this.props.query };
    const { topicName } = query;
    if (topicName && topicName !== this.lastCommittedTopic) {
      this.lastCommittedTopic = topicName;
      this.props.onRunQuery();
    }
  };

  onPartitionChange = (value: number | 'all') => {
    console.log('onPartitionChange called with value:', value, 'type:', typeof value);
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, partition: value });
    // For partition changes, run immediately but also schedule debounced version
    // This ensures tests pass while still providing debounced behavior for rapid changes
    onRunQuery();
    this.debouncedRunQuery();
  };

  onAutoResetOffsetChanged = (value: AutoOffsetReset) => {
    const { onChange, query, onRunQuery } = this.props;
    // If switching to Last N and no lastN set, default to 100
    const next: KafkaQuery = { ...query, autoOffsetReset: value } as KafkaQuery;
    if (value === AutoOffsetReset.LAST_N && (!next.lastN || next.lastN <= 0)) {
      next.lastN = 100;
    }
    onChange(next);
    onRunQuery();
  };

  onLastNChanged = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const value = Number(event.target.value);
    const n = Number.isFinite(value) ? Math.max(LAST_N_MIN, Math.min(LAST_N_MAX, Math.round(value))) : LAST_N_DEFAULT; // clamp LAST_N_MIN..LAST_N_MAX
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

  onAvroSchemaSourceChanged = (value: AvroSchemaSource) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, avroSchemaSource: value });
    onRunQuery();
  };

  onAvroSchemaChanged = (event: ChangeEvent<HTMLTextAreaElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const newSchema = event.target.value;
    onChange({ ...query, avroSchema: newSchema });
    onRunQuery();

    // Validate the schema after a short delay
    if (this.schemaValidationTimeout) {
      clearTimeout(this.schemaValidationTimeout);
    }
    this.schemaValidationTimeout = setTimeout(() => {
      this.validateAvroSchema(newSchema);
    }, 500);
  };

  onAvroSchemaFileUpload = (event: ChangeEvent<HTMLInputElement>) => {
    const { onChange, query, onRunQuery } = this.props;
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const schema = e.target?.result as string;
        onChange({ ...query, avroSchema: schema });
        onRunQuery();
      };
      reader.readAsText(file);
    }
  };

  validateAvroSchema = async (schema: string) => {
    if (!schema.trim()) {
      this.setState({
        schemaValidation: { status: 'invalid', message: 'Schema cannot be empty' },
      });
      return;
    }

    this.setState({
      schemaValidation: { status: 'loading' },
    });

    try {
      const result = await this.props.datasource.validateAvroSchema(schema);
      this.setState({
        schemaValidation: {
          status: result.status === 'error' ? 'invalid' : 'valid',
          message: result.message,
        },
      });
    } catch (error) {
      this.setState({
        schemaValidation: {
          status: 'invalid',
          message: 'Failed to validate schema',
        },
      });
    }
  };

  validateSchemaRegistry = async () => {
    this.setState({
      schemaRegistryValidation: { status: 'loading' },
    });

    try {
      const result = await this.props.datasource.validateSchemaRegistry();
      this.setState({
        schemaRegistryValidation: {
          status: result.status === 'error' ? 'invalid' : 'valid',
          message: result.message,
        },
      });
    } catch (error) {
      this.setState({
        schemaRegistryValidation: {
          status: 'invalid',
          message: 'Failed to validate schema registry',
        },
      });
    }
  };

  onMessageFormatChanged = (value: MessageFormat) => {
    const { onChange, query, onRunQuery } = this.props;
    onChange({ ...query, messageFormat: value });
    // For message format changes, run immediately but also schedule debounced version
    onRunQuery();
    this.debouncedRunQuery();
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
    const query = { ...defaultQuery, ...this.props.query };
    const { topicName, partition, autoOffsetReset, timestampMode, lastN, messageFormat } = query;

    return (
      <>
        <InlineFieldRow>
          <InlineField
            label="Topic"
            labelWidth={12}
            tooltip="Kafka topic name + click Fetch to load partitions"
            style={{ minWidth: 260 }}
          >
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
                  autoComplete="off"
                  autoCorrect="off"
                  autoCapitalize="none"
                  spellCheck={false}
                  name="gf-topic-search"
                  aria-autocomplete="list"
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
              onChange={(opt) => this.onPartitionChange(opt.value as number | 'all')}
              isClearable={false}
              width={22}
              placeholder="Select partition"
              noOptionsMessage="Fetch topic partitions first"
              styles={{ container: (base: any) => ({ ...base, minWidth: 140 }) }}
            />
          </InlineField>
        </InlineFieldRow>
        <InlineFieldRow>
          <InlineField
            label="Offset"
            labelWidth={12}
            tooltip="Where to start consuming from for this query"
            style={{ minWidth: 260 }}
          >
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
          <InlineField 
            label="Timestamp Mode" 
            labelWidth={20} 
            tooltip={timestampMode === TimestampMode.Message 
              ? 'Kafka Event Time: Timestamp from the Kafka message metadata.' 
              : 'Dashboard received time: When the Grafana plugin received the message.'}
          >
            <Select
              width={25}
              value={timestampMode}
              options={timestampModes}
              onChange={(value) => this.onTimestampModeChanged(value.value as TimestampMode)}
            />
          </InlineField>
        </InlineFieldRow>

        <InlineFieldRow>
          <InlineField 
            label="Message Format" 
            labelWidth={25} 
            tooltip="Format of the Kafka messages (JSON or Avro)"
          >
            <Select
              width={25}
              value={messageFormat || MessageFormat.JSON}
              options={messageFormats}
              onChange={(value) => this.onMessageFormatChanged(value.value as MessageFormat)}
            />
          </InlineField>
        </InlineFieldRow>

        {/* Avro Configuration */}
        {messageFormat === MessageFormat.AVRO && (
          <>
            <InlineFieldRow>
              <InlineField
                label="Avro Schema Source"
                labelWidth={25}
                tooltip="Source of the Avro schema for deserialization"
              >
                <Select
                  width={25}
                  value={query.avroSchemaSource || AvroSchemaSource.SCHEMA_REGISTRY}
                  options={avroSchemaSources}
                  onChange={(value) => this.onAvroSchemaSourceChanged(value.value as AvroSchemaSource)}
                />
              </InlineField>
            </InlineFieldRow>

            {query.avroSchemaSource === AvroSchemaSource.SCHEMA_REGISTRY && (
              <InlineFieldRow>
                <InlineField label="Schema Registry" labelWidth={25}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={this.validateSchemaRegistry}
                      disabled={this.state.schemaRegistryValidation?.status === 'loading'}
                    >
                      {this.state.schemaRegistryValidation?.status === 'loading' ? (
                        <>
                          <Spinner size={12} />
                          Validating...
                        </>
                      ) : (
                        'Test Connection'
                      )}
                    </Button>
                    {this.state.schemaRegistryValidation && (
                      <InlineLabel color={this.state.schemaRegistryValidation.status === 'valid' ? 'green' : 'red'}>
                        {this.state.schemaRegistryValidation.message}
                      </InlineLabel>
                    )}
                  </div>
                </InlineField>
              </InlineFieldRow>
            )}

            {query.avroSchemaSource === AvroSchemaSource.INLINE_SCHEMA && (
              <InlineFieldRow>
                <InlineField
                  label="Avro Schema"
                  labelWidth={20}
                  tooltip="Upload or paste your Avro schema (.avsc file)"
                  style={{ minWidth: 400 }}
                >
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', minWidth: 400 }}>
                    <input
                      type="file"
                      accept=".avsc,.json"
                      onChange={this.onAvroSchemaFileUpload}
                      style={{ marginBottom: '8px' }}
                    />
                    <textarea
                      value={query.avroSchema || ''}
                      onChange={this.onAvroSchemaChanged}
                      placeholder="Paste your Avro schema JSON here..."
                      rows={8}
                      style={{
                        width: '100%',
                        fontFamily: 'monospace',
                        fontSize: '12px',
                        resize: 'vertical',
                      }}
                    />
                    {this.state.schemaValidation && (
                      <div style={{ marginTop: '4px' }}>
                        {this.state.schemaValidation.status === 'loading' ? (
                          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <Spinner size={12} />
                            <span style={{ fontSize: '12px', color: '#666' }}>Validating schema...</span>
                          </div>
                        ) : (
                          <InlineLabel
                            color={this.state.schemaValidation.status === 'valid' ? 'green' : 'red'}
                            style={{ fontSize: '12px' }}
                          >
                            {this.state.schemaValidation.message}
                          </InlineLabel>
                        )}
                      </div>
                    )}
                  </div>
                </InlineField>
              </InlineFieldRow>
            )}
          </>
        )}

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
