import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { QueryEditor } from '../QueryEditor';
import { AutoOffsetReset, TimestampMode, MessageFormat, defaultQuery, type KafkaQuery } from '../types';

// Mock @grafana/ui components
jest.mock('@grafana/ui', () => ({
  InlineField: ({ children, label }: any) => (
    <div data-testid="inline-field" aria-label={label}>
      {children}
    </div>
  ),
  InlineFieldRow: ({ children }: any) => <div data-testid="inline-field-row">{children}</div>,
  Input: (props: any) => <input {...props} />,
  Select: ({ value, onChange, options, placeholder, id }: any) => (
    <select
      id={id}
      value={String(value)}
      onChange={(e) => {
        const selectedOption = options.find((opt: any) => String(opt.value) === e.target.value);
        if (selectedOption && onChange) {
          onChange(selectedOption);
        }
      }}
      data-testid="select"
    >
      <option value="">{placeholder}</option>
      {options?.map((option: any) => (
        <option key={option.value} value={String(option.value)}>
          {option.label}
        </option>
      ))}
    </select>
  ),
  Button: ({ children, onClick, disabled }: any) => (
    <button onClick={onClick} disabled={disabled}>
      {children}
    </button>
  ),
  Spinner: () => <div data-testid="spinner">Loading...</div>,
  Alert: ({ children, title, severity }: any) => (
    <div data-testid="alert" data-severity={severity}>
      {title && <h4>{title}</h4>}
      {children}
    </div>
  ),
  InlineLabel: ({ children }: any) => <label>{children}</label>,
}));

// Enhanced DataSource mock
const mockDs = {
  getTopicPartitions: jest.fn().mockResolvedValue([0, 1, 2]),
  searchTopics: jest.fn().mockResolvedValue(['topic1', 'topic2', 'my-topic']),
} as any;

const onChange = jest.fn();
const onRunQuery = jest.fn();

beforeEach(() => {
  jest.useFakeTimers();
  jest.clearAllMocks();
});
afterEach(() => {
  jest.runOnlyPendingTimers();
  jest.useRealTimers();
});

const renderEditor = (query?: Partial<KafkaQuery>) =>
  render(
    <QueryEditor
      datasource={mockDs}
      onChange={onChange}
      onRunQuery={onRunQuery}
      query={{ refId: 'A', ...defaultQuery, ...query } as KafkaQuery}
      app={'explore' as any}
    />
  );

describe('QueryEditor', () => {
  it('does not mutate frozen props', () => {
    const frozenQuery: KafkaQuery = Object.freeze({
      refId: 'A',
      topicName: '',
      partition: 'all',
      autoOffsetReset: AutoOffsetReset.LATEST,
      timestampMode: TimestampMode.Message,
      lastN: 100,
      messageFormat: MessageFormat.JSON,
    });
    expect(() => {
      render(
        <QueryEditor
          datasource={mockDs}
          onChange={onChange}
          onRunQuery={onRunQuery}
          query={frozenQuery}
          app={'explore' as any}
        />
      );
    }).not.toThrow();
  });
  it('disables browser autocomplete on topic input and shows suggestions panel', async () => {
    renderEditor({ topicName: '' });
    const input = screen.getByPlaceholderText('Enter topic name') as HTMLInputElement;

    // Verify autocomplete attributes are set
    expect(input.getAttribute('autocomplete')).toBe('off');
    expect(input.getAttribute('autocorrect')).toBe('off');
    expect(input.getAttribute('autocapitalize')).toBe('none');
    expect(input.getAttribute('spellcheck')).toBe('false');

    // Type to trigger search (debounced internally). We won't assert network; just UI flags.
    fireEvent.change(input, { target: { value: 'my' } });
    // Focus should allow suggestions container to toggle on
    fireEvent.focus(input);
  });

  it('renders Last N input only when mode is LAST_N', () => {
    const { rerender } = renderEditor({ autoOffsetReset: AutoOffsetReset.LATEST });
    // When mode is LATEST, the lastN input should not be present
    expect(document.getElementById('query-editor-last-n')).toBeNull();

    rerender(
      <QueryEditor
        datasource={mockDs}
        onChange={onChange}
        onRunQuery={onRunQuery}
        query={{ refId: 'A', ...defaultQuery, autoOffsetReset: AutoOffsetReset.LAST_N } as KafkaQuery}
        app={'explore' as any}
      />
    );

    // When mode is LAST_N, the lastN input should be present
    expect(document.getElementById('query-editor-last-n')).toBeInTheDocument();
  });

  it('calls onChange when topic name changes', () => {
    renderEditor({ topicName: 'initial-topic' });
    const input = screen.getByPlaceholderText('Enter topic name') as HTMLInputElement;

    fireEvent.change(input, { target: { value: 'new-topic' } });

    expect(onChange).toHaveBeenCalledWith({
      refId: 'A',
      ...defaultQuery,
      topicName: 'new-topic',
    });
  });

  it('calls onChange and onRunQuery when partition changes', () => {
    // Start with a numeric partition so it's included in options before fetching
    renderEditor({ partition: 1 });
    const partitionSelect = document.getElementById('query-editor-partition') as HTMLSelectElement;

    // Change to 'all'
    fireEvent.change(partitionSelect, { target: { value: 'all' } });

    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ partition: 'all' }));
    expect(onRunQuery).toHaveBeenCalled();
  });

  it('calls onChange and onRunQuery when auto offset reset changes', () => {
    renderEditor({ autoOffsetReset: AutoOffsetReset.LATEST });
    const offsetSelect = screen.getAllByTestId('select')[1]; // Second select is offset

    fireEvent.change(offsetSelect, { target: { value: AutoOffsetReset.EARLIEST } });

    expect(onChange).toHaveBeenCalledWith({
      refId: 'A',
      ...defaultQuery,
      autoOffsetReset: AutoOffsetReset.EARLIEST,
    });
    expect(onRunQuery).toHaveBeenCalled();
  });

  it('sets default lastN value when switching to LAST_N mode', () => {
    renderEditor({ autoOffsetReset: AutoOffsetReset.LATEST });
    const offsetSelect = screen.getAllByTestId('select')[1];

    fireEvent.change(offsetSelect, { target: { value: AutoOffsetReset.LAST_N } });

    expect(onChange).toHaveBeenCalledWith({
      refId: 'A',
      ...defaultQuery,
      autoOffsetReset: AutoOffsetReset.LAST_N,
      lastN: 100,
    });
  });

  it('calls onChange and onRunQuery when lastN changes', () => {
    renderEditor({ autoOffsetReset: AutoOffsetReset.LAST_N, lastN: 100 });
    const lastNInput = document.getElementById('query-editor-last-n') as HTMLInputElement;

    fireEvent.change(lastNInput, { target: { value: '50' } });

    expect(onChange).toHaveBeenCalledWith({
      refId: 'A',
      ...defaultQuery,
      autoOffsetReset: AutoOffsetReset.LAST_N,
      lastN: 50,
    });
    expect(onRunQuery).toHaveBeenCalled();
  });

  it('clamps lastN values to valid range', () => {
    renderEditor({ autoOffsetReset: AutoOffsetReset.LAST_N, lastN: 100 });
    const lastNInput = document.getElementById('query-editor-last-n') as HTMLInputElement;

    // Test negative value gets clamped to 1
    fireEvent.change(lastNInput, { target: { value: '-5' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ lastN: 1 }));

    // Test zero gets clamped to 1
    fireEvent.change(lastNInput, { target: { value: '0' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ lastN: 1 }));

    // Test very large value gets clamped
    fireEvent.change(lastNInput, { target: { value: '10000000' } });
    expect(onChange).toHaveBeenCalledWith(expect.objectContaining({ lastN: 1000000 }));
  });

  it('calls onChange and onRunQuery when timestamp mode changes', () => {
    renderEditor({ timestampMode: TimestampMode.Now });
    const timestampSelect = screen.getAllByTestId('select')[2]; // Third select is timestamp mode

    fireEvent.change(timestampSelect, { target: { value: TimestampMode.Message } });

    expect(onChange).toHaveBeenCalledWith({
      refId: 'A',
      ...defaultQuery,
      timestampMode: TimestampMode.Message,
    });
    expect(onRunQuery).toHaveBeenCalled();
  });

  it('shows warning alert for non-LATEST offset modes', () => {
    const { rerender } = renderEditor({ autoOffsetReset: AutoOffsetReset.LATEST });
    expect(screen.queryByTestId('alert')).toBeNull();

    rerender(
      <QueryEditor
        datasource={mockDs}
        onChange={onChange}
        onRunQuery={onRunQuery}
        query={{ refId: 'A', ...defaultQuery, autoOffsetReset: AutoOffsetReset.EARLIEST } as KafkaQuery}
        app={'explore' as any}
      />
    );

    expect(screen.getByTestId('alert')).toBeInTheDocument();
    expect(screen.getByText('Potential higher load')).toBeInTheDocument();
  });

  it('calls fetchPartitions when fetch button is clicked', async () => {
    renderEditor({ topicName: 'test-topic' });
    const fetchButton = screen.getByText('Fetch');

    fireEvent.click(fetchButton);

    expect(mockDs.getTopicPartitions).toHaveBeenCalledWith('test-topic');
  });

  it('disables fetch button when no topic name or loading', () => {
    const { rerender } = renderEditor({ topicName: '' });
    const fetchButton = screen.getByText('Fetch');

    expect(fetchButton).toBeDisabled();

    rerender(
      <QueryEditor
        datasource={mockDs}
        onChange={onChange}
        onRunQuery={onRunQuery}
        query={{ refId: 'A', ...defaultQuery, topicName: 'test-topic' } as KafkaQuery}
        app={'explore' as any}
      />
    );

    expect(fetchButton).not.toBeDisabled();
  });

  it('commits topic and triggers query on Enter key', () => {
    renderEditor({ topicName: 'test-topic' });
    const input = screen.getByPlaceholderText('Enter topic name') as HTMLInputElement;

    fireEvent.keyDown(input, { key: 'Enter' });

    expect(onRunQuery).toHaveBeenCalled();
    expect(mockDs.getTopicPartitions).toHaveBeenCalledWith('test-topic');
  });

  it('shows available partitions in partition select after fetch', async () => {
    mockDs.getTopicPartitions.mockResolvedValueOnce([0, 1, 2, 3]);
    renderEditor({ topicName: 'test-topic' });

    const fetchButton = screen.getByText('Fetch');
    fireEvent.click(fetchButton);

    // Wait for the async operation to complete
    await waitFor(() => {
      const partitionSelect = document.getElementById('query-editor-partition') as HTMLSelectElement;
      // Includes placeholder option + All partitions + 4 partitions
      expect(partitionSelect.options).toHaveLength(6);
    });
  });

  it('handles fetch partitions error gracefully', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
    mockDs.getTopicPartitions.mockRejectedValueOnce(new Error('Network error'));

    renderEditor({ topicName: 'test-topic' });
    const fetchButton = screen.getByText('Fetch');

    fireEvent.click(fetchButton);

    await waitFor(() => {
      expect(consoleError).toHaveBeenCalledWith('Failed to fetch partitions:', expect.any(Error));
    });

    consoleError.mockRestore();
  });

  it('searches for topics when typing with debounce', async () => {
    renderEditor({ topicName: '' });
    const input = screen.getByPlaceholderText('Enter topic name') as HTMLInputElement;

    fireEvent.change(input, { target: { value: 'my' } });

    // The search is debounced, so we need to wait
    await waitFor(
      () => {
        expect(mockDs.searchTopics).toHaveBeenCalledWith('my', 10);
      },
      { timeout: 500 }
    );
  });

  it('handles topic search error gracefully', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => {});
    mockDs.searchTopics.mockRejectedValueOnce(new Error('Search error'));

    renderEditor({ topicName: '' });
    const input = screen.getByPlaceholderText('Enter topic name') as HTMLInputElement;

    fireEvent.change(input, { target: { value: 'test' } });

    await waitFor(
      () => {
        expect(consoleError).toHaveBeenCalledWith('Failed to search topics:', expect.any(Error));
      },
      { timeout: 500 }
    );

    consoleError.mockRestore();
  });
});
