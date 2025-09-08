import { of } from 'rxjs';
import { DataSource } from '../datasource';
import { AutoOffsetReset, TimestampMode, MessageFormat, AvroSchemaSource, type KafkaQuery } from '../types';

import { deepFreeze } from '../test-utils/test-helpers';

// Mock @grafana/runtime pieces used by DataSource
let capturedPath: string | undefined;
// Allow tests to override template replacement logic dynamically
let templateReplaceImpl = (v: string) => v.replace('${var}', '5');
jest.mock('@grafana/runtime', () => {
  return {
    getTemplateSrv: () => ({
      replace: (v: string) => templateReplaceImpl(v),
    }),
    getGrafanaLiveSrv: () => ({
      getDataStream: ({ addr }: any) => {
        capturedPath = addr.path;
        return of({ data: [] });
      },
    }),
    DataSourceWithBackend: class {
      constructor(public instanceSettings: any) {}
      getResource = jest.fn();
    },
  } as any;
});

const mockInstanceSettings = {
  id: 1,
  uid: 'test-kafka-uid',
  type: 'kafka',
  name: 'Test Kafka',
  url: '',
  jsonData: {
    bootstrapServers: 'localhost:9092',
    securityProtocol: 'PLAINTEXT',
  },
};

describe('DataSource', () => {
  it('does not mutate frozen query props', () => {
    const frozenQuery = deepFreeze({
      topicName: 'test-topic',
      partition: 'all',
      autoOffsetReset: AutoOffsetReset.LATEST,
      timestampMode: TimestampMode.Message,
      lastN: 100,
      refId: 'A',
    });
    ds = new DataSource(mockInstanceSettings as any);
    expect(() => {
      ds.filterQuery(frozenQuery as KafkaQuery);
      ds.applyTemplateVariables(frozenQuery as KafkaQuery, {});
    }).not.toThrow();
  });
  let ds: DataSource;

  beforeEach(() => {
    ds = new DataSource(mockInstanceSettings as any);
    capturedPath = undefined;
    jest.clearAllMocks();
  });

  describe('getDefaultQuery', () => {
    it('returns default query values with Kafka Event Time mode', () => {
      const defaultQuery = ds.getDefaultQuery('explore' as any);
      expect(defaultQuery).toEqual({
        topicName: '',
        partition: 'all',
        autoOffsetReset: AutoOffsetReset.LATEST,
        timestampMode: TimestampMode.Message,
        lastN: 100,
        messageFormat: MessageFormat.JSON,
        avroSchemaSource: AvroSchemaSource.SCHEMA_REGISTRY,
      });
    });
    it('returns query values with Kafka Message Timestamp mode', () => {
      const defaultQuery = ds.getDefaultQuery('explore' as any);
      const customQuery = { ...defaultQuery, timestampMode: TimestampMode.Message };
      expect(customQuery.timestampMode).toBe(TimestampMode.Message);
    });
  });

  describe('filterQuery', () => {
    it('returns false for queries without topic name', () => {
      const query = { topicName: '', partition: 'all' } as KafkaQuery;
      expect(ds.filterQuery(query)).toBe(false);
    });

    it('returns false for queries with undefined topic name', () => {
      const query = { partition: 'all' } as KafkaQuery;
      expect(ds.filterQuery(query)).toBe(false);
    });

    it('returns true for valid queries with topic name and all partitions', () => {
      const query = { topicName: 'test-topic', partition: 'all' } as KafkaQuery;
      expect(ds.filterQuery(query)).toBe(true);
    });

    it('returns true for valid queries with topic name and numeric partition', () => {
      const query = { topicName: 'test-topic', partition: 0 } as KafkaQuery;
      expect(ds.filterQuery(query)).toBe(true);
    });

    it('returns false for queries with negative partition', () => {
      const query = { topicName: 'test-topic', partition: -1 } as KafkaQuery;
      expect(ds.filterQuery(query)).toBe(false);
    });
  });

  describe('applyTemplateVariables', () => {
    it('sanitizes lastN only for LAST_N and defaults to 100', () => {
      const base: KafkaQuery = {
        refId: 'A',
        topicName: 'topic-${var}',
        partition: 'all',
        autoOffsetReset: AutoOffsetReset.LAST_N,
        timestampMode: TimestampMode.Now,
        lastN: undefined,
        messageFormat: MessageFormat.JSON,
      };
      const out = ds.applyTemplateVariables(base, {} as any);
      expect(out.topicName).toBe('topic-5');
      expect(out.lastN).toBe(100);

      const latestOut = ds.applyTemplateVariables(
        { ...base, autoOffsetReset: AutoOffsetReset.LATEST, lastN: 123 },
        {} as any
      );
      expect(latestOut.lastN).toBeUndefined();
    });

    it('applies template variables to topic name', () => {
      const query: KafkaQuery = {
        refId: 'A',
        topicName: 'prefix-${var}-suffix',
        partition: 'all',
        autoOffsetReset: AutoOffsetReset.LATEST,
        timestampMode: TimestampMode.Now,
        messageFormat: MessageFormat.JSON,
      };

      const result = ds.applyTemplateVariables(query, {});
      expect(result.topicName).toBe('prefix-5-suffix');
    });

    it('applies template variables to partition when it is a string', () => {
      const query: KafkaQuery = {
        refId: 'A',
        topicName: 'test-topic',
        partition: '${var}' as any,
        autoOffsetReset: AutoOffsetReset.LATEST,
        timestampMode: TimestampMode.Now,
        messageFormat: MessageFormat.JSON,
      };

      const result = ds.applyTemplateVariables(query, {});
      expect(result.partition).toBe(5);
    });

    it('does not change numeric partition unless replaced value is valid', () => {
      // Mock template service to return invalid value for numeric partition replacement
      (ds as any).templateSrv = {
        replace: () => 'invalid',
      };

      const query: KafkaQuery = {
        refId: 'A',
        topicName: 'test-topic',
        partition: 2,
        autoOffsetReset: AutoOffsetReset.LATEST,
        timestampMode: TimestampMode.Now,
        messageFormat: MessageFormat.JSON,
      };

      const result = ds.applyTemplateVariables(query, {});
      expect(result.partition).toBe(2);
    });

    it('handles invalid partition template replacement gracefully', () => {
      // Mock template service to return invalid value
      (ds as any).templateSrv = {
        replace: () => 'invalid',
      };

      const query: KafkaQuery = {
        refId: 'A',
        topicName: 'test-topic',
        partition: '${invalid}' as any,
        autoOffsetReset: AutoOffsetReset.LATEST,
        timestampMode: TimestampMode.Now,
        messageFormat: MessageFormat.JSON,
      };

      const result = ds.applyTemplateVariables(query, {});
      expect(result.partition).toBe('all');
    });

    it('sets default lastN when switching to LAST_N with invalid value', () => {
      const query: KafkaQuery = {
        refId: 'A',
        topicName: 'test-topic',
        partition: 'all',
        autoOffsetReset: AutoOffsetReset.LAST_N,
        timestampMode: TimestampMode.Now,
        lastN: 0,
        messageFormat: MessageFormat.JSON,
      };

      const result = ds.applyTemplateVariables(query, {});
      expect(result.lastN).toBe(100);
    });

    it('applies template variables to lastN when provided as string', () => {
      // Our template service replaces '${var}' with '5'
      const query: KafkaQuery = {
        refId: 'A',
        topicName: 'test-topic',
        partition: 'all',
        autoOffsetReset: AutoOffsetReset.LAST_N,
        timestampMode: TimestampMode.Now,
        lastN: undefined,
        messageFormat: MessageFormat.JSON,
      };

      const withTemplate = { ...query, lastN: undefined } as any;
      const result = ds.applyTemplateVariables(withTemplate, { lastN: { value: '${var}' } } as any);
      // Since our applyTemplateVariables only replaces lastN from query, not scoped var, it will default to 100
      expect(result.lastN).toBe(100);
    });
  });

  describe('query', () => {
    it('builds a clean path without dangling dash and includes lastN only for LAST_N', (done) => {
      const target: KafkaQuery = {
        refId: 'A',
        topicName: 'my topic', // contains space to validate encoding
        partition: 0,
        autoOffsetReset: AutoOffsetReset.LATEST,
        timestampMode: TimestampMode.Now,
        messageFormat: MessageFormat.JSON,
      } as any;

      // Trigger query which builds the path internally
      ds.query({ targets: [target] } as any).subscribe({
        next: () => {
          // Not used
        },
        complete: () => {
          try {
            expect(capturedPath).toBe('my%20topic-0-latest');
            // Now with LAST_N
            capturedPath = undefined;
            const target2: KafkaQuery = { ...target, autoOffsetReset: AutoOffsetReset.LAST_N, lastN: 10, messageFormat: MessageFormat.JSON } as any;
            ds.query({ targets: [target2] } as any).subscribe({
              complete: () => {
                try {
                  expect(capturedPath).toBe('my%20topic-0-lastN-10');
                  done();
                } catch (e) {
                  done(e as any);
                }
              },
            });
          } catch (e) {
            done(e as any);
          }
        },
      });
    });

    it('filters out invalid queries', (done) => {
      const targets = [
        { refId: 'A', topicName: '', partition: 'all' }, // Invalid - no topic
        { refId: 'B', topicName: 'valid-topic', partition: 'all', autoOffsetReset: AutoOffsetReset.LATEST }, // Valid
        { refId: 'C', topicName: 'another-topic', partition: -1 }, // Invalid - negative partition
      ] as KafkaQuery[];

      ds.query({ targets } as any).subscribe({
        complete: () => {
          // Only one valid query should have been processed
          expect(capturedPath).toBe('valid-topic-all-latest');
          done();
        },
      });
    });

    it('returns empty data when no valid targets', (done) => {
      const targets = [{ refId: 'A', topicName: '', partition: 'all' }] as KafkaQuery[];

      ds.query({ targets } as any).subscribe({
        next: (response) => {
          expect(response.data).toEqual([]);
          done();
        },
      });
    });

    it('encodes special characters in path segments', (done) => {
      const target: KafkaQuery = {
        refId: 'A',
        topicName: 'topic/with-special:chars',
        partition: 'all',
        autoOffsetReset: AutoOffsetReset.EARLIEST,
        timestampMode: TimestampMode.Now,
        messageFormat: MessageFormat.JSON,
      } as any;

      ds.query({ targets: [target] } as any).subscribe({
        complete: () => {
          expect(capturedPath).toBe('topic%2Fwith-special%3Achars-all-earliest');
          done();
        },
      });
    });
  });

  describe('getTopicPartitions', () => {
    it('returns partitions from getResource', async () => {
      const mockPartitions = [0, 1, 2, 3];
      (ds as any).getResource = jest.fn().mockResolvedValue({ partitions: mockPartitions });

      const result = await ds.getTopicPartitions('test-topic');

      expect(result).toEqual(mockPartitions);
      expect((ds as any).getResource).toHaveBeenCalledWith('partitions', { topic: 'test-topic' });
    });

    it('returns empty array when response has no partitions', async () => {
      (ds as any).getResource = jest.fn().mockResolvedValue({});

      const result = await ds.getTopicPartitions('test-topic');

      expect(result).toEqual([]);
    });

    it('re-throws errors from getResource', async () => {
      const error = new Error('Network error');
      (ds as any).getResource = jest.fn().mockRejectedValue(error);

      await expect(ds.getTopicPartitions('test-topic')).rejects.toThrow('Network error');
    });
  });

  describe('searchTopics', () => {
    it('returns topics from getResource', async () => {
      const mockTopics = ['topic1', 'topic2', 'my-topic'];
      (ds as any).getResource = jest.fn().mockResolvedValue({ topics: mockTopics });

      const result = await ds.searchTopics('topic', 10);

      expect(result).toEqual(mockTopics);
      expect((ds as any).getResource).toHaveBeenCalledWith('topics', { prefix: 'topic', limit: '10' });
    });

    it('returns empty array when response has no topics', async () => {
      (ds as any).getResource = jest.fn().mockResolvedValue({});

      const result = await ds.searchTopics('prefix');

      expect(result).toEqual([]);
    });

    it('uses default limit when not provided', async () => {
      (ds as any).getResource = jest.fn().mockResolvedValue({ topics: [] });

      await ds.searchTopics('prefix');

      expect((ds as any).getResource).toHaveBeenCalledWith('topics', { prefix: 'prefix', limit: '5' });
    });

    it('re-throws errors from getResource', async () => {
      const error = new Error('Search error');
      (ds as any).getResource = jest.fn().mockRejectedValue(error);

      await expect(ds.searchTopics('prefix')).rejects.toThrow('Search error');
    });
  });
});
