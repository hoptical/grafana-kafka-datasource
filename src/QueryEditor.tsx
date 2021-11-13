import { defaults } from 'lodash';
import React from 'react';
import { Form, Field, Input, Switch } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from './datasource';
import { defaultQuery, KafkaDataSourceOptions, KafkaQuery } from './types';
import { handlerFactory } from 'handleEvent';

type Props = QueryEditorProps<DataSource, KafkaQuery, KafkaDataSourceOptions>;

export const QueryEditor = (props: Props) => {
  const { onChange } = props;
  const query = defaults(props.query, defaultQuery);
  const handleEvent = handlerFactory(query, onChange);
  const { topicName, partition, withStreaming } = query;

  return (
    <Form onSubmit={() => { }}>
      {() => (
        <div className="gf-form">
          <div className="m-r-1 m-t-1">
            <Field label="Topic Name">
              <Input
                name="topicName"
                value={topicName || ''}
                onChange={handleEvent('topicName')}
                type="text"
                css=""
                autoComplete="off"
                required
              />
            </Field>
          </div>
          <div className="m-r-1 m-t-1">
            <Field label="Partition">
              <Input
                name="partition"
                value={partition}
                onChange={handleEvent('partition')}
                type="number"
                step="1"
                min="0"
                css=""
                autoComplete="off"
                required
              />
            </Field>
          </div>
          <div className="m-r-1 m-t-1">
            <Field label="Enable streaming (v8+)">
              <Switch name="withStreaming" checked={withStreaming || false} onChange={handleEvent('withStreaming')} css="" />
            </Field>
          </div>
        </div>
      )}
    </Form>
  );
}
