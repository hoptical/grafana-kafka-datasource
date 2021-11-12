import React, { ChangeEvent, PureComponent } from 'react';
import { LegacyForms, InlineFormLabel } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import { KafkaOptions, KafkaSecureJsonData, AutoOffsetReset } from './types';

const { SecretFormField, FormField, Select } = LegacyForms;

const autoResetOffsets = [
  {
    label: 'Earliest',
    value: AutoOffsetReset.EARLIEST,
    description: 'Automatically reset the offset to the earliest offset'
  },
  {
    label: 'Latest',
    value: AutoOffsetReset.LATEST,
    description: 'Automatically reset the offset to the latest offset'
  },
  {
    label: 'None',
    value: AutoOffsetReset.NONE,
    description: 'Throw exception to the consumer if no previous offset is found for the consumer\'s group'
  },
] as Array<SelectableValue<AutoOffsetReset>>;

interface Props extends DataSourcePluginOptionsEditorProps<KafkaOptions> { }

interface State { }

export class ConfigEditor extends PureComponent<Props, State> {

  // Secure field (only sent to the backend)
  onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonData: {
        apiKey: event.target.value,
      },
    });
  };

  onResetAPIKey = () => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        apiKey: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        apiKey: '',
      },
    });
  };

  onBootstrapServersChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      bootstrapServers: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onAutoResetOffsetChanged = (selected: SelectableValue<AutoOffsetReset>) => {
    const { options, onOptionsChange } = this.props;

    const jsonData = {
      ...options.jsonData,
      autoOffsetReset: selected.value || AutoOffsetReset.EARLIEST,
    };
    onOptionsChange({ ...options, jsonData });
  };


  resolveAutoResetOffset = (value: string | undefined) => {
    if (value === AutoOffsetReset.LATEST) {
      return autoResetOffsets[1];
    }
    if (value === AutoOffsetReset.NONE) {
      return autoResetOffsets[3];
    }
    return autoResetOffsets[0];
  };

  render() {
    const { options } = this.props;
    const { jsonData, secureJsonFields } = options;
    const secureJsonData = (options.secureJsonData || {}) as KafkaSecureJsonData;

    return (
      <>
        <h3 className="page-heading">Kafka connections</h3>
        <div className="gf-form-group">
          <div className="gf-form">
            <FormField
              label="Bootstrap Servers"
              onChange={this.onBootstrapServersChange}
              value={jsonData.bootstrapServers || ''}
              placeholder="broker1:9092, broker2:9092"
              labelWidth={10}
              inputWidth={15}
            />
          </div>
          <div className="gf-form">
            <InlineFormLabel
              className="width-10"
              tooltip="What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted)"
            >
              Auto offset reset
            </InlineFormLabel>
            <Select
              className="width-15"
              value={this.resolveAutoResetOffset(jsonData.autoOffsetReset)}
              options={autoResetOffsets}
              defaultValue={autoResetOffsets[0]}
              onChange={this.onAutoResetOffsetChanged}
            />
          </div>
        </div>
        <h3 className="page-heading">Auth</h3>
        <div className="gf-form-group">
          <div className="gf-form-inline">
            <div className="gf-form">
              <SecretFormField
                isConfigured={(secureJsonFields && secureJsonFields.apiKey) as boolean}
                value={secureJsonData.apiKey || ''}
                label="API Key"
                placeholder="secure json field (backend only)"
                labelWidth={6}
                inputWidth={20}
                onReset={this.onResetAPIKey}
                onChange={this.onAPIKeyChange}
              />
            </div>
          </div>
        </div>
      </>
    );
  }
}
