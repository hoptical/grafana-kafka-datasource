import React, { ChangeEvent, PureComponent } from 'react';
import { LegacyForms } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { KafkaDataSourceOptions, KafkaSecureJsonData } from './types';

const { SecretFormField, FormField } = LegacyForms;

interface Props extends DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions> {}

interface State {}

export class ConfigEditor extends PureComponent<Props, State> {
  onBootstrapServersChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      bootstrapServers: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onSecurityProtocolChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      securityProtocol: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onSaslMechanismsChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      saslMechanisms: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onSaslUsernameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      saslUsername: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onSaslPasswordChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonData: {
        saslPassword: event.target.value,
      },
    });
  };

  onResetSaslPassword = () => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        saslPassword: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        saslPassword: '',
      },
    });
  };

  onDebugChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      debug: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onHealthcheckTimeoutChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      healthcheckTimeout: parseFloat(event.target.value),
    };
    onOptionsChange({ ...options, jsonData });
  };

  render() {
    const { options } = this.props;
    const { jsonData, secureJsonFields } = options;
    const secureJsonData = (options.secureJsonData || {}) as KafkaSecureJsonData;

    return (
      <div className="gf-form-group">
        <div className="gf-form">
          <FormField
            label="Servers"
            labelWidth={11}
            onChange={this.onBootstrapServersChange}
            value={jsonData.bootstrapServers || ''}
            placeholder="broker1:9092, broker2:9092"
            inputWidth={30}
          />
        </div>

        <div className="gf-form">
          <FormField
            label="Security Protocol"
            labelWidth={11}
            onChange={this.onSecurityProtocolChange}
            value={jsonData.securityProtocol || ''}
            placeholder="<PLAINTEXT|SASL_SSL>"
          />
        </div>

        <div className="gf-form">
          <FormField
            label="SASL Mechanisms"
            labelWidth={11}
            onChange={this.onSaslMechanismsChange}
            value={jsonData.saslMechanisms || ''}
            placeholder="<PLAIN|SCRAM-SHA-512>"
          />
        </div>

        <div className="gf-form">
          <FormField
            label="SASL Username"
            labelWidth={11}
            onChange={this.onSaslUsernameChange}
            value={jsonData.saslUsername || ''}
            placeholder="<SASL Username>"
          />
        </div>

        <div className="gf-form-inline">
          <div className="gf-form">
            <SecretFormField
              isConfigured={(secureJsonFields && secureJsonFields.saslPassword) as boolean}
              value={secureJsonData.saslPassword || ''}
              label="SASL Password"
              placeholder="<SASL Password>"
              labelWidth={11}
              inputWidth={20}
              onReset={this.onResetSaslPassword}
              onChange={this.onSaslPasswordChange}
            />
          </div>
        </div>

        <div className="gf-form">
          <FormField
            label="Debug"
            labelWidth={11}
            onChange={this.onDebugChange}
            value={jsonData.debug || ''}
            placeholder="<librdkafka Debug Level>"
          />
        </div>

        <div className="gf-form">
          <FormField
            label="Healthcheck Timeout (ms)"
            labelWidth={11}
            onChange={this.onHealthcheckTimeoutChange}
            value={jsonData.healthcheckTimeout || 2000}
            type="number"
            step="1"
            min="0"
          />
        </div>
      </div>
    );
  }
}
