import React, { ChangeEvent, PureComponent } from 'react';
import { InlineField, Input, SecretInput } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { KafkaDataSourceOptions, KafkaSecureJsonData, defaultDataSourceOptions } from './types';
import { defaults } from 'lodash';

interface Props extends DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions> { }

interface State { }

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

  onLogLevelChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      logLevel: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  onHealthcheckTimeoutChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const value = parseFloat(event.target.value);
    // Ensure non-negative values only
    const validatedValue = value < 0 ? 0 : value;
    const jsonData = {
      ...options.jsonData,
      healthcheckTimeout: validatedValue,
    };
    onOptionsChange({ ...options, jsonData });
  };

  /* apiKey is deprecated and will be removed in future versions */
  onAPIKeyChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...options.secureJsonData,
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

  render() {
    const { options } = this.props;
    const { secureJsonFields } = options;
    const jsonData = defaults(options.jsonData, defaultDataSourceOptions);
    const secureJsonData = (options.secureJsonData || {}) as KafkaSecureJsonData;

    // Define label widths for different sections
    const connectionLabelWidth = 20;
    const authenticationLabelWidth = 25;
    const advancedLabelWidth = 30;

    return (
      <div className="gf-form-group">
        {/* Connection Configuration Section */}
        <h3 className="page-heading">Connection</h3>

        <InlineField label="Bootstrap Servers" labelWidth={connectionLabelWidth} tooltip="Kafka bootstrap servers separated by commas">
          <Input
            id="config-editor-servers"
            onChange={this.onBootstrapServersChange}
            value={jsonData.bootstrapServers}
            placeholder="broker1:9092, broker2:9092"
            width={40}
          />
        </InlineField>

        {/* Authentication Section */}
        <h3 className="page-heading" style={{ marginTop: '32px' }}>Authentication</h3>
        <InlineField label="Security Protocol" labelWidth={connectionLabelWidth} tooltip="Security protocol for Kafka connection">
          <Input
            id="config-editor-security-protocol"
            onChange={this.onSecurityProtocolChange}
            value={jsonData.securityProtocol}
            placeholder="PLAINTEXT or SASL_SSL"
            width={40}
          />
        </InlineField>

        <InlineField label="SASL Mechanisms" labelWidth={authenticationLabelWidth} tooltip="SASL authentication mechanism">
          <Input
            id="config-editor-sasl-mechanisms"
            onChange={this.onSaslMechanismsChange}
            value={jsonData.saslMechanisms}
            placeholder="PLAIN or SCRAM-SHA-512"
            width={40}
          />
        </InlineField>

        <InlineField label="SASL Username" labelWidth={authenticationLabelWidth} tooltip="SASL username for authentication">
          <Input
            id="config-editor-sasl-username"
            onChange={this.onSaslUsernameChange}
            value={jsonData.saslUsername}
            placeholder="SASL Username"
            width={40}
          />
        </InlineField>

        <InlineField label="SASL Password" labelWidth={authenticationLabelWidth} tooltip="SASL password for authentication">
          <SecretInput
            id="config-editor-sasl-password"
            isConfigured={(secureJsonFields && secureJsonFields.saslPassword) as boolean}
            value={secureJsonData.saslPassword || ''}
            placeholder="SASL Password"
            width={40}
            onReset={this.onResetSaslPassword}
            onChange={this.onSaslPasswordChange}
          />
        </InlineField>

        <InlineField label="API Key (Deprecated)" labelWidth={authenticationLabelWidth} tooltip="Deprecated API key field">
          <SecretInput
            id="config-editor-api-key"
            isConfigured={(secureJsonFields && secureJsonFields.apiKey) as boolean}
            value={secureJsonData.apiKey || ''}
            placeholder="secure json field (backend only)"
            width={40}
            onReset={this.onResetAPIKey}
            onChange={this.onAPIKeyChange}
          />
        </InlineField>

        {/* Advanced Settings Section */}
        <h3 className="page-heading" style={{ marginTop: '32px' }}>Advanced Settings</h3>

        <InlineField label="Log Level" labelWidth={advancedLabelWidth} tooltip="Logging level for debugging">
          <Input
            id="config-editor-log-level"
            onChange={this.onLogLevelChange}
            value={jsonData.logLevel}
            placeholder="debug or error"
            width={40}
          />
        </InlineField>

        <InlineField label="Healthcheck Timeout (ms)" labelWidth={advancedLabelWidth} tooltip="Timeout for health check in milliseconds (non-negative values only)">
          <Input
            id="config-editor-healthcheck-timeout"
            onChange={this.onHealthcheckTimeoutChange}
            value={jsonData.healthcheckTimeout}
            type="number"
            step={1}
            min={0}
            width={40}
          />
        </InlineField>
      </div>
    );
  }
}
