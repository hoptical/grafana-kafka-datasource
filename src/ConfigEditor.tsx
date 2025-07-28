import React, { ChangeEvent, useEffect } from 'react';
import { InlineField, Input, Divider, Select, SecretInput, Checkbox, SecretTextArea } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps, SelectableValue } from '@grafana/data';
import {
  ConfigSection,
  DataSourceDescription,
} from '@grafana/plugin-ui';
import { KafkaDataSourceOptions, defaultDataSourceOptions, KafkaSecureJsonData } from './types';
import { defaults } from 'lodash';

interface Props extends DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions> { }

// Security Protocol options
const SECURITY_PROTOCOL_OPTIONS: SelectableValue[] = [
  { label: 'PLAINTEXT', value: 'PLAINTEXT', description: 'No authentication or encryption' },
  { label: 'SSL', value: 'SSL', description: 'SSL encryption without SASL authentication' },
  { label: 'SASL_PLAINTEXT', value: 'SASL_PLAINTEXT', description: 'SASL authentication without encryption' },
  { label: 'SASL_SSL', value: 'SASL_SSL', description: 'SASL authentication with SSL encryption' },
];

// SASL Mechanism options
const SASL_MECHANISM_OPTIONS: SelectableValue[] = [
  { label: 'PLAIN', value: 'PLAIN', description: 'Simple username/password authentication' },
  { label: 'SCRAM-SHA-256', value: 'SCRAM-SHA-256', description: 'SCRAM with SHA-256' },
  { label: 'SCRAM-SHA-512', value: 'SCRAM-SHA-512', description: 'SCRAM with SHA-512' },
];

export const ConfigEditor = (props: Props) => {
  const { options, onOptionsChange } = props;

  // Ensure default values are set
  useEffect(() => {
    const jsonData = defaults(options.jsonData, defaultDataSourceOptions);
    if (JSON.stringify(options.jsonData) !== JSON.stringify(jsonData)) {
      onOptionsChange({ ...options, jsonData });
    }
  }, [options, onOptionsChange]);

  const onBootstrapServersChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      bootstrapServers: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onClientIdChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      clientId: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onSecurityProtocolChange = (option: SelectableValue) => {
    const jsonData = {
      ...options.jsonData,
      securityProtocol: option.value || 'PLAINTEXT',
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onSaslMechanismChange = (option: SelectableValue) => {
    const jsonData = {
      ...options.jsonData,
      saslMechanisms: option.value || '',
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onSaslUsernameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      saslUsername: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onSaslPasswordChange = (event: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...options.secureJsonData,
        saslPassword: event.target.value,
      },
    });
  };

  const onResetSaslPassword = () => {
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

  // TLS Configuration handlers
  const onTlsSkipVerifyChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      tlsSkipVerify: event.target.checked,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onTlsAuthWithCACertChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      tlsAuthWithCACert: event.target.checked,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onTlsClientAuthChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      tlsAuth: event.target.checked,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onServerNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      serverName: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onTlsCACertChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...options.secureJsonData,
        tlsCACert: event.target.value,
      },
    });
  };

  const onResetTlsCACert = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        tlsCACert: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        tlsCACert: '',
      },
    });
  };

  const onTlsClientCertChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...options.secureJsonData,
        tlsClientCert: event.target.value,
      },
    });
  };

  const onResetTlsClientCert = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        tlsClientCert: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        tlsClientCert: '',
      },
    });
  };

  const onTlsClientKeyChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    onOptionsChange({
      ...options,
      secureJsonData: {
        ...options.secureJsonData,
        tlsClientKey: event.target.value,
      },
    });
  };

  const onResetTlsClientKey = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        tlsClientKey: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        tlsClientKey: '',
      },
    });
  };

  const onLogLevelChange = (event: ChangeEvent<HTMLInputElement>) => {
    const jsonData = {
      ...options.jsonData,
      logLevel: event.target.value,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const onHealthcheckTimeoutChange = (event: ChangeEvent<HTMLInputElement>) => {
    const value = parseFloat(event.target.value);
    // Ensure non-negative values only
    const validatedValue = value < 0 ? 0 : value;
    const jsonData = {
      ...options.jsonData,
      healthcheckTimeout: validatedValue,
    };
    onOptionsChange({ ...options, jsonData });
  };

  const jsonData = defaults(options.jsonData, defaultDataSourceOptions);
  const secureJsonData = (options.secureJsonData || {}) as KafkaSecureJsonData;
  const { secureJsonFields } = options;

  // Check if SASL authentication is required
  const isSaslRequired = jsonData.securityProtocol === 'SASL_PLAINTEXT' || jsonData.securityProtocol === 'SASL_SSL';
  
  // Check if TLS is required (SSL or SASL_SSL)
  const isTlsRequired = jsonData.securityProtocol === 'SSL' || jsonData.securityProtocol === 'SASL_SSL';

  return (
    <>
      <DataSourceDescription
        dataSourceName="Kafka"
        docsLink="https://github.com/hamedkarbasi93/grafana-kafka-datasource"
        hasRequiredFields={true}
      />
      
      <Divider spacing={4} />
      
      {/* Connection Settings */}
      <ConfigSection 
        title="Connection" 
        description="Configure your Kafka cluster connection settings"
      >
        <InlineField 
          label="Bootstrap Servers" 
          labelWidth={20} 
          tooltip="Kafka bootstrap servers as CSV: host1:9092,host2:9092"
          grow
          required
        >
          <Input
            id="config-editor-bootstrap-servers"
            onChange={onBootstrapServersChange}
            value={jsonData.bootstrapServers}
            placeholder="broker1:9092,broker2:9092"
            width={40}
          />
        </InlineField>

        <InlineField 
          label="Client ID" 
          labelWidth={20} 
          tooltip="Custom client identifier (optional)"
          grow
        >
          <Input
            id="config-editor-client-id"
            onChange={onClientIdChange}
            value={jsonData.clientId || ''}
            placeholder="my-kafka-client"
            width={40}
          />
        </InlineField>
      </ConfigSection>
      
      <Divider spacing={4} />
      
      {/* Security Protocol */}
      <ConfigSection 
        title="Security Protocol" 
        description="Select and enable security layers"
      >
        <InlineField 
          label="Security Protocol" 
          labelWidth={20} 
          tooltip="Security protocol for Kafka connection"
          grow
          required
        >
          <Select
            options={SECURITY_PROTOCOL_OPTIONS}
            value={SECURITY_PROTOCOL_OPTIONS.find(option => option.value === jsonData.securityProtocol)}
            onChange={onSecurityProtocolChange}
            placeholder="Select security protocol"
            width={40}
          />
        </InlineField>
      </ConfigSection>
      
      <Divider spacing={4} />
      
      {/* Authentication */}
      <ConfigSection 
        title="Authentication" 
        description="Configure authentication settings"
      >
        {isSaslRequired && (
          <>
            <InlineField 
              label="SASL Mechanism" 
              labelWidth={20} 
              tooltip="SASL authentication mechanism"
              grow
              required
            >
              <Select
                options={SASL_MECHANISM_OPTIONS}
                value={SASL_MECHANISM_OPTIONS.find(option => option.value === jsonData.saslMechanisms)}
                onChange={onSaslMechanismChange}
                placeholder="Select SASL mechanism"
                width={40}
              />
            </InlineField>

            <InlineField 
              label="SASL Username" 
              labelWidth={20} 
              tooltip="SASL username for authentication"
              grow
              required
            >
              <Input
                id="config-editor-sasl-username"
                onChange={onSaslUsernameChange}
                value={jsonData.saslUsername}
                placeholder="SASL Username"
                width={40}
              />
            </InlineField>

            <InlineField 
              label="SASL Password" 
              labelWidth={20} 
              tooltip="SASL password for authentication"
              grow
              required
            >
              <SecretInput
                id="config-editor-sasl-password"
                isConfigured={(secureJsonFields && secureJsonFields.saslPassword) as boolean}
                value={secureJsonData.saslPassword || ''}
                placeholder="SASL Password"
                width={40}
                onReset={onResetSaslPassword}
                onChange={onSaslPasswordChange}
              />
            </InlineField>
          </>
        )}

        {/* TLS Settings */}
        {isTlsRequired && (
          <>
            <h4 style={{ marginTop: '20px', marginBottom: '10px' }}>TLS Settings</h4>
            
            <InlineField 
              label="Skip TLS Verification" 
              labelWidth={30} 
              tooltip="Skip TLS certificate validation (not recommended for production)"
              grow
            >
              <Checkbox
                value={jsonData.tlsSkipVerify || false}
                onChange={onTlsSkipVerifyChange}
              />
            </InlineField>

            <InlineField 
              label="Self-signed Certificate" 
              labelWidth={30} 
              tooltip="Enable if using self-signed certificates"
              grow
            >
              <Checkbox
                value={jsonData.tlsAuthWithCACert || false}
                onChange={onTlsAuthWithCACertChange}
              />
            </InlineField>

            {jsonData.tlsAuthWithCACert && (
              <div style={{ marginLeft: '40px' }}>
                <InlineField 
                  label="CA Certificate" 
                  labelWidth={30} 
                  tooltip="Certificate Authority certificate"
                  htmlFor="config-editor-tls-ca-cert"
                  interactive
                  grow
                >
                  <SecretTextArea
                    id="config-editor-tls-ca-cert"
                    isConfigured={(secureJsonFields && secureJsonFields.tlsCACert) as boolean}
                    onReset={onResetTlsCACert}
                    onChange={(e) => onTlsCACertChange(e as ChangeEvent<HTMLTextAreaElement>)}
                    placeholder="Begins with -----BEGIN CERTIFICATE-----"
                    rows={6}
                  />
                </InlineField>
              </div>
            )}

            <InlineField 
              label="TLS Client Authentication" 
              labelWidth={30} 
              tooltip="Enable TLS client authentication"
              grow
            >
              <Checkbox
                value={jsonData.tlsAuth || false}
                onChange={onTlsClientAuthChange}
              />
            </InlineField>

            {jsonData.tlsAuth && (
              <div style={{ marginLeft: '40px' }}>
                <InlineField 
                  label="Server Name" 
                  labelWidth={30} 
                  tooltip="Server name for TLS validation"
                  grow
                  required
                >
                  <Input
                    id="config-editor-server-name"
                    onChange={onServerNameChange}
                    value={jsonData.serverName || ''}
                    placeholder="domain.example.com"
                    width={40}
                  />
                </InlineField>

                <InlineField 
                  label="Client Certificate" 
                  labelWidth={30} 
                  tooltip="TLS client certificate"
                  htmlFor="client-auth-client-certificate-input"
                  interactive
                  grow
                >
                  <SecretTextArea
                    id="client-auth-client-certificate-input"
                    isConfigured={(secureJsonFields && secureJsonFields.tlsClientCert) as boolean}
                    onReset={onResetTlsClientCert}
                    onChange={(e) => onTlsClientCertChange(e as ChangeEvent<HTMLTextAreaElement>)}
                    placeholder="Begins with -----BEGIN CERTIFICATE-----"
                    rows={6}
                  />
                </InlineField>

                <InlineField 
                  label="Client Key" 
                  labelWidth={30} 
                  tooltip="TLS client private key"
                  htmlFor="config-editor-tls-client-key"
                  interactive
                  grow
                  required
                >
                  <SecretTextArea
                    id="config-editor-tls-client-key"
                    isConfigured={(secureJsonFields && secureJsonFields.tlsClientKey) as boolean}
                    onReset={onResetTlsClientKey}
                    onChange={(e) => onTlsClientKeyChange(e as ChangeEvent<HTMLTextAreaElement>)}
                    placeholder="Begins with -----BEGIN PRIVATE KEY-----"
                    rows={6}
                  />
                </InlineField>
              </div>
            )}
          </>
        )}
      </ConfigSection>
      
      <Divider spacing={4} />
      {/* Advanced Settings */}
      <ConfigSection
        title="Advanced Settings"
        description="Additional settings for debugging and performance tuning."
        isCollapsible={true}
        isInitiallyOpen={false}
      >
        <InlineField 
          label="Log Level" 
          labelWidth={24} 
          tooltip="Logging level for debugging"
          grow
        >
          <Input
            id="config-editor-log-level"
            onChange={onLogLevelChange}
            value={jsonData.logLevel}
            placeholder="debug | info | warn | error"
            width={40}
          />
        </InlineField>

        <InlineField 
          label="Healthcheck Timeout (ms)" 
          labelWidth={24} 
          tooltip="Timeout for health check in milliseconds (non-negative values only)"
          grow
        >
          <Input
            id="config-editor-healthcheck-timeout"
            onChange={onHealthcheckTimeoutChange}
            value={jsonData.healthcheckTimeout}
            type="number"
            step={1}
            min={0}
            width={40}
          />
        </InlineField>
      </ConfigSection>
    </>
  );
};
