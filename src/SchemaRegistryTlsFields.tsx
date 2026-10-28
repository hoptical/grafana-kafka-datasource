import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretTextArea, RadioButtonGroup, Checkbox } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { KafkaDataSourceOptions, KafkaSecureJsonData, SchemaRegistryTlsMode } from './types';

interface Props {
  options: DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions, KafkaSecureJsonData>['options'];
  onOptionsChange: DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions, KafkaSecureJsonData>['onOptionsChange'];
}

const TLS_MODE_OPTIONS = [
  {
    label: 'Same as Kafka',
    value: SchemaRegistryTlsMode.KAFKA,
    description: 'Reuse the CA and client certificate from the Kafka TLS settings',
  },
  {
    label: 'Custom',
    value: SchemaRegistryTlsMode.CUSTOM,
    description: 'Use separate certificates for the Schema Registry',
  },
];

type SecureKey = 'schemaRegistryTlsCACert' | 'schemaRegistryTlsClientCert' | 'schemaRegistryTlsClientKey';

// SchemaRegistryTlsFields renders the TLS settings for the Schema Registry
// HTTP client. In "Same as Kafka" mode (the default, and the behaviour before
// this option existed) the backend reuses the Kafka TLS fields; "Custom" lets
// a registry with its own CA or mTLS client certificate be configured
// independently of the brokers.
export function SchemaRegistryTlsFields({ options, onOptionsChange }: Props) {
  const { jsonData, secureJsonData = {}, secureJsonFields } = options;
  const mode = jsonData.schemaRegistryTlsMode || SchemaRegistryTlsMode.KAFKA;

  const updateJsonData = (patch: Partial<KafkaDataSourceOptions>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, ...patch } });
  };

  const onSecureChange = (key: SecureKey) => (e: ChangeEvent<HTMLTextAreaElement>) => {
    onOptionsChange({ ...options, secureJsonData: { ...secureJsonData, [key]: e.target.value } });
  };

  const onSecureReset = (key: SecureKey) => () => {
    onOptionsChange({
      ...options,
      secureJsonFields: { ...secureJsonFields, [key]: false },
      secureJsonData: { ...secureJsonData, [key]: '' },
    });
  };

  return (
    <>
      <InlineField
        label="TLS"
        labelWidth={25}
        tooltip="TLS settings used when connecting to the Schema Registry over HTTPS"
        grow
      >
        <RadioButtonGroup
          data-testid="schema-registry-tls-mode"
          options={TLS_MODE_OPTIONS}
          value={mode}
          onChange={(value) => value && updateJsonData({ schemaRegistryTlsMode: value })}
        />
      </InlineField>

      {mode === SchemaRegistryTlsMode.CUSTOM && (
        <>
          <InlineField
            label="Skip TLS Verification"
            labelWidth={25}
            tooltip="Skip Schema Registry certificate validation (not recommended for production)"
          >
            <Checkbox
              id="config-editor-schema-registry-tls-skip-verify"
              value={jsonData.schemaRegistryTlsSkipVerify || false}
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                updateJsonData({ schemaRegistryTlsSkipVerify: e.target.checked })
              }
            />
          </InlineField>

          <InlineField label="Server Name" labelWidth={25} tooltip="Server name for TLS validation (optional)" grow>
            <Input
              id="config-editor-schema-registry-tls-server-name"
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                updateJsonData({ schemaRegistryTlsServerName: e.target.value })
              }
              value={jsonData.schemaRegistryTlsServerName || ''}
              placeholder="registry.example.com"
              width={40}
            />
          </InlineField>

          <InlineField
            label="CA Certificate"
            labelWidth={25}
            tooltip="PEM CA certificate used to verify the Schema Registry (optional; system CAs are used when empty)"
            htmlFor="config-editor-schema-registry-tls-ca-cert"
            interactive
            grow
          >
            <SecretTextArea
              id="config-editor-schema-registry-tls-ca-cert"
              isConfigured={(secureJsonFields && secureJsonFields.schemaRegistryTlsCACert) as boolean}
              onReset={onSecureReset('schemaRegistryTlsCACert')}
              onChange={(e) => onSecureChange('schemaRegistryTlsCACert')(e as ChangeEvent<HTMLTextAreaElement>)}
              placeholder="Begins with -----BEGIN CERTIFICATE-----"
              rows={6}
            />
          </InlineField>

          <InlineField
            label="Client Certificate"
            labelWidth={25}
            tooltip="PEM client certificate for mTLS (optional; requires a client key)"
            htmlFor="config-editor-schema-registry-tls-client-cert"
            interactive
            grow
          >
            <SecretTextArea
              id="config-editor-schema-registry-tls-client-cert"
              isConfigured={(secureJsonFields && secureJsonFields.schemaRegistryTlsClientCert) as boolean}
              onReset={onSecureReset('schemaRegistryTlsClientCert')}
              onChange={(e) => onSecureChange('schemaRegistryTlsClientCert')(e as ChangeEvent<HTMLTextAreaElement>)}
              placeholder="Begins with -----BEGIN CERTIFICATE-----"
              rows={6}
            />
          </InlineField>

          <InlineField
            label="Client Key"
            labelWidth={25}
            tooltip="PEM private key for the client certificate (optional; requires a client certificate)"
            htmlFor="config-editor-schema-registry-tls-client-key"
            interactive
            grow
          >
            <SecretTextArea
              id="config-editor-schema-registry-tls-client-key"
              isConfigured={(secureJsonFields && secureJsonFields.schemaRegistryTlsClientKey) as boolean}
              onReset={onSecureReset('schemaRegistryTlsClientKey')}
              onChange={(e) => onSecureChange('schemaRegistryTlsClientKey')(e as ChangeEvent<HTMLTextAreaElement>)}
              placeholder="Begins with -----BEGIN PRIVATE KEY-----"
              rows={6}
            />
          </InlineField>
        </>
      )}
    </>
  );
}
