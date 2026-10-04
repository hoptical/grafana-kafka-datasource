import React, { ChangeEvent } from 'react';
import { InlineField, Input, SecretInput, SecretTextArea, TextArea, RadioButtonGroup, Checkbox } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { KafkaDataSourceOptions, KafkaSecureJsonData, GssapiAuthType } from './types';

interface Props {
  options: DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions, KafkaSecureJsonData>['options'];
  onOptionsChange: DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions, KafkaSecureJsonData>['onOptionsChange'];
}

const AUTH_TYPE_OPTIONS = [
  { label: 'Password', value: GssapiAuthType.PASSWORD },
  { label: 'Keytab', value: GssapiAuthType.KEYTAB },
];

// GssapiFields renders the SASL/GSSAPI (Kerberos) configuration block. It is
// factored out of ConfigEditor.tsx because, unlike the OAUTHBEARER fields it
// sits alongside, GSSAPI needs ~10 fields; inlining that into
// ConfigEditor's per-mechanism ternary would make it unreadable.
//
// krb5.conf and the keytab are only accepted as pasted content (the keytab
// base64-encoded, stored encrypted). File paths are deliberately not
// supported: the plugin must not read files from the Grafana host.
export function GssapiFields({ options, onOptionsChange }: Props) {
  const { jsonData, secureJsonData = {}, secureJsonFields } = options;

  const updateJsonData = (patch: Partial<KafkaDataSourceOptions>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, ...patch } });
  };

  const updateSecureJsonData = (patch: Partial<KafkaSecureJsonData>) => {
    onOptionsChange({ ...options, secureJsonData: { ...secureJsonData, ...patch } });
  };

  const onServiceNameChange = (e: ChangeEvent<HTMLInputElement>) =>
    updateJsonData({ saslGssapiServiceName: e.target.value });
  const onRealmChange = (e: ChangeEvent<HTMLInputElement>) => updateJsonData({ saslGssapiRealm: e.target.value });
  const onUsernameChange = (e: ChangeEvent<HTMLInputElement>) => updateJsonData({ saslGssapiUsername: e.target.value });

  const onKrb5ConfigChange = (e: ChangeEvent<HTMLTextAreaElement>) =>
    updateJsonData({ saslGssapiKrb5Config: e.target.value });

  const onAuthTypeChange = (value: GssapiAuthType) => updateJsonData({ saslGssapiAuthType: value });

  const onPasswordChange = (e: ChangeEvent<HTMLInputElement>) =>
    updateSecureJsonData({ saslGssapiPassword: e.target.value });
  const onResetPassword = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: { ...secureJsonFields, saslGssapiPassword: false },
      secureJsonData: { ...secureJsonData, saslGssapiPassword: '' },
    });
  };

  const onKeytabChange = (e: ChangeEvent<HTMLTextAreaElement>) =>
    updateSecureJsonData({ saslGssapiKeytab: e.target.value });
  const onResetKeytab = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: { ...secureJsonFields, saslGssapiKeytab: false },
      secureJsonData: { ...secureJsonData, saslGssapiKeytab: '' },
    });
  };

  const onDisablePAFXFASTChange = (e: ChangeEvent<HTMLInputElement>) =>
    updateJsonData({ saslGssapiDisablePAFXFAST: e.target.checked });

  const authType = jsonData.saslGssapiAuthType || GssapiAuthType.PASSWORD;

  return (
    <>
      <InlineField label="Service Name" labelWidth={30} tooltip="Kerberos service name for the Kafka SPN" grow required>
        <Input
          id="config-editor-gssapi-service-name"
          data-testid="gssapi-service-name"
          onChange={onServiceNameChange}
          value={jsonData.saslGssapiServiceName || ''}
          placeholder="kafka"
          width={40}
        />
      </InlineField>

      <InlineField
        label="Realm"
        labelWidth={30}
        tooltip="Kerberos realm, exactly as it appears in krb5.conf"
        grow
        required
      >
        <Input
          id="config-editor-gssapi-realm"
          data-testid="gssapi-realm"
          onChange={onRealmChange}
          value={jsonData.saslGssapiRealm || ''}
          placeholder="EXAMPLE.COM"
          width={40}
        />
      </InlineField>

      <InlineField
        label="Principal"
        labelWidth={30}
        tooltip="Kerberos principal/username, without the @REALM suffix"
        grow
        required
      >
        <Input
          id="config-editor-gssapi-username"
          data-testid="gssapi-username"
          onChange={onUsernameChange}
          value={jsonData.saslGssapiUsername || ''}
          placeholder="grafana"
          width={40}
        />
      </InlineField>

      <InlineField
        label="krb5.conf Content"
        labelWidth={30}
        tooltip="Contents of krb5.conf, pasted directly (not a secret)"
        htmlFor="config-editor-gssapi-krb5-config"
        interactive
        grow
        required
      >
        <TextArea
          id="config-editor-gssapi-krb5-config"
          data-testid="gssapi-krb5-config"
          onChange={onKrb5ConfigChange}
          value={jsonData.saslGssapiKrb5Config || ''}
          placeholder="[libdefaults]&#10;  default_realm = EXAMPLE.COM"
          rows={6}
        />
      </InlineField>

      <InlineField label="Authentication Method" labelWidth={30} tooltip="How to authenticate the principal" grow>
        <RadioButtonGroup
          data-testid="gssapi-auth-type"
          options={AUTH_TYPE_OPTIONS}
          value={authType}
          onChange={(value) => value && onAuthTypeChange(value)}
        />
      </InlineField>

      {authType === GssapiAuthType.KEYTAB ? (
        <InlineField
          label="Keytab Content"
          labelWidth={30}
          tooltip="Base64-encoded keytab file content, e.g. the output of `base64 -w0 grafana.keytab` (stored encrypted)"
          htmlFor="config-editor-gssapi-keytab"
          interactive
          grow
          required
        >
          <SecretTextArea
            id="config-editor-gssapi-keytab"
            data-testid="gssapi-keytab"
            isConfigured={(secureJsonFields && secureJsonFields.saslGssapiKeytab) as boolean}
            onReset={onResetKeytab}
            onChange={(e) => onKeytabChange(e as ChangeEvent<HTMLTextAreaElement>)}
            placeholder="Base64-encoded keytab content"
            rows={4}
          />
        </InlineField>
      ) : (
        <InlineField label="Password" labelWidth={30} tooltip="Kerberos password for the principal" grow required>
          <SecretInput
            id="config-editor-gssapi-password"
            data-testid="gssapi-password"
            isConfigured={(secureJsonFields && secureJsonFields.saslGssapiPassword) as boolean}
            value={secureJsonData.saslGssapiPassword || ''}
            placeholder="Kerberos Password"
            width={40}
            onReset={onResetPassword}
            onChange={onPasswordChange}
          />
        </InlineField>
      )}

      <InlineField
        label="Disable PA-FX-FAST"
        labelWidth={30}
        tooltip="Disable Kerberos pre-authentication padata (PA_REQ_ENC_PA_REP). Some Active Directory and older MIT KDCs reject this padata by default."
        grow
      >
        <Checkbox
          data-testid="gssapi-disable-pafxfast"
          value={jsonData.saslGssapiDisablePAFXFAST || false}
          onChange={onDisablePAFXFASTChange}
        />
      </InlineField>
    </>
  );
}
