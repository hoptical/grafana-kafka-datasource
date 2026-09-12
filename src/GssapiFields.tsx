import React, { ChangeEvent, useState } from 'react';
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

const SOURCE_OPTIONS = [
  { label: 'Paste content', value: 'inline' as const },
  { label: 'File path', value: 'path' as const },
];

// GssapiFields renders the SASL/GSSAPI (Kerberos) configuration block. It is
// factored out of ConfigEditor.tsx because, unlike the OAUTHBEARER fields it
// sits alongside, GSSAPI needs ~10 fields plus two independent
// inline-content-vs-file-path toggles (krb5.conf and, for keytab auth, the
// keytab); inlining that into ConfigEditor's per-mechanism ternary would
// make it unreadable.
export function GssapiFields({ options, onOptionsChange }: Props) {
  const { jsonData, secureJsonData = {}, secureJsonFields } = options;

  // Which source (pasted content vs. file path) is shown for each of
  // krb5.conf and the keytab. This is UI-only state, not persisted: on
  // load, default to "path" only when a path is already set and there is no
  // inline content, so existing path-based configurations still render
  // correctly; otherwise default to "paste content".
  const [krb5ConfigSource, setKrb5ConfigSource] = useState<'inline' | 'path'>(
    !jsonData.saslGssapiKrb5Config && jsonData.saslGssapiKrb5ConfigPath ? 'path' : 'inline'
  );
  const [keytabSource, setKeytabSource] = useState<'inline' | 'path'>(
    !secureJsonFields?.saslGssapiKeytab && jsonData.saslGssapiKeytabPath ? 'path' : 'inline'
  );

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
  const onKrb5ConfigPathChange = (e: ChangeEvent<HTMLInputElement>) =>
    updateJsonData({ saslGssapiKrb5ConfigPath: e.target.value });

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
  const onKeytabPathChange = (e: ChangeEvent<HTMLInputElement>) =>
    updateJsonData({ saslGssapiKeytabPath: e.target.value });

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
        label="krb5.conf Source"
        labelWidth={30}
        tooltip="How the Kerberos configuration file is provided"
        grow
      >
        <RadioButtonGroup
          data-testid="gssapi-krb5-config-source"
          options={SOURCE_OPTIONS}
          value={krb5ConfigSource}
          onChange={(value) => value && setKrb5ConfigSource(value)}
        />
      </InlineField>

      {krb5ConfigSource === 'inline' ? (
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
      ) : (
        <InlineField
          label="krb5.conf Path"
          labelWidth={30}
          tooltip="Path to krb5.conf on the Grafana backend host"
          grow
          required
        >
          <Input
            id="config-editor-gssapi-krb5-config-path"
            data-testid="gssapi-krb5-config-path"
            onChange={onKrb5ConfigPathChange}
            value={jsonData.saslGssapiKrb5ConfigPath || ''}
            placeholder="/etc/krb5.conf"
            width={40}
          />
        </InlineField>
      )}

      <InlineField label="Authentication Method" labelWidth={30} tooltip="How to authenticate the principal" grow>
        <RadioButtonGroup
          data-testid="gssapi-auth-type"
          options={AUTH_TYPE_OPTIONS}
          value={authType}
          onChange={(value) => value && onAuthTypeChange(value)}
        />
      </InlineField>

      {authType === GssapiAuthType.KEYTAB ? (
        <>
          <InlineField label="Keytab Source" labelWidth={30} tooltip="How the keytab is provided" grow>
            <RadioButtonGroup
              data-testid="gssapi-keytab-source"
              options={SOURCE_OPTIONS}
              value={keytabSource}
              onChange={(value) => value && setKeytabSource(value)}
            />
          </InlineField>

          {keytabSource === 'inline' ? (
            <InlineField
              label="Keytab Content"
              labelWidth={30}
              tooltip="Base64-encoded keytab file content (stored encrypted)"
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
            <InlineField
              label="Keytab Path"
              labelWidth={30}
              tooltip="Path to the keytab file on the Grafana backend host"
              grow
              required
            >
              <Input
                id="config-editor-gssapi-keytab-path"
                data-testid="gssapi-keytab-path"
                onChange={onKeytabPathChange}
                value={jsonData.saslGssapiKeytabPath || ''}
                placeholder="/etc/security/keytabs/grafana.keytab"
                width={40}
              />
            </InlineField>
          )}
        </>
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
