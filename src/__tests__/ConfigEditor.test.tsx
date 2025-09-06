import React from 'react';
import { render, screen, fireEvent } from '@testing-library/react';
import { ConfigEditor } from '../ConfigEditor';
import { defaultDataSourceOptions, type KafkaDataSourceOptions, type KafkaSecureJsonData } from '../types';
import type { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { deepFreeze } from '../test-utils/test-helpers';

// Mock @grafana/ui components
jest.mock('@grafana/ui', () => ({
  InlineField: ({ children, label, required }: any) => (
    <div data-testid="inline-field" aria-label={label} data-required={required}>
      {children}
    </div>
  ),
  Input: (props: any) => <input {...props} />,
  Divider: ({ spacing }: any) => <div data-testid="divider" data-spacing={spacing} />,
  SecretInput: ({ value, onChange, onReset, isConfigured, placeholder, id }: any) => (
    <div>
      <input id={id} value={value} onChange={onChange} placeholder={placeholder} data-testid="secret-input" />
      {isConfigured && (
        <button onClick={onReset} data-testid="reset-button">
          Reset
        </button>
      )}
    </div>
  ),
  Checkbox: ({ value, onChange }: any) => (
    <input type="checkbox" checked={value} onChange={onChange} data-testid="checkbox" />
  ),
  SecretTextArea: ({ value, onChange, onReset, isConfigured, placeholder, id, rows }: any) => (
    <div>
      <textarea
        id={id}
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        rows={rows}
        data-testid="secret-textarea"
      />
      {isConfigured && (
        <button onClick={onReset} data-testid="reset-textarea-button">
          Reset
        </button>
      )}
    </div>
  ),
  Select: ({ value, onChange, options, placeholder }: any) => (
    <select
      value={value?.value ? String(value.value) : ''}
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
}));

// Mock @grafana/plugin-ui components
jest.mock('@grafana/plugin-ui', () => ({
  ConfigSection: ({ title, description, children, isCollapsible, isInitiallyOpen }: any) => (
    <div
      data-testid="config-section"
      data-title={title}
      data-collapsible={isCollapsible}
      data-initially-open={isInitiallyOpen}
    >
      <h3>{title}</h3>
      {description && <p>{description}</p>}
      {children}
    </div>
  ),
  DataSourceDescription: ({ dataSourceName, docsLink, hasRequiredFields }: any) => (
    <div
      data-testid="datasource-description"
      data-name={dataSourceName}
      data-docs={docsLink}
      data-required={hasRequiredFields}
    >
      Data source: {dataSourceName}
    </div>
  ),
}));

// Mock lodash
jest.mock('lodash', () => ({
  defaults: (obj: any, defaults: any) => ({ ...defaults, ...obj }),
  isEqual: (a: any, b: any) => JSON.stringify(a) === JSON.stringify(b),
}));

const mockOnOptionsChange = jest.fn();

const createMockOptions = (
  jsonData?: Partial<KafkaDataSourceOptions>,
  secureJsonData?: Partial<KafkaSecureJsonData>,
  secureJsonFields?: any,
  freeze = false
): DataSourcePluginOptionsEditorProps<KafkaDataSourceOptions>['options'] => {
  const options = {
    id: 1,
    uid: 'test-uid',
    orgId: 1,
    name: 'Test Kafka',
    type: 'kafka',
    access: 'proxy',
    url: '',
    basicAuth: false,
    basicAuthUser: '',
    database: '',
    user: '',
    isDefault: false,
    readOnly: false,
    withCredentials: false,
    typeName: 'kafka',
    typeLogoUrl: '',
    jsonData: { ...defaultDataSourceOptions, ...jsonData } as KafkaDataSourceOptions,
    secureJsonData: secureJsonData || {},
    secureJsonFields: secureJsonFields || {},
    version: 1,
  };
  return freeze ? deepFreeze(options) : options;
};

const renderConfigEditor = (
  jsonData?: Partial<KafkaDataSourceOptions>,
  secureJsonData?: Partial<KafkaSecureJsonData>,
  secureJsonFields?: any
) => {
  const options = createMockOptions(jsonData, secureJsonData, secureJsonFields);
  return render(<ConfigEditor options={options} onOptionsChange={mockOnOptionsChange} />);
};

beforeEach(() => {
  jest.clearAllMocks();
});

describe('ConfigEditor', () => {
  it('does not mutate frozen props', () => {
    const frozenOptions = createMockOptions({}, {}, {}, true);
    expect(() => {
      render(<ConfigEditor options={frozenOptions} onOptionsChange={mockOnOptionsChange} />);
    }).not.toThrow();
  });
  it('renders data source description', () => {
    renderConfigEditor();
    expect(screen.getByTestId('datasource-description')).toBeInTheDocument();
    expect(screen.getByText('Data source: Kafka')).toBeInTheDocument();
  });

  it('renders connection section with bootstrap servers and client ID', () => {
    renderConfigEditor();
    expect(screen.getByPlaceholderText('broker1:9092,broker2:9092')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('my-kafka-client')).toBeInTheDocument();
  });

  it('calls onOptionsChange when bootstrap servers change', () => {
    renderConfigEditor();
    const input = screen.getByPlaceholderText('broker1:9092,broker2:9092');

    fireEvent.change(input, { target: { value: 'localhost:9092' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          bootstrapServers: 'localhost:9092',
        }),
      })
    );
  });

  it('calls onOptionsChange when client ID changes', () => {
    renderConfigEditor();
    const input = screen.getByPlaceholderText('my-kafka-client');

    fireEvent.change(input, { target: { value: 'test-client' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          clientId: 'test-client',
        }),
      })
    );
  });

  it('calls onOptionsChange when security protocol changes', () => {
    renderConfigEditor();
    const select = screen.getByTestId('select');

    fireEvent.change(select, { target: { value: 'SASL_PLAINTEXT' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          securityProtocol: 'SASL_PLAINTEXT',
        }),
      })
    );
  });

  it('shows SASL fields when SASL_PLAINTEXT is selected', () => {
    renderConfigEditor({ securityProtocol: 'SASL_PLAINTEXT' });

    expect(screen.getByPlaceholderText('SASL Username')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('SASL Password')).toBeInTheDocument();
  });

  it('shows SASL fields when SASL_SSL is selected', () => {
    renderConfigEditor({ securityProtocol: 'SASL_SSL' });

    expect(screen.getByPlaceholderText('SASL Username')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('SASL Password')).toBeInTheDocument();
  });

  it('hides SASL fields when PLAINTEXT is selected', () => {
    renderConfigEditor({ securityProtocol: 'PLAINTEXT' });

    expect(screen.queryByPlaceholderText('SASL Username')).not.toBeInTheDocument();
    expect(screen.queryByPlaceholderText('SASL Password')).not.toBeInTheDocument();
  });

  it('shows TLS fields when SSL is selected', () => {
    renderConfigEditor({ securityProtocol: 'SSL' });

    expect(screen.getByText('TLS Settings')).toBeInTheDocument();
    expect(screen.getByText('Skip TLS Verification')).toBeInTheDocument();
    expect(screen.getByText('Self-signed Certificate')).toBeInTheDocument();
    expect(screen.getByText('TLS Client Authentication')).toBeInTheDocument();
  });

  it('shows TLS fields when SASL_SSL is selected', () => {
    renderConfigEditor({ securityProtocol: 'SASL_SSL' });

    expect(screen.getByText('TLS Settings')).toBeInTheDocument();
  });

  it('calls onOptionsChange when SASL username changes', () => {
    renderConfigEditor({ securityProtocol: 'SASL_PLAINTEXT' });
    const input = screen.getByPlaceholderText('SASL Username');

    fireEvent.change(input, { target: { value: 'testuser' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          saslUsername: 'testuser',
        }),
      })
    );
  });

  it('calls onOptionsChange when SASL password changes', () => {
    renderConfigEditor({ securityProtocol: 'SASL_PLAINTEXT' });
    const input = screen.getByPlaceholderText('SASL Password');

    fireEvent.change(input, { target: { value: 'testpass' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secureJsonData: expect.objectContaining({
          saslPassword: 'testpass',
        }),
      })
    );
  });

  it('calls onOptionsChange when TLS skip verify changes', () => {
    renderConfigEditor({ securityProtocol: 'SSL' });
    const checkbox = screen.getAllByTestId('checkbox')[0]; // First checkbox is skip verify

    fireEvent.click(checkbox);

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          tlsSkipVerify: true,
        }),
      })
    );
  });

  it('calls onOptionsChange when TLS auth with CA cert changes', () => {
    renderConfigEditor({ securityProtocol: 'SSL' });
    const checkbox = screen.getAllByTestId('checkbox')[1]; // Second checkbox is CA cert

    fireEvent.click(checkbox);

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          tlsAuthWithCACert: true,
        }),
      })
    );
  });

  it('shows CA certificate textarea when TLS auth with CA cert is enabled', () => {
    renderConfigEditor({ securityProtocol: 'SSL', tlsAuthWithCACert: true });

    expect(screen.getByPlaceholderText('Begins with -----BEGIN CERTIFICATE-----')).toBeInTheDocument();
  });

  it('calls onOptionsChange when TLS client auth changes', () => {
    renderConfigEditor({ securityProtocol: 'SSL' });
    const checkbox = screen.getAllByTestId('checkbox')[2]; // Third checkbox is client auth

    fireEvent.click(checkbox);

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          tlsAuth: true,
        }),
      })
    );
  });

  it('shows client certificate fields when TLS client auth is enabled', () => {
    renderConfigEditor({ securityProtocol: 'SSL', tlsAuth: true });

    expect(screen.getByPlaceholderText('domain.example.com')).toBeInTheDocument();
    expect(screen.getAllByPlaceholderText('Begins with -----BEGIN CERTIFICATE-----')).toHaveLength(1);
    expect(screen.getByPlaceholderText('Begins with -----BEGIN PRIVATE KEY-----')).toBeInTheDocument();
  });

  it('calls onOptionsChange when server name changes', () => {
    renderConfigEditor({ securityProtocol: 'SSL', tlsAuth: true });
    const input = screen.getByPlaceholderText('domain.example.com');

    fireEvent.change(input, { target: { value: 'kafka.example.com' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          serverName: 'kafka.example.com',
        }),
      })
    );
  });

  it('calls onOptionsChange when CA certificate changes', () => {
    renderConfigEditor({ securityProtocol: 'SSL', tlsAuthWithCACert: true });
    const textarea = screen.getByPlaceholderText('Begins with -----BEGIN CERTIFICATE-----');

    fireEvent.change(textarea, { target: { value: '-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secureJsonData: expect.objectContaining({
          tlsCACert: '-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----',
        }),
      })
    );
  });

  it('renders advanced settings section', () => {
    renderConfigEditor();

    expect(screen.getByText('Advanced Settings')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('debug | info | warn | error')).toBeInTheDocument();
  });

  it('calls onOptionsChange when log level changes', () => {
    renderConfigEditor();
    const input = screen.getByPlaceholderText('debug | info | warn | error');

    fireEvent.change(input, { target: { value: 'debug' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          logLevel: 'debug',
        }),
      })
    );
  });

  it('calls onOptionsChange when healthcheck timeout changes', () => {
    renderConfigEditor();
    const input = screen.getByDisplayValue('2000'); // Default healthcheck timeout

    fireEvent.change(input, { target: { value: '5000' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          healthcheckTimeout: 5000,
        }),
      })
    );
  });

  it('validates healthcheck timeout to non-negative values', () => {
    renderConfigEditor();
    const input = screen.getByDisplayValue('2000');

    fireEvent.change(input, { target: { value: '-100' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          healthcheckTimeout: 0,
        }),
      })
    );
  });

  it('calls onOptionsChange when request timeout changes', () => {
    renderConfigEditor();
    const input = screen.getByDisplayValue('0'); // Default request timeout

    fireEvent.change(input, { target: { value: '30000' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          timeout: 30000,
        }),
      })
    );
  });

  it('validates request timeout to non-negative values', () => {
    renderConfigEditor();
    const input = screen.getByDisplayValue('0');

    fireEvent.change(input, { target: { value: '-500' } });

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        jsonData: expect.objectContaining({
          timeout: 0,
        }),
      })
    );
  });

  it('handles SASL password reset', () => {
    renderConfigEditor(
      { securityProtocol: 'SASL_PLAINTEXT' },
      { saslPassword: 'existing-password' },
      { saslPassword: true }
    );

    const resetButton = screen.getByTestId('reset-button');
    fireEvent.click(resetButton);

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secureJsonFields: expect.objectContaining({
          saslPassword: false,
        }),
        secureJsonData: expect.objectContaining({
          saslPassword: '',
        }),
      })
    );
  });

  it('handles TLS CA certificate reset', () => {
    renderConfigEditor(
      { securityProtocol: 'SSL', tlsAuthWithCACert: true },
      { tlsCACert: 'existing-cert' },
      { tlsCACert: true }
    );

    const resetButton = screen.getByTestId('reset-textarea-button');
    fireEvent.click(resetButton);

    expect(mockOnOptionsChange).toHaveBeenCalledWith(
      expect.objectContaining({
        secureJsonFields: expect.objectContaining({
          tlsCACert: false,
        }),
        secureJsonData: expect.objectContaining({
          tlsCACert: '',
        }),
      })
    );
  });

  it('preserves existing configuration values', () => {
    const existingConfig = {
      bootstrapServers: 'existing-servers:9092',
      clientId: 'existing-client',
      securityProtocol: 'SASL_SSL',
      saslMechanisms: 'SCRAM-SHA-256',
      saslUsername: 'existing-user',
      logLevel: 'info',
      healthcheckTimeout: 5000,
      timeout: 10000,
    };

    renderConfigEditor(existingConfig);

    expect(screen.getByDisplayValue('existing-servers:9092')).toBeInTheDocument();
    expect(screen.getByDisplayValue('existing-client')).toBeInTheDocument();
    expect(screen.getByDisplayValue('existing-user')).toBeInTheDocument();
    expect(screen.getByDisplayValue('info')).toBeInTheDocument();
    expect(screen.getByDisplayValue('5000')).toBeInTheDocument();
    expect(screen.getByDisplayValue('10000')).toBeInTheDocument();
  });
});
