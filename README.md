![flowrs_logo](./image/README/1683789045509.png)

Flowrs is a TUI application for [Apache Airflow](https://airflow.apache.org/). It allows you to monitor, inspect and manage Airflow DAGs from the comforts of your terminal. It is build with the [ratatui](https://ratatui.rs/) library.

![flowrs demo](./vhs/flowrs.gif)

## Installation

You can install `flowrs` via Homebrew if you're on macOS / Linux / WSL2:

```
brew tap jvanbuel/flowrs
brew install flowrs
```

or by downloading the binary directly from GitHub:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/jvanbuel/flowrs/releases/latest/download/flowrs-tui-installer.sh | sh
```

Alternatively, you can build `flowrs` from source with `cargo`:

```bash
cargo install flowrs-tui --locked
```

## Usage

### Managed Airflow services

The easiest way to user `flowrs` is with a managed Airflow service. The currently supported managed services are:

- [x] Conveyor
- [x] Amazon Managed Workflows for Apache Airflow (MWAA)
- [ ] Google Cloud Composer
- [x] Astronomer

To enable a managed service, run `flowrs config enable -m <service>`. This will add the configuration for the managed service to your configuration file, or prompt you for the necessary configuration details. On startup `flowrs` will then try to find and connect to all available managed service's Airflow instances.

Note that for Astronomer, you need to set the `ASTRO_API_TOKEN` environment variable with your Astronomer API token (Organization, Workspace or Deployment) to be able to connect to the service.

### Custom Airflow instances

If you're self-hosting an Airflow instance, or your favorite managed service is not yet supported, you can register an Airflow server instance with the `flowrs config add` command:

![flowrs config add demo](./vhs/add_config.gif)

This creates an entry in a `~/.flowrs` configuration file. If you have multiple Airflow servers configured, you can easily switch between them in `flowrs` configuration screen.

Only basic authentication and bearer token authentication are supported. When selecting the bearer token option, you can either provide a static token or a command that generates a token.

### Proxy Configuration

If your Airflow instance is behind a proxy, `flowrs` supports proxy configuration in multiple ways:

#### Per-Server Proxy Configuration

You can configure a proxy for each Airflow server in your `~/.flowrs` configuration file:

```toml
[[servers]]
name = "my-server"
endpoint = "http://airflow.internal:8080"
proxy = "http://proxy.company.com:8080"
version = "V2"

[servers.auth.Basic]
username = "${AIRFLOW_USERNAME}"
password = "${AIRFLOW_PASSWORD}"
```

The proxy URL supports environment variable expansion using `${VAR}` syntax:

```toml
proxy = "${PROXY_URL}"
```

#### Environment Variables

`flowrs` automatically respects standard HTTP proxy environment variables if no per-server proxy is configured:

- `HTTP_PROXY` or `http_proxy`: Proxy for HTTP requests
- `HTTPS_PROXY` or `https_proxy`: Proxy for HTTPS requests
- `NO_PROXY` or `no_proxy`: Comma-separated list of hosts to exclude from proxying

Example:

```bash
export HTTP_PROXY=http://proxy.company.com:8080
export HTTPS_PROXY=http://proxy.company.com:8080
flowrs run
```

**Priority**: Per-server proxy configuration takes precedence over environment variables.

**Proxy Authentication**: Proxies requiring authentication can be configured using the standard URL format:

```toml
proxy = "http://username:password@proxy.company.com:8080"
```
