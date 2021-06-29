# dbt-py

Python module to enable dbt on steroids. Enable enhanced logging and alerting. This package  installs an executable `pydbt` that replaces the standard `dbt` executable.

Sentry is integrated by default. Enable sentry by setting the default sentry environment variables.

## Alerting

- Enable `slack` alerting by setting the `SLACK_URL` environment variable.

## Monitoring

- Enable `datadog` monitoring by setting the `DATADOG_HOST` and `DATADOG_PORT` environment variables.
- Enable `prometheus` monitoring by setting the `PUSHGATEWAY_HOST` and `PUSHGATEWAY_PORT` environment variables.

## Running locally
```
# Install package locally
make install
```

# Running tests
```
make test
```
