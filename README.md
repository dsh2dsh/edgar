# Download data files from [EDGAR](https://www.sec.gov/edgar)

[![Go](https://github.com/dsh2dsh/edgar/actions/workflows/go.yml/badge.svg)](https://github.com/dsh2dsh/edgar/actions/workflows/go.yml)

Right now this project is under constraction.

## How to test

In project's directory do

``` shell
make test
```

and it runs all local tests. For running E2E tests, which fetches real data from
EDGAR, first create `.env` file with content like

```
EDGAR_UA="Sample Company Name AdminContact@<sample company domain>.com"
```

Or somehow else define env variable `EDGAR_UA`. Change `EDGAR_UA` to real
company name and real contact and do

``` shell
make test-e2e
```
