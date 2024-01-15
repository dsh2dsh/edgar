[![Go](https://github.com/dsh2dsh/edgar/actions/workflows/go.yml/badge.svg)](https://github.com/dsh2dsh/edgar/actions/workflows/go.yml)

# Download data files from [EDGAR](https://www.sec.gov/edgar)

Right now this project is under constraction.

## Install

Install edgar cli using

```
$ go install github.com/dsh2dsh/edgar@latest
```

or execute it directly using

```
$ go run github.com/dsh2dsh/edgar@latest
```

## Usage

Before using this programm, you need postgresql instance. Create an `.env` file
with content like this:

```
EDGAR_DB_URL="postgres://username:password@localhost:5432/database_name
EDGAR_UA="Sample Company Name AdminContact@<sample company domain>.com"
```

Replace company name and contact info to your real info and initialize the db:

```
$ edgar db init
```

Download all facts into the db:

```
$ edgar db upload
```

Periodically fetch new facts:

```
$ edgar db update
```

## How to test

You need postgresql instance. In project's directory create `.env` file:

```
EDGAR_DB_URL="postgres://username:password@localhost:5432/database_name
EDGAR_UA="Sample Company Name AdminContact@<sample company domain>.com"
```

Replace company name and contact info to your real info and do:

```
$ make test
```

It runs all local tests. For running E2E tests, which fetches real data from
EDGAR, do:

```
$ make test-e2e
```
