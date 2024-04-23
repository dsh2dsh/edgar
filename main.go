//go:generate mockery
package main

import (
	_ "embed"

	"github.com/dsh2dsh/edgar/cmd"
	"github.com/dsh2dsh/edgar/cmd/db"
)

var (
	//go:embed db/schema.sql
	schemaSQL string
	version   string
)

func init() {
	db.SchemaSQL = schemaSQL
}

func main() {
	cmd.Execute(version)
}
