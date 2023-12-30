//go:generate mockery
package main

import (
	_ "embed"

	"github.com/dsh2dsh/edgar/cmd"
	"github.com/dsh2dsh/edgar/cmd/db"
)

//go:embed db/schema.sql
var schemaSQL string

func init() {
	db.SchemaSQL = schemaSQL
}

func main() {
	cmd.Execute()
}
