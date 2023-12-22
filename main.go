//go:generate mockery
package main

import (
	_ "embed"

	"github.com/dsh2dsh/edgar/cmd"
)

//go:embed db/schema.sql
var schemaSQL string

func init() {
	cmd.SchemaSQL = schemaSQL
}

func main() {
	cmd.Execute()
}
