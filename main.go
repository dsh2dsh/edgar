//go:generate mockery
package main

import "github.com/dsh2dsh/edgar/cmd"

func main() {
	cmd.Execute()
}
