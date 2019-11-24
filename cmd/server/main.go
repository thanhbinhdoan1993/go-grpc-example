package main

import (
	"fmt"
	"os"

	"github.com/thanhbinhdoan1993/go-grpc-example/pkg/cmd"
)

func main() {
	if err := cmd.RunServer(); err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
