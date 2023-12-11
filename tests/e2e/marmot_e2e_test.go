package main_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMarmot(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Marmot E2E Tests")
}
