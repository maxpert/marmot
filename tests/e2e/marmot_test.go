package main_test

import (
	"os/exec"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Marmot", Ordered, func() {
	var node1, node2, node3 *exec.Cmd
	BeforeAll(func() {
		node1, node2, node3 = startCluster()
	})
	AfterAll(func() {
		stopCluster(node1, node2, node3)
	})
	Context("when the system is running", func() {
		It("should be able to run tests", func() {
			Expect(true).To(BeTrue())
		})
	})
})
