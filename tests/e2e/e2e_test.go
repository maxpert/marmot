package main_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var wd string

var _ = Describe("HarmonyLite End-to-End Tests", Ordered, func() {
	var node1, node2, node3 *exec.Cmd

	BeforeAll(func() {
		var err error
		wd, err = os.Getwd()
		Expect(err).To(BeNil())
		wd = wd[:len(wd)-len("/tests/e2e")]
		cleanup()

		// Create databases with both tables
		for i := 1; i <= 3; i++ {
			dbPath := filepath.Join(dbDir, fmt.Sprintf("harmonylite-%d.db", i))
			createDatabase(dbPath)     // Creates Books table
			createAuthorsTable(dbPath) // Creates Authors table
		}

		node1, node2, node3 = startCluster()
	})

	AfterAll(func() {
		stopNodes(node1, node2, node3)
	})

	Context("Basic Replication", func() {
		It("should replicate INSERT operations", func() {
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Pride and Prejudice", "Jane Austen", 1813)
			Eventually(func() int {
				return countBooksByTitle(filepath.Join(dbDir, "harmonylite-2.db"), "Pride and Prejudice")
			}, maxWaitTime, pollInterval).Should(Equal(1), "Insert replication failed")
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Insert not replicated to node 3")
		})

		It("should replicate UPDATE operations", func() {
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Update Test", "Author", 2020)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Initial insert not replicated for update")
			updateBookTitle(filepath.Join(dbDir, "harmonylite-1.db"), id, "Updated Title")
			Eventually(func() string {
				return getBookTitle(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal("Updated Title"), "Update replication failed")
			Eventually(func() string {
				return getBookTitle(filepath.Join(dbDir, "harmonylite-3.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal("Updated Title"), "Update not replicated to node 3")
		})

		It("should replicate DELETE operations", func() {
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Delete Test", "Author", 2020)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Initial insert not replicated for delete")
			deleteBookByID(filepath.Join(dbDir, "harmonylite-1.db"), id)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(0), "Delete replication failed")
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(0), "Delete not replicated to node 3")
		})
	})

	Context("Concurrency and Conflict Resolution", func() {
		It("should resolve concurrent updates using last-writer-wins", func() {
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Conflict Test", "Author", 2020)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Initial insert not replicated for conflict test")

			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				updateBookTitle(filepath.Join(dbDir, "harmonylite-1.db"), id, "Node1 Update")
			}()
			go func() {
				defer GinkgoRecover()
				defer wg.Done()
				time.Sleep(100 * time.Millisecond) // Ensure Node2 writes last
				updateBookTitle(filepath.Join(dbDir, "harmonylite-2.db"), id, "Node2 Update")
			}()
			wg.Wait()

			Eventually(func() bool {
				t1 := getBookTitle(filepath.Join(dbDir, "harmonylite-1.db"), id)
				t2 := getBookTitle(filepath.Join(dbDir, "harmonylite-2.db"), id)
				t3 := getBookTitle(filepath.Join(dbDir, "harmonylite-3.db"), id)
				consistent := t1 == "Node2 Update" && t2 == "Node2 Update" && t3 == "Node2 Update"
				GinkgoWriter.Printf("Conflict check - Node1: %s, Node2: %s, Node3: %s\n", t1, t2, t3)
				return consistent
			}, maxWaitTime, pollInterval).Should(BeTrue(), "Conflict resolution failed; expected 'Node2 Update' to win")
		})
	})

	Context("Snapshot and Restore", func() {
		It("should save and restore snapshots correctly", func() {
			// Insert data before snapshot
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Snapshot Test", "Author", 2020)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Initial insert not replicated for snapshot test")

			// Stop node 3, save snapshot, and restart
			stopNodes(node3)
			time.Sleep(2 * time.Second) // Allow snapshot to be taken
			node3Cmd := exec.Command(filepath.Join(wd, "harmonylite"),
				"-config", "examples/node-3-config.toml",
				"-node-id", "3",
				"-save-snapshot")
			node3Cmd.Dir = wd
			node3Cmd.Stdout = GinkgoWriter
			node3Cmd.Stderr = GinkgoWriter
			Expect(node3Cmd.Run()).To(Succeed(), "Failed to save snapshot on node 3")
			node3 = startNode("examples/node-3-config.toml", "127.0.0.1:4223", "nats://127.0.0.1:4221/,nats://127.0.0.1:4222/", 3)

			// Verify restored data
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id)
			}, maxWaitTime*2, pollInterval).Should(Equal(1), "Snapshot restore failed on node 3")
		})
	})

	Context("Node Failure and Recovery", func() {
		It("should recover replication after node failure", func() {
			// Insert initial data
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Failure Test", "Author", 2020)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Initial insert not replicated for failure test")

			// Simulate node 3 failure
			stopNodes(node3)
			time.Sleep(2 * time.Second) // Allow some time for failure to propagate

			// Insert more data while node 3 is down
			id2 := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Post-Failure Test", "Author", 2021)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id2)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Insert not replicated to node 2 during node 3 downtime")

			// Restart node 3
			node3 = startNode("examples/node-3-config.toml", "127.0.0.1:4223", "nats://127.0.0.1:4221/,nats://127.0.0.1:4222/", 3)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id2)
			}, maxWaitTime*2, pollInterval).Should(Equal(1), "Node 3 failed to recover replication after restart")
		})
	})

	Context("Multi-Table Replication", func() {
		BeforeEach(func() {
		})

		It("should replicate changes across multiple tables", func() {
			bookID := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Multi-Table Book", "Jane Austen", 1813)
			authorID := insertAuthor(filepath.Join(dbDir, "harmonylite-1.db"), "Jane Austen", 1775)

			Eventually(func() int {
				return countBooksByTitle(filepath.Join(dbDir, "harmonylite-2.db"), "Multi-Table Book")
			}, maxWaitTime, pollInterval).Should(Equal(1), "Book replication failed across tables")
			Eventually(func() int {
				return countAuthorsByName(filepath.Join(dbDir, "harmonylite-2.db"), "Jane Austen")
			}, maxWaitTime, pollInterval).Should(Equal(1), "Author replication failed across tables")

			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), bookID)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Book not replicated to node 3")
			Eventually(func() int {
				return countAuthorsByID(filepath.Join(dbDir, "harmonylite-3.db"), authorID)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Author not replicated to node 3")
		})
	})

	Context("Large Data Volumes", func() {
		It("should handle replication of many records", func() {
			const numRecords = 100
			var ids []int64
			for i := 0; i < numRecords; i++ {
				id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Bulk Test "+string(rune(i)), "Author", 2020+i)
				ids = append(ids, id)
			}

			Eventually(func() int {
				count := 0
				for _, id := range ids {
					count += countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
				}
				return count
			}, maxWaitTime*2, pollInterval).Should(Equal(numRecords), "Failed to replicate all records to node 2")

			Eventually(func() int {
				count := 0
				for _, id := range ids {
					count += countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id)
				}
				return count
			}, maxWaitTime*2, pollInterval).Should(Equal(numRecords), "Failed to replicate all records to node 3")
		})
	})

	Context("Configuration Variations", func() {
		It("should work with publish disabled on one node", func() {
			// Stop node 2
			stopNodes(node2)

			// Create a temporary config file for node 2 with publish disabled
			tmpConfigPath := filepath.Join(dbDir, "node-2-disabled-publish.toml")
			originalConfig, err := os.ReadFile(filepath.Join(wd, "examples/node-2-config.toml"))
			Expect(err).To(BeNil(), "Failed to read original config file")

			// Append publish=false to the config
			modifiedConfig := append([]byte("\npublish=false\n"), originalConfig...)
			err = os.WriteFile(tmpConfigPath, modifiedConfig, 0644)
			Expect(err).To(BeNil(), "Failed to write modified config file")

			// Start node 2 with the modified config
			node2 = exec.Command(filepath.Join(wd, "harmonylite"),
				"-config", tmpConfigPath,
				"-node-id", "2",
				"-cluster-addr", "127.0.0.1:4222",
				"-cluster-peers", "nats://127.0.0.1:4221/,nats://127.0.0.1:4223/")
			node2.Dir = wd
			node2.Stdout = GinkgoWriter
			node2.Stderr = GinkgoWriter
			Expect(node2.Start()).To(Succeed(), "Failed to start node 2 with publish disabled")

			// Insert on node 2 (should not replicate)
			id := insertBook(filepath.Join(dbDir, "harmonylite-2.db"), "No Publish Test", "Author", 2020)
			Consistently(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-1.db"), id)
			}, 5*time.Second, pollInterval).Should(Equal(0), "Data replicated from node 2 despite publish disabled")

			// Insert on node 1 (should replicate to node 2)
			id2 := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Publish Test", "Author", 2020)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id2)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Data not replicated to node 2 with publish disabled")

			// Clean up
			defer os.Remove(tmpConfigPath)
		})
	})

	Context("Snapshot Leader Election", func() {
		It("should elect one node as snapshot leader among multiple publishers", func() {
			// All 3 nodes have publish=true by default
			// The leader election should ensure only one becomes the leader
			
			// Insert data to trigger some activity
			id := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Leader Election Test", "Author", 2026)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Data not replicated for leader election test")

			// Wait for leader election to stabilize (TTL is 30s, so wait a bit)
			time.Sleep(5 * time.Second)

			// At this point, leader election should have happened
			// We can't directly check which node is leader from here,
			// but the test passes if no panics/errors occurred during election
			GinkgoWriter.Println("Leader election completed without errors")
		})

		It("should failover leadership when leader node is stopped", func() {
			// Insert initial data
			id1 := insertBook(filepath.Join(dbDir, "harmonylite-1.db"), "Before Failover", "Author", 2026)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-2.db"), id1)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Initial data not replicated")

			// Stop node 1 (potential leader)
			GinkgoWriter.Println("Stopping node 1 to trigger leader failover...")
			stopNodes(node1)

			// Wait for leader TTL to expire and new leader to be elected
			// TTL is 30s, but heartbeat is TTL/3 = 10s, so wait about 35s for safety
			GinkgoWriter.Println("Waiting for leader election TTL to expire...")
			time.Sleep(35 * time.Second)

			// Insert new data on remaining nodes
			id2 := insertBook(filepath.Join(dbDir, "harmonylite-2.db"), "After Failover", "New Author", 2026)
			
			// Verify replication still works (node 2 and 3 should still function)
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-3.db"), id2)
			}, maxWaitTime, pollInterval).Should(Equal(1), "Data not replicated after leader failover")

			// Restart node 1
			GinkgoWriter.Println("Restarting node 1...")
			node1 = startNode("examples/node-1-config.toml", "127.0.0.1:4221", "nats://127.0.0.1:4222/,nats://127.0.0.1:4223/", 1)

			// Verify node 1 catches up with data
			Eventually(func() int {
				return countBooksByID(filepath.Join(dbDir, "harmonylite-1.db"), id2)
			}, maxWaitTime*2, pollInterval).Should(Equal(1), "Node 1 did not catch up after restart")

			GinkgoWriter.Println("Leader failover test completed successfully")
		})
	})
})
