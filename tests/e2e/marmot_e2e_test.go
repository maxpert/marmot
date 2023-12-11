package main_test

import (
	"database/sql"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMarmot(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Marmot E2E Tests")
}

func cleanup() {
	os.RemoveAll("/tmp/marmot-1-*")
	os.RemoveAll("/tmp/marmot-2-*")
	os.RemoveAll("/tmp/marmot-3-*")
	os.RemoveAll("/tmp/nats")
}

func startCluster() (*exec.Cmd, *exec.Cmd, *exec.Cmd) {

	cleanup()

	createDB("/tmp/marmot-1.db")
	createDB("/tmp/marmot-2.db")
	createDB("/tmp/marmot-3.db")

	node1 := startNode("examples/node-1-config.toml", "localhost:4221", "nats://localhost:4222/,nats://localhost:4223/")
	time.Sleep(time.Second)

	node2 := startNode("examples/node-2-config.toml", "localhost:4222", "nats://localhost:4221/,nats://localhost:4223/")
	time.Sleep(time.Second)

	node3 := startNode("examples/node-3-config.toml", "localhost:4223", "nats://localhost:4221/,nats://localhost:4222/")

	return node1, node2, node3
}

func stopCluster(node1, node2, node3 *exec.Cmd) {

	node1.Process.Kill()
	node2.Process.Kill()
	node3.Process.Kill()

	err := node1.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			Expect(exitErr.Stderr).To(BeNil())
		}
	}
	err = node2.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			Expect(exitErr.Stderr).To(BeNil())
		}
	}
	err = node3.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			Expect(exitErr.Stderr).To(BeNil())
		}
	}
	cleanup()
}

func createDB(dbFile string) {
	db, err := sql.Open("sqlite3", dbFile)
	Expect(err).To(BeNil())
	defer db.Close()

	_, err = db.Exec(`
        DROP TABLE IF EXISTS Books;
        CREATE TABLE Books (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            publication_year INTEGER
        );
        INSERT INTO Books (title, author, publication_year)
        VALUES
        ('The Hitchhiker''s Guide to the Galaxy', 'Douglas Adams', 1979),
        ('The Lord of the Rings', 'J.R.R. Tolkien', 1954),
        ('Harry Potter and the Sorcerer''s Stone', 'J.K. Rowling', 1997),
        ('The Catcher in the Rye', 'J.D. Salinger', 1951),
        ('To Kill a Mockingbird', 'Harper Lee', 1960),
        ('1984', 'George Orwell', 1949),
        ('The Great Gatsby', 'F. Scott Fitzgerald', 1925);
    `)
	Expect(err).To(BeNil())
}

func startNode(config, addr, peers string) *exec.Cmd {
	cmd := exec.Command("go", "run", "marmot", "-config", config, "-cluster-addr", addr, "-cluster-peers", peers)
	err := cmd.Start()
	Expect(err).To(BeNil())
	return cmd
}
