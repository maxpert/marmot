package main_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func cleanup() {
	patterns := []string{
		"/tmp/marmot-1*",
		"/tmp/marmot-2*",
		"/tmp/marmot-3*",
	}

	for _, pattern := range patterns {
		files, err := filepath.Glob(pattern)
		if err != nil {
			log.Fatal(err)
		}

		for _, file := range files {
			err := os.RemoveAll(file)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	os.RemoveAll("/tmp/nats")
}

func startCluster() (*exec.Cmd, *exec.Cmd, *exec.Cmd) {

	cleanup()

	createDB("/tmp/marmot-1.db")
	createDB("/tmp/marmot-2.db")
	createDB("/tmp/marmot-3.db")

	node1 := startNode("examples/node-1-config.toml", "localhost:4221", "nats://localhost:4222/,nats://localhost:4223/")

	node2 := startNode("examples/node-2-config.toml", "localhost:4222", "nats://localhost:4221/,nats://localhost:4223/")

	node3 := startNode("examples/node-3-config.toml", "localhost:4223", "nats://localhost:4221/,nats://localhost:4222/")

	time.Sleep(time.Second * 10)

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
	cmd := exec.Command("marmot", "-config", config, "-cluster-addr", addr, "-cluster-peers", peers)
	if wd, err := os.Getwd(); err == nil {
		wd = wd[:len(wd)-len("/tests/e2e")]
		cmd.Dir = wd
	}
	err := cmd.Start()
	Expect(err).To(BeNil())
	return cmd
}

func insertBook() {
	db, err := sql.Open("sqlite3", "/tmp/marmot-1.db")
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	query := `INSERT INTO Books (title, author, publication_year) VALUES (?, ?, ?)`
	_, err = db.Exec(query, "Pride and Prejudice", "Jane Austen", 1813)
	if err != nil {
		fmt.Println("Error inserting book:", err)
	}
}

func countBook(title string) int {
	db, err := sql.Open("sqlite3", "/tmp/marmot-2.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM Books WHERE title = ?", title).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	return count
}

var _ = Describe("Marmot", Ordered, func() {
	var node1, node2, node3 *exec.Cmd
	BeforeAll(func() {
		node1, node2, node3 = startCluster()
	})
	AfterAll(func() {
		stopCluster(node1, node2, node3)
	})
	Context("when the system is running", func() {
		It("should be able to replicate a row", func() {
			insertBook()

			time.Sleep(time.Second * 3)

			Expect(countBook("Pride and Prejudice")).To(Equal(1))
		})
	})
})
