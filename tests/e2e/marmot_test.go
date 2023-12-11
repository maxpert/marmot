package main_test

import (
	"database/sql"
	"fmt"
	"log"
	"os/exec"
	"time"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
