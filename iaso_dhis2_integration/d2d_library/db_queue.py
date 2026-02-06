import sqlite3
import threading


class Queue:
    """A thread-safe queue backed by a SQLite database for managing DHIS2 extract file paths.

    Methods
    -------
    enqueue(filename: str)
        Add a new filename to the queue if it does not already exist.
    dequeue()
        Remove and return the first filename in the queue.
    peek()
        Return the first filename in the queue without removing it.
    count() -> int
        Return the number of items in the queue.
    reset()
        Clear all contents from the queue and reset the indexing.
    view_queue()
        View all elements in the queue without removing them.
    count_queue_items() -> int
        Count the number of items in the queue.
    """

    def __init__(self, db_path: str):
        """Initialize the queue with the given SQLite database path."""
        self.db_path = db_path
        self._lock = threading.Lock()
        self._initialize_queue()

    def _initialize_queue(self):
        """Create the queue table if it does not exist."""
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL
                )
            """)
            conn.commit()

    def enqueue(self, filename: str) -> None:
        """Add a new filename to the queue only if it does not already exist."""
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM queue WHERE filename = ?", (filename,))
            exists = cursor.fetchone()[0]

            if not exists:  # Insert only if the filename does not exist
                cursor.execute("INSERT INTO queue (filename) VALUES (?)", (filename,))
                conn.commit()

    def dequeue(self) -> str:
        """Remove and return the first filename in the queue.

        Returns
        -------
        str
            The filename of the dequeued item, or None if the queue is empty.
        """
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, filename FROM queue ORDER BY id LIMIT 1")
            item = cursor.fetchone()
            if item:
                cursor.execute("DELETE FROM queue WHERE id = ?", (item[0],))
                conn.commit()
                return item[1]
        return None

    def peek(self) -> str:
        """Return the first filename in the queue without removing it.

        Returns
        -------
        str
            The filename of the first item in the queue, or None if the queue is empty.
        """
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT filename FROM queue ORDER BY id LIMIT 1")
            item = cursor.fetchone()
        return item[0] if item else None

    def count(self) -> int:
        """Return the number of items in the queue.

        Returns
        -------
        int
            The number of items in the queue.
        """
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM queue")
            count = cursor.fetchone()[0]
        return count  # noqa: RET504

    def reset(self) -> None:
        """Clear all contents from the queue and reset the indexing."""
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS queue;")  # Drop table to reset indexing
            conn.commit()
        # Recreate table
        self._initialize_queue()

    def view_queue(self) -> None:
        """View all elements in the queue without removing them."""
        with self._lock, sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, filename FROM queue ORDER BY id")
            items = cursor.fetchall()
            if items:
                print("Queue contents:")
                for item in items:
                    print(f"ID: {item[0]}, filename: {item[1]}")
            else:
                print("The queue is empty.")
