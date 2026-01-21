"""
Database connection pool for Databricks SQL Warehouse.

Provides thread-safe connection pooling to reduce overhead of creating
new connections for each query.
"""
import os
import threading
import queue
from typing import Optional
import logging

try:
    import databricks.sql as dbsql
except ImportError:
    dbsql = None

logger = logging.getLogger("discount_tire_demo.db_pool")


class SQLWarehousePool:
    """Thread-safe connection pool for Databricks SQL Warehouse."""
    
    def __init__(self, pool_size: int = 3, timeout: int = 30):
        """
        Initialize connection pool.
        
        Args:
            pool_size: Number of connections to maintain
            timeout: Connection timeout in seconds
        """
        self.pool_size = pool_size
        self.timeout = timeout
        self.connections = queue.Queue(maxsize=pool_size)
        self.lock = threading.Lock()
        self._closed = False
        
        # Configuration from environment
        self.host = os.getenv("DATABRICKS_HOST")
        self.http_path = os.getenv("DATABRICKS_SQL_HTTP_PATH")
        self.token = os.getenv("DATABRICKS_TOKEN_FOR_SQL")
        
        if self.host and self.http_path and self.token and dbsql:
            self._initialize_pool()
        else:
            logger.warning("SQL Warehouse pool not initialized (missing config or databricks-sql-connector)")
    
    def _create_connection(self):
        """Create a new SQL Warehouse connection."""
        if not dbsql:
            return None
        
        try:
            conn = dbsql.connect(
                server_hostname=self.host,
                http_path=self.http_path,
                access_token=self.token,
                _socket_timeout=self.timeout
            )
            logger.info("Created new SQL Warehouse connection")
            return conn
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            return None
    
    def _initialize_pool(self):
        """Populate the connection pool."""
        for i in range(self.pool_size):
            conn = self._create_connection()
            if conn:
                self.connections.put(conn)
                logger.debug(f"Added connection {i+1}/{self.pool_size} to pool")
    
    def get_connection(self, block: bool = True, timeout: Optional[float] = None):
        """
        Get a connection from the pool.
        
        Args:
            block: If True, block until a connection is available
            timeout: Maximum time to wait for a connection
            
        Returns:
            Connection object or None if unavailable
        """
        if self._closed:
            logger.error("Cannot get connection from closed pool")
            return None
        
        try:
            conn = self.connections.get(block=block, timeout=timeout)
            logger.debug("Retrieved connection from pool")
            return conn
        except queue.Empty:
            logger.warning("No connections available in pool")
            return None
    
    def return_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: Connection object to return
        """
        if self._closed:
            try:
                conn.close()
            except Exception:
                pass
            return
        
        try:
            # Test connection is still alive
            conn.cursor().execute("SELECT 1")
            self.connections.put(conn, block=False)
            logger.debug("Returned connection to pool")
        except Exception as e:
            logger.warning(f"Connection test failed, creating new one: {e}")
            # Replace dead connection with new one
            new_conn = self._create_connection()
            if new_conn:
                try:
                    self.connections.put(new_conn, block=False)
                except queue.Full:
                    new_conn.close()
    
    def close_all(self):
        """Close all connections in the pool."""
        with self.lock:
            if self._closed:
                return
            
            self._closed = True
            closed_count = 0
            
            while not self.connections.empty():
                try:
                    conn = self.connections.get_nowait()
                    conn.close()
                    closed_count += 1
                except queue.Empty:
                    break
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
            
            logger.info(f"Closed {closed_count} connections from pool")
    
    def get_pool_status(self):
        """Get current pool status."""
        return {
            "pool_size": self.pool_size,
            "available": self.connections.qsize(),
            "in_use": self.pool_size - self.connections.qsize(),
            "closed": self._closed
        }


# Global pool instance (singleton)
_SQL_POOL: Optional[SQLWarehousePool] = None
_SQL_POOL_LOCK = threading.Lock()


def get_sql_pool() -> SQLWarehousePool:
    """Get or create the global SQL connection pool."""
    global _SQL_POOL
    
    if _SQL_POOL is None:
        with _SQL_POOL_LOCK:
            if _SQL_POOL is None:
                pool_size = int(os.getenv("SQL_POOL_SIZE", "3"))
                _SQL_POOL = SQLWarehousePool(pool_size=pool_size)
                logger.info(f"Initialized SQL connection pool with size {pool_size}")
    
    return _SQL_POOL


def run_sql_with_pool(sql_query: str) -> Optional[list]:
    """
    Execute SQL query using connection pool.
    
    Args:
        sql_query: SQL query to execute
        
    Returns:
        List of rows or None on failure
    """
    pool = get_sql_pool()
    conn = pool.get_connection(timeout=5.0)
    
    if conn is None:
        logger.error("Failed to get connection from pool")
        return None
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return None
    finally:
        pool.return_connection(conn)
