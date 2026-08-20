import asyncio
import asyncpg
import logging
from utils.logger import setup_logger



setup_logger()
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, 
        host: str = "localhost",          # Server address (e.g., 'localhost' or an IP)
        database: str = "your_database",   # Database name
        user: str = "your_username",       # PostgreSQL username
        password: str = "your_password",   # PostgreSQL password
        port: str = "5432"  
    ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.create_connection()
    

    async def create_connection(self):
        '''
        Establishes a connection to the PostgreSQL database using asyncpg.
        '''

        conn = await asyncpg.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.connection = conn
        return conn


    async def get_connection(self):
        if not self.connection:
            await self.create_connection()
        return self.connection
        

    async def execute_query(self, query: str, params: str = None):
        '''
        Executes a query that modifies data (e.g., INSERT, UPDATE, DELETE).
        For SELECT queries, use fetch_one or fetch_all instead.
        '''
        if not self.connection:
            await self.create_connection()
        try:
            cursor = await self.connection.cursor()
            await cursor.execute(query, params)
            await self.connection.commit()
        except Exception as e:
            logger.exception(f"Error executing query: {e}")
            raise e
    

    async def fetch_one(self, query: str, params: str = None):
        '''
        Executes a SELECT query and returns a single result.
        '''
        if not self.connection:
            await self.create_connection()
        try:            
            cursor = await self.connection.cursor()
            await cursor.execute(query, params)
            result = await cursor.fetchone()
            return result
        except Exception as e:
            logger.exception(f"Error fetching data: {e}")
            raise e


    async def fetch_all(self, query: str, params: str = None):
        '''
        Executes a SELECT query and returns all results.
        '''
        if not self.connection:
            await self.create_connection()
        try:
            cursor = await self.connection.cursor()
            await cursor.execute(query, params)
            result = await cursor.fetchall()
            return result
        except Exception as e:
            logger.exception(f"Error fetching data: {e}")
            raise e