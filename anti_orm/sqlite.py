from aiosqlite import connect
from .transaction import Transaction
from .data import DataSource, DataScope
from .storage import VoidStorage, WeakStorage

class SQLiteTransaction(Transaction):

	def __init__(self, connection):
		super().__init__()
		self.connection = connection

	@classmethod
	async def begin(cls, db):
		return cls(await connect(db))

	async def do_commit(self):
		await self.connection.commit()
		await self.connection.close()

	async def do_rollback(self):
		await self.connection.rollback()
		await self.connection.close()

class SQLiteSource(DataSource):

	@property
	def db(self):
		return self.transaction.connection

class SQLiteScope(DataScope):

	def __init__(self, connection_string):
		if not hasattr(self, 'storage'):
			self.storage = VoidStorage()
		super().__init__()
		self.storage.make_mapping(id = True)
		self.connection_string = connection_string

	@property
	def transaction_id(self):
		return f'aiosqlite://{self.connection_string}'

	async def create_transaction(self):
		return await SQLiteTransaction.begin(self.connection_string)

	def create_source(self, *args):
		return SQLiteSource(*args)

class SQLiteSingleClientScope(SQLiteScope):

	def __init__(self, connection_string):
		if not hasattr(self, 'storage'):
			self.storage = WeakStorage()
		super().__init__(connection_string)