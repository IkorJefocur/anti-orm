from .transaction import Transaction
from .storage import Storage
from .storage_transaction import StorageTransaction
from .storage_lock import StorageLock, StorageLockClient

class DataSource:

	def __init__(self, transaction, storage, lock):
		self.transaction = transaction
		transaction.add_source(self)

		self.storage = storage
		storage.restore = self.restore_cached

		self.lock = lock

	async def flush(self):
		pass

	async def release(self):
		pass

	def identify(self, obj):
		return self.storage.id.key_of(obj)

	async def restore(self, obj):
		return await self.storage(obj)

	async def writable(self, obj):
		await self.lock(obj).acquire()
		self.storage.take_writable(obj)
		return await self.restore(obj)

	async def restore_cached(self, obj):
		pass

class DataScope:

	transaction_id = object()
	deps = ()

	def __init__(self):
		self.storage_lock_val = None
		if not hasattr(self, 'storage'):
			self.storage = Storage()
		self.storage.make_mapping(id = False)

	@property
	def storage_lock(self):
		if not self.storage_lock_val:
			self.storage_lock_val = StorageLock(self.storage)
		return self.storage_lock_val

	async def create_transaction(self):
		return Transaction()

	def create_storage(self):
		return StorageTransaction(self.storage)

	def create_lock(self):
		return StorageLockClient(self.storage_lock)

	def create_source(self, *args):
		return DataSource(*args)

	def lock(self, obj):
		return self.storage_lock(obj)