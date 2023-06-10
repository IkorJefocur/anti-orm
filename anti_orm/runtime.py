from asyncio import gather
from .transaction import BaseTransaction
from .storage import Storage

class TransactionPool(BaseTransaction):

	def __init__(self, *scopes, write = set(), cache = None):
		super().__init__()
		self.transactions = {}
		self.cache = cache or OneTimeCache()
		self.lock_clients = {scope: scope.create_lock() for scope in scopes}
		self.sources = {scope: None for scope in scopes}
		self.to_write = {*write}

	async def __aenter__(self):
		self.cache.reload()
		global_storages = {scope.storage: scope for scope in self.sources}
		await gather(*(
			self.prepare_obj(obj, global_storages)
				for obj in self.to_write
		))

		result = []
		for scope in [*self.sources]:
			result.append(await self.create_source(scope))
		return result

	async def __aexit__(self, err_type, *_):
		try:
			if err_type:
				await self.rollback()
			else:
				await self.commit()
				self.cache.flush()

		finally:
			for lock in self.lock_clients.values():
				lock.release_all()

	async def create_source(self, scope):
		for dep in scope.deps:
			await self.create_source(dep)

		if scope.transaction_id not in self.transactions:
			self.transactions[scope.transaction_id] = \
				await scope.create_transaction()

		if scope not in self.sources or not self.sources[scope]:
			self.sources[scope] = scope.create_source(
				self.transactions[scope.transaction_id],
				self.cache.storage(scope),
				self.lock_clients.get(scope) or scope.create_lock(),
				*(self.sources[dep] for dep in scope.deps)
			)
		return self.sources[scope]

	async def prepare_obj(self, obj, global_storages):
		scope = global_storages[Storage.of(obj)]
		await self.lock_clients[scope](obj).acquire()
		self.cache.storage(scope).take_writable(obj)

	async def commit(self):
		await gather(*(trx.commit() for trx in self.transactions.values()))

	async def rollback(self):
		await gather(*(trx.rollback() for trx in self.transactions.values()))

class Cache:

	def __init__(self):
		self.storages = {}

	def __enter__(self):
		return self

	def __exit__(self, *_):
		self.clear()

	def storage(self, scope):
		if scope not in self.storages:
			self.storages[scope] = scope.create_storage()
		return self.storages[scope]

	def flush(self):
		for storage in self.storages.values():
			storage.flush()

	def reload(self):
		for scope, storage in self.storages.items():
			self.storages[scope] = scope.create_storage()
			for value in storage:
				self.storages[scope].take(value)
			storage.finish()

	def clear(self):
		for storage in self.storages.values():
			storage.finish()

class OneTimeCache(Cache):

	def flush(self):
		super().flush()
		self.clear()