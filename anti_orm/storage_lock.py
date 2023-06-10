from asyncio import Lock, gather
from weakref import WeakValueDictionary
from .storage import MapAggregator

VOID = object()

class LockMapAggregator(MapAggregator):

	def __call__(self, nkw__value = VOID, **keys):
		return CompositeLock(self, nkw__value, keys)

class StorageLock(LockMapAggregator):

	def __init__(self, storage):
		super().__init__(**{
			name: IdentityMapLock(mapping)
				for name, mapping in storage.mapping()
		})

class StorageLockClient(LockMapAggregator):

	def __init__(self, storage_lock):
		super().__init__(**{
			name: IdentityMapLockClient(lock)
				for name, lock in storage_lock.mapping()
		})

	def release_all(self):
		for mapping in self.entries.values():
			mapping.release_all()

class LockMap:

	def __init__(self, lock_or_identity_map):
		self.locks = WeakValueDictionary()
		self.source = lock_or_identity_map

	def __call__(self, key):
		if key is self.void:
			return CustomLock()
		lock = self.locks.get(key)
		if not lock:
			lock = self.create_lock(key)
			self.locks[key] = lock
		return lock

	def __getitem__(self, key):
		return self.source[key]

	def __contains__(self, key):
		return key in self.source

	@property
	def void(self):
		return self.source.void

	def key_of(self, value):
		return self.source.key_of(value)

	def create_lock(self, key):
		return CustomLock()

class IdentityMapLock(LockMap):

	def create_lock(self, key):
		return GlobalLock(self.source, key)

class IdentityMapLockClient(LockMap):

	def __init__(self, identity_map_lock):
		super().__init__(identity_map_lock)
		self.locks = {}

	def create_lock(self, key):
		return LockClient(self.source(key))

	def release_all(self):
		for lock in self.locks.values():
			lock.ensure_release()

class CustomLock(Lock):

	async def ensure_acquire(self):
		if not self.locked():
			await self.acquire()

	def ensure_release(self):
		if self.locked():
			self.release()

class GlobalLock(CustomLock):

	def __init__(self, mapping, key):
		super().__init__()
		self.map = mapping
		self.key = key

	async def acquire(self):
		self.map.take(self.key)
		await super().acquire()

	async def ensure_acquire(self):
		await self.acquire()

	def release(self):
		super().release()
		self.map.release(self.key)

class LockClient(CustomLock):

	def __init__(self, global_lock):
		super().__init__()
		self.context_locked = False
		self.local_lock = Lock()
		self.global_lock = global_lock

	async def __aenter__(self):
		self.context_locked = not self.local_lock.locked()
		await self.ensure_acquire()
		await super().acquire()

	async def __aexit__(self, *_):
		super().release()
		if self.context_locked:
			self.release()

	async def acquire(self):
		await super().acquire()
		await self.local_lock.acquire()
		await self.global_lock.acquire()
		super().release()

	async def ensure_acquire(self):
		if not self.local_lock.locked():
			await self.acquire()

	def release(self):
		self.local_lock.release()
		self.global_lock.release()

	def ensure_release(self):
		if self.local_lock.locked():
			self.release()

class CompositeLock(CustomLock):

	def __init__(self, lock_map_aggregator, value = VOID, keys = {}):
		super().__init__()
		self.locks = {}
		self.target = lock_map_aggregator
		self.value = value
		self.keys = {**keys}

	async def __aenter__(self):
		await super().acquire()
		self.update_locks()
		await gather(*(lock.__aenter__() for lock in self.locks.values()))

	async def __aexit__(self, *exc):
		await gather(*(lock.__aexit__(*exc) for lock in self.locks.values()))
		self.locks.clear()
		super().release()

	async def acquire(self):
		await super().acquire()
		self.update_locks()
		await gather(*(lock.ensure_acquire() for lock in self.locks.values()))

	def release(self):
		for lock in self.locks.values():
			lock.release()
		self.locks.clear()
		super().release()

	def update_locks(self):
		if self.value is VOID:
			for map_name, mapping in self.target.mapping():
				if map_name in self.keys and self.keys[map_name] in mapping:
					self.value = mapping[self.keys[map_name]]
					break

		if self.value is not VOID:
			for map_name, mapping in self.target.mapping():
				self.keys[map_name] = mapping.key_of(self.value)

		for map_name, key in self.keys.items():
			self.locks[map_name] = self.target[map_name](key)