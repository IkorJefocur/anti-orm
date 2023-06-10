from asyncio import gather
from .storage import MapAggregator, DoubleSideMapProxy

class StorageTransaction(MapAggregator):

	def __init__(self, storage, restore = None):
		self.maps = {
			name: IdentityMapTransaction(mapping)
				for name, mapping in storage.mapping()
		}
		super().__init__(**{
			name: StorageEntry(self, mapping)
				for name, mapping in self.maps.items()
		})

		self.existed = {}
		self.restored = set()
		self.restore = restore

	def __contains__(self, value):
		for mapping in self.maps.values():
			if mapping.has(value):
				return True
		return False

	def __iter__(self):
		seen = set()
		for mapping in self.maps.values():
			for value in mapping.values():
				if value not in seen:
					yield value
					seen.add(value)

	async def __call__(self, value):
		value = self.take(value)
		if not self.restore or value in self.restored:
			return value

		if value is not None:
			await self.restore(value)
			self.restored.add(value)
		return value

	def take(self, value):
		for mapping in self.maps.values():
			if mapping.has(value):
				return mapping[mapping.key_of(value)]
		return self.refresh(value)

	def all(self):
		for mapping in self.maps.values():
			mapping.take_all()
		return iter(self)

	def release(self, value):
		for mapping in self.maps.values():
			if mapping.has(value):
				mapping.release(mapping.key_of(value))
		del self.existed[value]

	def take_writable(self, value):
		value = self.refresh(value)
		if value is None:
			for mapping in self.maps.values():
				mapping.make_writable(mapping.key_of(value))
			return None

		public_value = Cloneable.optional_clone(value)
		self.existed[public_value] = value
		if value in self.restored:
			self.restored.add(public_value)

		for mapping in self.maps.values():
			if mapping.has(value):
				mapping[mapping.key_of(value)] = public_value

		return public_value

	def refresh(self, value):
		fresh_value = None

		for mapping in self.maps.values():
			key = mapping.key_of(value)
			if key in mapping and mapping[key] in self.existed:
				del self.existed[mapping[key]]

			mapping.take(key)
			if key in mapping:
				fresh_value = mapping[key]

		if fresh_value is not None:
			self.existed[fresh_value] = fresh_value
		return fresh_value

	def save(self, value, **keys):
		for map_name, key in keys.items():
			self.maps[map_name].insert(key, value)

		for map_name, mapping in self.maps.items():
			if map_name not in keys:
				mapping.add(value)

		self.restored.add(value)

	def remember(self, value, **keys):
		self.save(value, **keys)
		self.existed[value] = value

		for mapping in self.maps.values():
			if mapping.has(value):
				key = mapping.key_of(value)
				mapping.push(key)
				mapping.make_readonly(key)

	def delete(self, value):
		for mapping in self.maps.values():
			if mapping.has(value):
				mapping.remove(value)

	def new(self):
		for value in self:
			if value not in self.existed:
				yield value

	def deleted(self):
		for value in self.existed:
			if value not in self:
				yield value

	def tracked(self, value):
		return self.existed.get(value, value)

	def track(self, value):
		self.existed[value] = Cloneable.optional_clone(value)

	def track_delete(self, value):
		self.existed.pop(value, None)

	def flush(self):
		for mapping in self.maps.values():
			mapping.flush()

	def finish(self):
		for mapping in self.maps.values():
			mapping.finish()

class StorageEntry:

	def __init__(self, storage, mapping):
		self.storage = storage
		self.map = mapping

	def __getitem__(self, key):
		for subkey in self.untaken(key):
			self.map.take(subkey)
			if subkey in self.map:
				self.storage.refresh(self.map[subkey])
		return self.map.natural(key)

	def __contains__(self, key):
		for subkey in self.map.subkeys(key):
			return True
		return False

	async def __call__(self, key):
		value = self[key]
		await gather(*(
			self.storage(self.map[subkey])
				for subkey in self.map.subkeys(key)
				if subkey in self.map
		))
		return value

	def key_of(self, value):
		return self.map.key_of(value)

	def untaken(self, key):
		for subkey in self.map.subkeys(key):
			if subkey not in self.map:
				yield subkey

class IdentityMapTransaction(DoubleSideMapProxy):

	def __init__(self, identity_map):
		super().__init__(identity_map.empty_copy())
		self.taken = set()
		self.updated = set()
		self.global_map = identity_map

	def __setitem__(self, key, value):
		self.register(key)
		self.make_writable(key)
		super().__setitem__(key, value)

	def __delitem__(self, key):
		self.make_writable(key)
		super().__delitem__(key)

	def generate_key(self, value):
		local_key = super().generate_key(value)
		return local_key if local_key is not self.void \
			else self.global_map.key_of(value)

	def subkeys(self, main_key):
		return self.global_map.subkeys(main_key)

	def exists(self, key):
		return key in self.global_map

	def take(self, key):
		if self.exists(key):
			self.register(key)
			super().__setitem__(key, self.global_map[key])

	def take_all(self):
		for key in self.global_map:
			if key not in self.taken:
				self.take(key)

	def push(self, key):
		if key in self:
			self.global_map[key] = self[key]
		else:
			del self.global_map[key]

	def register(self, key):
		if key not in self.taken:
			self.global_map.take(key)
			self.taken.add(key)

	def make_readonly(self, key):
		self.updated.discard(key)

	def make_writable(self, key):
		if key is not self.void:
			self.updated.add(key)

	def release(self, key):
		self.global_map.release(key)
		self.taken.remove(key)
		self.pop(key, None)

	def flush(self):
		for key in self.updated:
			if key in self.taken:
				self.push(key)

	def finish(self):
		for key in [*self.taken]:
			self.release(key)

class Cloneable:

	@staticmethod
	def optional_clone(obj):
		return obj.clone() if isinstance(obj, Cloneable) else obj

	def clone(self):
		return Cloneable()