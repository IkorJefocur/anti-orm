from weakref import WeakKeyDictionary
from collections import UserDict

class MapAggregator:

	def __init__(self, **entries):
		self.entries = entries

	def __getattr__(self, map_name):
		return self[map_name]

	def __getitem__(self, map_name):
		return self.entries[map_name]

	def mapping(self):
		return self.entries.items()

class Storage(MapAggregator):

	values_storages = WeakKeyDictionary()

	def __init__(self, **maps):
		super().__init__()
		self.make_mapping(**maps)

	@classmethod
	def of(cls, value):
		return cls.values_storages.get(value)

	def bind(self, value):
		self.values_storages[value] = self

	def cache(self, value):
		pass

	def uncache(self, value):
		return False

	def make_mapping(self, **maps):
		for name, setup in maps.items():
			mapping = id_map_name = void = None
			if not isinstance(setup, tuple):
				setup = (setup,)
			if isinstance(setup[0], str):
				id_map_name, mapping = setup
			else:
				mapping = setup[0]
				void = setup[1] if len(setup) > 1 else None

			if isinstance(mapping, DoubleSideMap):
				self.entries[name] = IdentityMap(mapping, self)
			else:
				hash_fn = mapping if callable(mapping) \
					else None if mapping \
					else (lambda value: value)
				self.entries[name] = IdentityMap(DoubleSideCollectionMap(
					self[id_map_name], hash_fn
				) if id_map_name else DoubleSideMap(
					hash_fn, void
				), self)

	def add_mapping(self, **maps):
		self.make_mapping(**{
			name: setup for name, setup in maps if name not in self.entries
		})

class VoidStorage(Storage):

	def uncache(self, value):
		return True

class WeakStorage(Storage):

	def uncache(self, value):
		for mapping in self.entries.values():
			if mapping.taken_count(mapping.key_of(value)) > 0:
				return False
		for mapping in self.entries.values():
			if mapping.has(value):
				mapping.remove(value)
		return False

class DoubleSideMap(UserDict):

	def __init__(self, hash_fn = None, void = None):
		super().__init__()
		self.reverse = WeakKeyDictionary()
		self.hash = hash_fn
		self.void = void

	def __setitem__(self, key, value):
		if key is not self.void:
			self.data[key] = value
			self.reverse[value] = key

	def key_of(self, value):
		return self.reverse[value] if value in self.reverse \
			else self.generate_key(value)

	def generate_key(self, value):
		return self.hash(value) if self.hash else self.void

	def subkey(self, main_key, value):
		return main_key

	def subkeys(self, main_key):
		return [main_key] if main_key in self else []

	def natural(self, main_key):
		return self.get(main_key)

	def has(self, value):
		return self.key_of(value) in self

	def add(self, value):
		self[self.key_of(value)] = value

	def remove(self, value):
		del self[self.key_of(value)]

	def insert(self, main_key, value):
		self[self.subkey(main_key, value)] = value

	def empty_copy(self):
		return DoubleSideMap(self.hash, self.void)

class DoubleSideCollectionMap(DoubleSideMap):

	def __init__(self, unique_map, hash_fn = None):
		super().__init__(hash_fn, unique_map.void)
		self.id_map = unique_map

	def __getitem__(self, key):
		return self.data[key[0]][key[1]]

	def __setitem__(self, key, value):
		if self.void not in key:
			if key[0] not in self.data:
				self.data[key[0]] = {}
			self.data[key[0]][key[1]] = value
			self.reverse[value] = key

	def __delitem__(self, key):
		del self.data[key[0]][key[1]]
		if len(self.data[key[0]]) == 0:
			del self.data[key[0]]

	def __contains__(self, key):
		return isinstance(key, tuple) and len(key) == 2 \
			and key[0] in self.data and key[1] in self.data[key[0]]

	def __iter__(self):
		for key in self.data:
			yield from self.data[key]

	def __len__(self):
		return sum(len(collection) for collection in self.data.values())

	def key_of(self, value):
		return (super().key_of(value), self.id_map.key_of(value))

	def subkey(self, main_key, value):
		return (main_key, self.id_map.key_of(value))

	def subkeys(self, main_key):
		for subkey in self.data.get(main_key, []):
			yield (main_key, subkey)

	def natural(self, main_key):
		return self.data[main_key].values() if main_key in self.data else []

	def empty_copy(self):
		return DoubleSideCollectionMap(self.id_map, self.hash)

class DoubleSideMapProxy(DoubleSideMap):

	def __init__(self, original_map):
		super().__init__(original_map.hash, original_map.void)
		self.data = original_map

	def generate_key(self, value):
		return self.data.key_of(value)

	def subkey(self, main_key, value):
		return self.data.subkey(main_key, value)

	def subkeys(self, main_key):
		return self.data.subkeys(main_key)

	def natural(self, main_key):
		return self.data.natural(main_key)

	def empty_copy(self):
		return self.data.empty_copy()

class IdentityMap(DoubleSideMapProxy):

	def __init__(self, original_map, storage):
		super().__init__(original_map)
		self.taken = {}
		self.storage = storage

	def __setitem__(self, key, value):
		super().__setitem__(key, value)
		self.storage.bind(value)

	def key_of(self, value):
		return self.generate_key(value)

	def taken_count(self, key):
		return self.taken.get(key, 0)

	def take(self, key):
		self.taken[key] = self.taken_count(key) + 1
		if key in self:
			self.storage.cache(self[key])
			return self[key]

	def release(self, key):
		self.taken[key] -= 1
		if self.taken[key] == 0:
			del self.taken[key]
		if key in self:
			if self.storage.uncache(self[key]):
				del self[key]