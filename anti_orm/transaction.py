from asyncio import gather

class BaseTransaction:

	async def commit(self):
		pass

	async def rollback(self):
		pass

class Transaction(BaseTransaction):

	def __init__(self):
		self.sources = []

	def add_source(self, source):
		if source not in self.sources:
			self.sources.append(source)

	async def commit(self):
		try:
			await self.flush()
		except Exception:
			await self.rollback()
			raise
		try:
			await self.do_commit()
		finally:
			await self.release()

	async def do_commit(self):
		pass

	async def rollback(self):
		try:
			await self.do_rollback()
		finally:
			await self.release()

	async def do_rollback(self):
		pass

	async def flush(self):
		await gather(*(source.flush() for source in self.sources))

	async def release(self):
		await gather(*(source.release() for source in self.sources))