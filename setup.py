from setuptools import setup, find_packages

setup(
	name = 'Anti ORM',
	packages = find_packages(),
	version = '0.1',
	description =
		'Database-agnostic data manipulation mechanisms'
		' and interfaces for ORM haters',
	author = 'Ikor Jefocur',
	author_email = 'ikor.jfcr@gmail.com',
	url = 'https://github.com/IkorJefocur/anti-orm',
	extras_require = {
		'sqlite': ['aiosqlite']
	}
)