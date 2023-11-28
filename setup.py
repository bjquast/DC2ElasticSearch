from setuptools import setup
import sys

# check for python version and define the right requirements
if sys.version_info < (3, 0):
	raise Exception ('python version must be >= 3.0')
else:
	pass

requires = [
	'configparser',
	'elasticsearch',
	'pyodbc',
	'pudb'
]

setup(name='DC2ElasticSearch',
	author='Björn Quast',
	author_email='b.quast@leibniz-lib.de',
	license='CC By 4.0',
	install_requires=requires
)
