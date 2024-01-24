from setuptools import setup

with open('README.md', 'r') as oF:
	long_description=oF.read()

setup(
	name='record-redis',
	version='1.0.0',
	description='Provides caching of records using Redis',
	long_description=long_description,
	long_description_content_type='text/markdown',
	url='https://ouroboroscoding.com/record-redis',
	project_urls={
		'Documentation': 'https://ouroboroscoding.com/record-redis',
		'Source': 'https://github.com/ouroboroscoding/record-redis-python',
		'Tracker': 'https://github.com/ouroboroscoding/record-redis-python/issues'
	},
	keywords=['data','define','cache','record'],
	author='Chris Nasr - Ouroboros Coding Inc.',
	author_email='chris@ouroboroscoding.com',
	license='MIT',
	packages=['record_redis'],
	python_requires='>=3.10',
	install_requires=[
		"jsonb>=1.0.0,<1.1",
		"record-oc>=1.0.0,<1.1",
		"namedredis>=1.0.1,<1.1",
		"tools-oc>=1.2.2,<1.3"
	],
	test_suite='tests',
	zip_safe=True
)