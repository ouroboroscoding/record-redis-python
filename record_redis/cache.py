# coding=utf8
"""Record Redis Cache

Extends the base Cache class in order to add Redis as an option
"""
from __future__ import annotations

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-08-26"

# Ouroboros imports
import jsonb
from nredis import nr
from record import Cache
from tools import evaluate
import undefined

# Python imports
from typing import List, Literal, Union

# Constants
_GET_SECONDARY = """
local primary = redis.call('GET', KEYS[1])
return redis.call('GET', primary)
"""

class RedisCache(Cache):
	"""Redis Cache

	Extends Cache to add support for Redis when caching Records

	Extends:
		Cache
	"""

	def __init__(self, conf: dict):
		"""Constructor

		Used to create a new instance of the Redis Cache

		Arguments:
			conf (dict): Configuration data from the Record instance

		Returns:
			RedisCache
		"""

		# Store the time to live if there is one, otherwise, assume records
		#	never expire
		try: self._ttl = int(conf['ttl'])
		except KeyError: self._ttl = 0
		except TypeError: self._ttl = 0

		# Get the redis connection
		self._redis = nr(conf['redis'])

		# Add the lua script for fetching secondary indexes
		self._get_secondary = self._redis.register_script(_GET_SECONDARY)

		# Init the indexes
		self._indexes = {}

		# If there's any indexes
		if 'indexes' in conf:

			# If it's not a list
			if not isinstance(conf['indexes'], list):
				raise ValueError(
					'conf.indexes',
					'Cache config indexes must be dict[]'
				)

			# Go through each one
			for i in range(len(conf['indexes'])):

				# If it's not a dict
				if not isinstance(conf['indexes'][i], dict):
					raise ValueError(
						'conf.indexes[%d]' % i,
						'Cache config indexes must be dict'
					)

				# Look for missing info
				try: evaluate(conf['indexes'][i], ['fields', 'name'])
				except ValueError as e:
					raise ValueError('conf.indexes[%d]' % i, e.args)

				# Make sure the fields are a string or a list
				if not isinstance(conf['indexes'][i]['fields'], list):

					# If it's a string, make it a list of one
					if isinstance(conf['indexes'][i]['fields'], str):
						conf['indexes'][i]['fields'] = [
							conf['indexes'][i]['fields']
						]

					# Else, it's invalid
					else:
						raise ValueError(
							'conf.indexes[%d].fields' % i,
							'Cache config index fields must be str | str[]'
						)

				# Store the index
				self._indexes[conf['indexes'][i]['name']] = \
					conf['indexes'][i]['fields']

	def add_missing(self, _id: str | List[str], ttl = undefined) -> bool:
		"""Add Missing

		Used to mark one or more IDs as missing from the DB so that they are \
		not constantly fetched over and over

		Arguments:
			_id (str | str[]): The ID(s) of the record that is missing
			ttl (int): Optional, used to set the ttl for this record. By \
						default the ttl used is the same as stored records

		Returns:
			bool | bool[]
		"""

		# Get the length
		try:
			iLen = len(_id)
			lIDs = _id
		except TypeError:
			iLen = 1
			lIDs = [_id]

		# If ttl is not set, use the instance one
		if ttl is undefined:
			ttl = self._ttl

		# If we have one item only, set it
		if iLen == 1:
			return self._redis.set(lIDs[0], '0', ex = ttl or None)

		# Else, open a pipeline and loop through each
		else:

			# Get the pipeline
			oPipe = self._redis.pipeline()

			# Go through each ID and set it
			for sID in lIDs:
				oPipe.set(sID, '0', ex = ttl or None)

			# Execute all statements
			return oPipe.execute()

	def fetch(self,
		_id: str | tuple | List[str] | List[tuple],
		index = undefined
	) -> None | False | dict | List[None, False, dict]:
		"""Fetch

		Fetches one or more records from the cache. If a record does not \
		exist, None is returned, if the record has previously been marked as \
		missing, False is returned, else the dict of the record is returned. \
		An alternate index can be used to fetch the data, assuming the index \
		is handled by the implementation. In the case of fetching multiple \
		IDs, a list is returned with the same possible types: False, None, or \
		dict

		Arguments:
			_id (str | str[]): One or more IDs to fetch from the cache
			index (str): An alternate index to use to fetch the record

		Returns:
			None | False | dict | List[None | False | dict]
		"""

		# If we have an index and it doesn't exist
		if index is not undefined and index not in self._indexes:
			raise ValueError('index', 'No such index "%s"' % index)

		# If we have a single tuple
		if isinstance(_id, tuple):

			# If there's no index
			if index is undefined:
				raise ValueError(
					'_id',
					'tuples can only be used when fetching a secondary index'
				)

			# Generate the key
			sKey = '%s:%s' % (index, ':'.join(_id))

			# Fetch the data using the secondary index
			sRecord = self._get_secondary(keys=[sKey])

			# If it's found
			if sRecord:

				# If it's 0
				if sRecord == '0':
					return False

				# Decode and return the data
				return jsonb.decode(sRecord)

			# Return failure
			return None

		# If we have one ID
		elif isinstance(_id, str):

			# If we have an index
			if index:
				sRecord = self._get_secondary(keys=['%s:%s' % (index, _id)])

			# Else, use the key as is
			else:
				sRecord = self._redis.get(_id)

			# If it's found
			if sRecord:

				# If it's 0
				if sRecord == '0':
					return False

				# Decode and return the data
				return jsonb.decode(sRecord)

			# Return failure
			return None

		# Else, we are looking for multiple records,
		#	Do we have an index?
		if index:

			# Create a pipeline
			oPipe = self._redis

			# Go through each ID
			for m in _id:

				# Generate the key
				sKey = '%s:%s' % (
					index,
					isinstance(m, tuple) and ':'.join(m) or m
				)

				# Fetch the secondary index (via the pipeline)
				self._get_secondary(keys=[sKey], args=[], client=oPipe)

			# Execute the pipeline
			lRecords = oPipe.execute()

		# Else, just use regular multi-get
		else:
			lRecords = self._redis.mget(_id)

		# Go through each one
		for sID in range(len(_id)):

			# If we have a record
			if lRecords[sID]:

				# If it's 0, set it to False
				if lRecords[sID] == '0':
					lRecords[sID] = False

				# Else, decode it
				else:
					lRecords[sID] = jsonb.decode(lRecords[sID])

		# Return the list
		return lRecords

	def store(self, _id: str, record: dict) -> bool:
		"""Store

		Stores a single record in the Cache based on the instances ttl

		Arguments:
			_id (str): The ID of the record to store
			record (dict): The data to store in the cache

		Returns:
			bool
		"""

		# If we have additional indexes
		if self._indexes:

			# Create a pipeline
			oPipe = self._redis.pipeline()

			# Add the primary record
			oPipe.set(
				_id,
				jsonb.encode(record),
				ex = self._ttl or None
			)

			# Go through each index
			for d in self._indexes:

				# Generate the index key using the values in the associated
				#	fields of the record
				sKey = '%s:%s' % (d['name'], ':'.join([
					record[s] for s in d['fields']
				]))

				# Add the ID under the key
				oPipe.set(sKey, _id, ex = self._ttl or None)

			# Execute the pipeline
			return oPipe.execute()

		# Else, we just have the primary record
		else:
			self._redis.set(
				_id,
				jsonb.encode(record),
				ex = self._ttl or None
			)