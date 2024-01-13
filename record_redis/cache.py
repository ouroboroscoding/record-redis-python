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

# Pip imports
from redis.exceptions import ResponseError

# Python imports
from typing import List, Union

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

	def __init__(self, name: str, conf: dict):
		"""Constructor

		Used to create a new instance of the Redis Cache

		Arguments:
			name (str): The unique name of the record instance
			conf (dict): Configuration data from the Record instance

		Returns:
			RedisCache
		"""

		# Call the parent init
		super().__init__(name, conf)

		# Store the time to live if there is one, otherwise, assume records
		#	never expire
		try: self._ttl = int(conf['redis']['ttl'])
		except KeyError: self._ttl = 0
		except TypeError: self._ttl = 0

		# Get the redis connection
		self._redis = nr(conf['redis']['name'])

		# Add the lua script for fetching secondary indexes
		self._get_secondary = self._redis.register_script(_GET_SECONDARY)

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

	def get(self,
		_id: str | tuple | List[str] | List[tuple],
		index = undefined
	) -> None | False | dict | List[Union[None, False, dict]]:
		"""Get

		Fetches one or more records from the cache. If a record does not \
		exist, None is returned, if the record has previously been marked as \
		missing, False is returned, else the dict of the record is returned. \
		An alternate index can be used to fetch the data, assuming the index \
		is handled by the implementation. In the case of fetching multiple \
		IDs, a list is returned with the same possible types: False, None, or \
		dict

		Arguments:
			_id (str | str[] | tuple | tuple[]): One or more IDs to fetch from \
				the cache
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
			sKey = '%s:%s:%s' % (self._name, index, ':'.join(_id))

			# Fetch the data using the secondary index
			try:
				sRecord = self._get_secondary(keys=[sKey])
			except ResponseError as e:
				return None

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
				try:
					sRecord = self._get_secondary(keys=['%s:%s:%s' % (
						self._name, index, _id
					)])
				except ResponseError as e:
					return None

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
				sKey = '%s:%s:%s' % (
					self._name,
					index,
					isinstance(m, tuple) and ':'.join(m) or m
				)

				# Fetch the secondary index (via the pipeline)
				self._get_secondary(keys=[sKey], args=[], client=oPipe)

			# Execute the pipeline
			try:
				lRecords = oPipe.execute()
			except ResponseError as e:
				return None

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

	def set(self,
		_id: str,
		data: dict
	) -> bool:
		"""Store

		Stores the data under the given ID in the cache. Also stores \
		additional indexes if passed

		Arguments:
			_id (str): The ID to store the data under
			data (dict): The data to store under the ID

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
				jsonb.encode(data),
				ex = self._ttl or None
			)

			# Go through each index
			for s,l in self._indexes.items():

				# Generate the index key using the values in the associated
				#	fields of the record
				sKey = '%s:%s:%s' % (self._name, s, ':'.join([
					data[s] for s in l
				]))

				# Set the ID under the key
				oPipe.set(sKey, _id, ex = self._ttl or None)

			# Execute the pipeline
			return oPipe.execute()

		# Else, we just have the primary record
		else:
			self._redis.set(
				_id,
				jsonb.encode(data),
				ex = self._ttl or None
			)