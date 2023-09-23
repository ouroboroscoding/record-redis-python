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
import undefined

# Python imports
from typing import List, Literal, Union

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
		try: self.ttl = int(conf['ttl'], 10)
		except KeyError: self.ttl = 0

		# Get the redis connection
		self.redis = nr(conf['redis'])

	def fetch(self,
		_id: str | List[str]
	) -> None | Literal[False] | dict | List[Union[None, Literal[False], dict]]:
		"""Fetch

		Fetches one or more records from the cache. If a record does not \
		exist, None is returned, if the record has previously been marked as \
		missing, False is returned, else the dict of the record is returned. \
		In the case of fetching multiple IDs, a list is returned with the same \
		possible types, False, None, or dict

		Arguments
			_id (str | str[]): One or more IDs to fetch from the cache

		Returns:
			None | False | dict | List[None | False | dict]
		"""

		# If we have one ID
		if isinstance(_id, str):

			# Try to fetch it from the cache
			sRecord = self.redis.get(_id)

			# If it's found
			if sRecord:

				# If it's 0
				if sRecord == '0':
					return False

				# Decode and return the data
				return jsonb.decode(sRecord)

			# Return failure
			return None

		# Else, we have multiple records to fetch
		lRecords = self.redis.mget(_id)

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

		# If we have a ttl, use it
		if self.ttl:
			self.redis.setex(
				_id,
				self.ttl,
				jsonb.encode(record)
			)

		# Else, put it in the cache forever
		else:
			self.redis.set(
				_id,
				jsonb.encode(record)
			)

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
			ttl = self.ttl

		# If we have one item only
		if iLen == 1:

			# If we have a ttl, use it
			if ttl:
				return self.redis.setex(lIDs[0], ttl, '0')

			# Else, put it in the cache forever
			else:
				return self.redis.set(lIDs[0], '0')

		# Else, open a pipeline and loop through each
		else:

			# Get the pipeline
			oPipe = self.redis.pipeline()

			# Go through each ID
			for sID in lIDs:

				# If we have a ttl, use it
				if ttl:
					oPipe.setex(sID, ttl, '0')

				# Else, put it in the cache forever
				else:
					oPipe.set(sID, '0')

			# Execute all statements
			return oPipe.execute()