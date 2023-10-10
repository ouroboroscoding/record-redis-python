# coding=utf8
"""Record Redis

Cache Record data using Redis
"""

__author__		= "Chris Nasr"
__copyright__	= "Ouroboros Coding Inc."
__email__		= "chris@ouroboroscoding.com"
__created__		= "2023-08-26"

# Limit imports (this module exports nothing)
__all__ = [ ]

# Local imports
from record_redis.cache import RedisCache

# Register itself
RedisCache.register('redis')