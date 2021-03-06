# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2020/12/16

import logging

from autoSlowSQLKiller.sentinel import Sentinel

logger = logging.getLogger("sql")


"""
 -- forexample, you have a sql like this
 
  -- ==allow-kill=True, allow-max-etl-seconds=600==
  select xxx from xxxxx ...
"""
"""
your sql comment should like '==allow-kill=True' or you defined something startswith '==' 
as query label, by default query label must greater than 10 or get a ValueError if mock is False.
when mock set as True, then kill process action just be logging but not execute.
"""


if __name__ == '__main__':
 
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)

    sentinel = Sentinel()
    sentinel.url = "postgresql://user-name:password@your-host:5432/your-database"
    sentinel.mock = True
    sentinel.GLOBAL_ALLOW_MAX_ETL_SECONDS = 15

    sentinel.start(interval_seconds=5)
