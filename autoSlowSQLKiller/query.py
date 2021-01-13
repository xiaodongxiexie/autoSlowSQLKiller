# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/01/12

import logging

from records import Database

logger = logging.getLogger("sql.query")


def query(url, **kwargs):
    """
    ... example:
        query = query("mysql+pymysql//user:password@localhost:3306/database)
        data = query("select count(1) from table")
    """
    return Database(db_url=url, **kwargs).query
