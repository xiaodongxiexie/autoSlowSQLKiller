# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/7/13

import re
import logging


logger = logging.getLogger("sql.modify")


class SQL:

    _projectname: str = ""
    _allow_killed: bool = True
    _allow_max_etl_seconds: int = 300

    @classmethod
    def _build_header(cls) -> str:
        header: list = [
            f"-- ==allow-kill={cls._allow_killed}",
            f"projectname={cls._projectname}" if cls._projectname else "",
            f"allow-max-etl-seconds={cls._allow_max_etl_seconds}"
        ]
        return ", ".join(filter(None, header)) + "=="

    def __init__(self, sql: str, to_spark_read: bool = False):
        self.to_spark_read = to_spark_read
        self._origin_sql = sql
        self.sql = self.preprocess(sql)

    def preprocess(self, osql: str) -> str:
        sql = osql
        cleansql = sql.lstrip()
        if self.to_spark_read:
            if re.match("select", cleansql, re.I):
                sql = f"({self._origin_sql}) t"
                logger.info("[old sql]\n %s, \n[new sql]\n %s", osql, sql)

            if sql.startswith("("):
                sql = "(\n"  + SQL._build_header() + "\n" + sql[1:]
        else:
            sql = SQL._build_header() + "\n" + sql
        logger.info("[sql][add header] \n%s", sql)
        return sql
