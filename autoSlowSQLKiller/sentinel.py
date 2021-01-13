# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/1/11

import logging
import re
import enum

from apscheduler.schedulers.background import BlockingScheduler

from .query import query as _query


logger = logging.getLogger("sql.sentinel")


LOW_QUERY = """
            SELECT
                datname,
                procpid,
                sess_id,
                usename,
                current_query,
                query_start,
                round((extract(epoch from (current_timestamp - query_start)))::numeric, 2)  as timeuse
            FROM
                pg_stat_activity
            where 
                current_query <> '<IDLE>'
            and current_query like '%{query_label}%'
"""
CANCEL_QUERY = """select pgadmin.cancel_process({procpid})"""
TERMINATE_QUERY = """select pgadmin.terminate_process({procpid})"""


class Strategy(enum.Enum):
    mmin = min
    mmax = max


class Sentinel:
    """
    巡检，用于自动 kill 带有允许kill标志的属于自己的 SQL 查询
    """
    url: str = None
    mock: bool = True
    strategy: Strategy = Strategy.mmin
    ALLOW_KILL: str = "allow-kill"
    ALLOW_MAX_ETL_SECONDS: str = "allow-max-etl-seconds"

    DEFAULT_ALLOW_MAX_ETL_SECONDS: int = 900
    GLOBAL_ALLOW_MAX_ETL_SECONDS: int = 1800

    def _check(self, query_label, min_query_label_length=10):
        query_label = query_label.strip()
        if not query_label.startswith("=="):
            return False
        if len(query_label) < min_query_label_length:
            return False
        return True

    def look(self, statement):
        allow_kills = re.findall(rf"{self.ALLOW_KILL}=(.+?)[,，\s=]", statement, flags=re.I | re.M)
        allow_max_etl_seconds = re.findall(rf"{self.ALLOW_MAX_ETL_SECONDS}=(.+?)[,，\s=]", statement, flags=re.I | re.M)

        if not allow_kills:
            return False, None
        
        for allow_kill in allow_kills:
            if not allow_kill == "True":
                return False, None
        
        if not allow_max_etl_seconds:
            allow_max_etl_seconds = [self.DEFAULT_ALLOW_MAX_ETL_SECONDS]

        this_allow_max_etl_second = min(
            int(self.strategy.value(allow_max_etl_seconds, key=int)),
            self.GLOBAL_ALLOW_MAX_ETL_SECONDS
        )
        return True, this_allow_max_etl_second

    def __init__(self):
        self.query = _query(url=self.url)

    def find(self, query_label):
        if not self._check(query_label) and not self.mock:
            raise ValueError("query label length is too short")
        low_query = LOW_QUERY.format(query_label=query_label)
        rs = self.query(low_query)
        return rs

    def kill(self, procpid):
        query = self.query
        if self.mock:
            query = lambda *args, **kwargs: None

        query(CANCEL_QUERY.format(procpid=procpid))
        logger.warning("%s %s", ["[CANCEL]", "[MOCK]"][self.mock], CANCEL_QUERY.format(procpid=procpid))
        query(TERMINATE_QUERY.format(procpid=procpid))
        logger.warning("%s %s", ["[TERMINATE]", "[MOCK]"][self.mock], TERMINATE_QUERY.format(procpid=procpid))


    def pipe(self, query_label):
        for r in self.find(query_label):
            allow_kill, allow_max_etl_seconds = self.look(r.current_query)
            if allow_kill and r.timeuse > allow_max_etl_seconds:
                logger.warning("%ssql: %s, time use: %s", ["", "[MOCK]"][self.mock], r.current_query, r.timeuse)
                self.kill(r.procpid)
