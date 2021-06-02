# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/6/2

import re
import logging
import itertools

import click
import flashtext
from sqlparse import keywords


COLORS = set(re.findall(r"\* ``(.+)?``", click.style.__doc__)) - {"reset"}


class ColordHandlerFlashtext(logging.StreamHandler):
    _mapping = {
        k.lower(): v
        for k, v in zip(
            itertools.chain(keywords.KEYWORDS_COMMON, keywords.KEYWORDS, {"<", ">", "=", ">=", "<=", "substr"}),
            itertools.cycle(COLORS)
        )
    }
    _terminator = "\n"
    kp = flashtext.KeywordProcessor()

    def emit(self, record):
        msg = self.format(record)
        for keyword, color in self._mapping.items():
            self.kp.add_keyword(keyword, click.style(keyword.upper(), bold=True, fg=color))
        msg = self.kp.replace_keywords(msg)
        self.stream.write(msg + self._terminator)
        self.flush()


def _test():
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
    c = ColordHandlerFlashtext()
    c.setFormatter(formatter)
    logger.addHandler(c)
    logger.setLevel(logging.DEBUG)

    logger.info("""
                SELECT DISTINCT
                    t1.username,
                    t2.tag as tag
                FROM
                    (
                    select * from mm.user t
                    where substr(created_date::varchar, 0, 10) <= '2020-12-20' 
                    ) t
                LEFT JOIN 
                    mm.tag t1 ON t.tag_id = t1.id
    """)


if __name__ == '__main__':

    _test()
 
