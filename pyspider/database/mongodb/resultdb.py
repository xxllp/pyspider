#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-13 22:18:36

import json
import time
from pymongo import MongoClient
from pyspider.database.base.resultdb import ResultDB as BaseResultDB
from .mongodbbase import SplitTableMixin


class ResultDB(SplitTableMixin, BaseResultDB):
    collection_prefix = 'resultdb'

    def __init__(self, url, database='spiderdb'):
        self.conn = MongoClient(url)
        self.conn.admin.command("ismaster")
        self.database = self.conn[database]
        self.projects = set()

        self._list_project()

    def _parse(self, data):
        return data

    def _stringify(self, data):
        return data

    def save(self, project, taskid, url, result):
        collection_name = self.collection_prefix
        obj = {
            'taskid': taskid,
            'url': url,
            'result': result,
            'updatetime': time.time(),
        }
        return self.database[collection_name].update(
            {'taskid': taskid}, {"$set": self._stringify(obj)}, upsert=True
        )

    def select(self, project, fields=None, offset=0, limit=0):
        collection_name = self.collection_prefix
        for result in self.database[collection_name].find({}, fields, skip=offset, limit=limit):
            yield self._parse(result)

    def count(self, project):

        collection_name = self.collection_prefix
        return self.database[collection_name].count()

    def get(self, project, taskid, fields=None):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return
        collection_name = self.collection_prefix
        ret = self.database[collection_name].find_one({'taskid': taskid}, fields)
        if not ret:
            return ret
        return self._parse(ret)
