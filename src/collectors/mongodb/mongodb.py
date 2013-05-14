# coding=utf-8

"""
Collects all number values from the db.serverStatus() command, other
values are ignored.

#### Dependencies

 * pymongo

"""

import diamond.collector
import re

try:
    import pymongo
    pymongo  # workaround for pyflakes issue #13
except ImportError:
    pymongo = None

try:
    from pymongo import ReadPreference
    ReadPreference  # workaround for pyflakes issue #13
except ImportError:
    ReadPreference = None


class MongoDBCollector(diamond.collector.Collector):

    def __init__(self, *args, **kwargs):
        self.__totals = {}
        super(MongoDBCollector, self).__init__(*args, **kwargs)

    def get_default_config_help(self):
        config_help = super(MongoDBCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'Array of hostname(:port) elements to get metrics from',
            'host': 'A single hostname(:port) to get metrics from'
                    ' (can be used instead of hosts and overrides it)',
            'databases': 'A regex of which databases to gather metrics for.'
                         ' Defaults to all databases.',
            'ignore_collections': 'A regex of which collections to ignore.'
                                  ' MapReduce temporary collections (tmp.mr.*)'
                                  ' are ignored by default.',

        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MongoDBCollector, self).get_default_config()
        config.update({
            'path':      'mongo',
            'hosts':     ['localhost'],
            'databases': '.*',
            'ignore_collections': '^tmp\.mr\.',
        })
        return config

    def collect(self):
        """Collect number values from db.serverStatus()"""

        if pymongo is None:
            self.log.error('Unable to import pymongo')
            return {}

        # we need this for backwards compatibility
        if 'host' in self.config:
            self.config['hosts'] = [self.config['host']]

        for host in self.config['hosts']:
            if len(self.config['hosts']) == 1:
                # one host only, no need to have a prefix
                base_prefix = []
            else:
                base_prefix = [re.sub('[:\.]', '_', host)]

            try:
                if ReadPreference is None:
                    conn = pymongo.Connection(host)
                else:
                    conn = pymongo.Connection(
                        host,
                        read_preference=ReadPreference.SECONDARY)
            except Exception, e:
                self.log.error('Couldnt connect to mongodb: %s', e)
                return {}
            data = conn.db.command('serverStatus')
            self._publish_dict_with_prefix(data, base_prefix)
            self._publish_transformed(data, base_prefix)
            db_name_filter = re.compile(self.config['databases'])
            ignored_collections = re.compile(self.config['ignore_collections'])
            for db_name in conn.database_names():
                if not db_name_filter.search(db_name):
                    continue
                db_stats = conn[db_name].command('dbStats')
                db_prefix = base_prefix + ['databases', db_name]
                self._publish_dict_with_prefix(db_stats, db_prefix)
                for collection_name in conn[db_name].collection_names():
                    if ignored_collections.search(collection_name):
                        continue
                    collection_stats = conn[db_name].command('collstats',
                                                             collection_name)
                    collection_prefix = db_prefix + [collection_name]
                    self._publish_dict_with_prefix(collection_stats,
                                                   collection_prefix)

    def _publish_transformed(self, data, base_prefix):
        """ Publish values of type: counter or percent """
        self._publish_dict_with_prefix(data.get('opcounters', {}),
                                       base_prefix + ['opcounters_per_sec'],
                                       self.publish_counter)
        self._publish_dict_with_prefix(data.get('opcountersRepl', {}),
                                       base_prefix + ['opcountersRepl_per_sec'],
                                       self.publish_counter)
        self._publish_metrics(base_prefix + ['backgroundFlushing_per_sec'],
                              'flushes',
                              data.get('backgroundFlushing', {}),
                              self.publish_counter)
        self._publish_dict_with_prefix(data.get('network', {}),
                                       base_prefix + ['network_per_sec'],
                                       self.publish_counter)
        self._publish_metrics(base_prefix + ['extra_info_per_sec'],
                              'page_faults',
                              data.get('extra_info', {}),
                              self.publish_counter)

        def get_dotted_value(data, key_name):
            key_name = key_name.split('.')
            for i in key_name:
                data = data.get(i, {})
                if not data:
                    return 0
            return data

        def compute_interval(data, total_name):
            current_total = get_dotted_value(data, total_name)
            total_key = '.'.join(base_prefix) + '.' + total_name
            last_total = self.__totals.get(total_key, current_total)
            interval = current_total - last_total
            self.__totals[total_key] = current_total
            return interval

        def publish_percent(value_name, total_name, data):
            value = float(get_dotted_value(data, value_name) * 100)
            interval = compute_interval(data, total_name)
            key = '.'.join(base_prefix) + '.percent.' + value_name
            self.publish_counter(key, value, time_delta=bool(interval),
                                 interval=interval)

        publish_percent('globalLock.lockTime', 'globalLock.totalTime', data)
        publish_percent('indexCounters.btree.misses',
                        'indexCounters.btree.accesses', data)

        locks = data.get('locks')
        if locks:
            if '.' in locks:
                locks['_global_'] = locks['.']
                del (locks['.'])
            key_prefix = '.'.join(base_prefix) + '.percent.'
            db_name_filter = re.compile(self.config['databases'])
            interval = compute_interval(data, 'uptimeMillis')
            for db_name in locks:
                if not db_name_filter.search(db_name):
                    continue
                r = get_dotted_value(
                    locks,
                    '%s.timeLockedMicros.r' % db_name)
                R = get_dotted_value(
                    locks,
                    '.%s.timeLockedMicros.R' % db_name)
                value = float(r + R) / 10
                if value:
                    self.publish_counter(key_prefix + 'locks.%s.read' % db_name,
                                         value, time_delta=bool(interval),
                                         interval=interval)
                w = get_dotted_value(
                    locks,
                    '%s.timeLockedMicros.w' % db_name)
                W = get_dotted_value(
                    locks,
                    '%s.timeLockedMicros.W' % db_name)
                value = float(w + W) / 10
                if value:
                    self.publish_counter(
                        key_prefix + 'locks.%s.write' % db_name,
                        value, time_delta=bool(interval), interval=interval)

    def _publish_dict_with_prefix(self, dict, prefix, publishfn=None):
        for key in dict:
            self._publish_metrics(prefix, key, dict, publishfn)

    def _publish_metrics(self, prev_keys, key, data, publishfn=None):
        """Recursively publish keys"""
        if not key in data:
            return
        value = data[key]
        keys = prev_keys + [key]
        if not publishfn:
            publishfn = self.publish
        if isinstance(value, dict):
            for new_key in value:
                self._publish_metrics(keys, new_key, value)
        elif isinstance(value, int) or isinstance(value, float):
            publishfn('.'.join(keys), value)
        elif isinstance(value, long):
            publishfn('.'.join(keys), float(value))
