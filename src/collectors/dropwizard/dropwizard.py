# coding=utf-8

"""
Collect [dropwizard](http://dropwizard.codahale.com/) stats for the local node

#### Dependencies

 * urlib2

"""

import urllib2
import httplib

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector
from diamond.collector import str_to_bool


class DropwizardCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(DropwizardCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': "Hostname",
            'port': "Port number",
            'url_path': "Servlet url",
            'secure': "Enable https"
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(DropwizardCollector, self).get_default_config()
        config.update({
            'host':	'127.0.0.1',
            'port':	9091,
            'path':	'dropwizard',
            'secure':	'False',
            'url_path': 'metrics',
	    'collect_jvm': 'True',
	    'collect_jetty': 'True',
            'collect_logback': 'True',
            'timed':    ''
        })
        return config

    def collect(self):
        if json is None:
            self.log.error('Unable to import json')
            return {}
	if str_to_bool(self.config['secure']):
		proto = "https"
	else:
		proto = "http"

        url = '%s://%s:%i/%s' % (
		proto,
		self.config['host'],
		int(self.config['port']),
		self.config['url_path']
	)

        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError, err:
            self.log.error("HTTPError: %s, %s", url, err)
            return
	except urllib2.URLError, err:
            self.log.error("URLError %s, %s", url, err)
            return
	except httplib.HTTPException, err:
            self.log.error("HTTPException %s, %s", url, err)
            return

        try:
            result = json.load(response)
        except (TypeError, ValueError):
            self.log.error("Unable to parse response from metrics servlet as a"
                           + " json object")
            return

        metrics = {}

	# Jetty metrics	
	if str_to_bool(self.config['collect_jetty']):
        	jetty = result['org.eclipse.jetty.servlet.ServletContextHandler']
        	metrics['org.eclipse.jetty.servlet.ServletContextHandler.2xx-responses.1MinuteRate'] = jetty['2xx-responses']['m1']
        	metrics['org.eclipse.jetty.servlet.ServletContextHandler.4xx-responses.1MinuteRate'] = jetty['4xx-responses']['m1']
        	metrics['org.eclipse.jetty.servlet.ServletContextHandler.5xx-responses.1MinuteRate'] = jetty['5xx-responses']['m1']

	# Logback metrics	
	if str_to_bool(self.config['collect_logback']):
        	logback = result['ch.qos.logback.core.Appender']
        	metrics['ch.qos.logback.core.Appender.info.1MinuteRate'] = logback['info']['m1']
        	metrics['ch.qos.logback.core.Appender.warn.1MinuteRate'] = logback['warn']['m1']
        	metrics['ch.qos.logback.core.Appender.error.1MinuteRate'] = logback['error']['m1']

	# JVM metrics
	if str_to_bool(self.config['collect_jvm']):
        	memory = result['jvm']['memory']
        	mempool = memory['memory_pool_usages']
        	jvm = result['jvm']
        	thread_st = jvm['thread-states']

        	metrics['jvm.memory.totalInit'] = memory['totalInit']
        	metrics['jvm.memory.totalUsed'] = memory['totalUsed']
        	metrics['jvm.memory.totalMax'] = memory['totalMax']
        	metrics['jvm.memory.totalCommitted'] = memory['totalCommitted']
        	metrics['jvm.memory.totalInit'] = memory['totalInit']
        	metrics['jvm.memory.totalUsed'] = memory['totalUsed']
        	metrics['jvm.memory.totalMax'] = memory['totalMax']
        	metrics['jvm.memory.totalCommitted'] = memory['totalCommitted']
        	metrics['jvm.memory.heapInit'] = memory['heapInit']
        	metrics['jvm.memory.heapUsed'] = memory['heapUsed']
        	metrics['jvm.memory.heapMax'] = memory['heapMax']
        	metrics['jvm.memory.heapCommitted'] = memory['heapCommitted']
        	metrics['jvm.memory.heap_usage'] = memory['heap_usage']
        	metrics['jvm.memory.non_heap_usage'] = memory['non_heap_usage']

        	metrics['jvm.memory.code_cache'] = mempool['Code Cache']

		if 'Eden Space' in mempool:
        		metrics['jvm.memory.eden_space'] = mempool['Eden Space']
		elif 'PS Eden Space' in mempool:
        		metrics['jvm.memory.eden_space'] = mempool['PS Eden Space']
		else:
			metrics['jvm.memory.eden_space'] = 0

		if 'Perm Gen' in mempool:
			metrics['jvm.memory.perm_gen'] = mempool['Perm Gen']
		elif 'PS Perm Gen' in mempool:
			metrics['jvm.memory.perm_gen'] = mempool['PS Perm Gen']
		else:
			metrics['jvm.memory.perm_gen'] = 0

		if 'Survivor Space' in mempool:
			metrics['jvm.memory.survivor_space'] = mempool['Survivor Space']
		elif 'PS Survivor Space' in mempool:
			metrics['jvm.memory.survivor_space'] = mempool['PS Survivor Space']
		else:
			metrics['jvm.memory.survivor_space'] = 0

        	metrics['jvm.daemon_thread_count'] = jvm['daemon_thread_count']
        	metrics['jvm.thread_count'] = jvm['thread_count']
        	metrics['jvm.fd_usage'] = jvm['fd_usage']

        	metrics['jvm.thread_states.timed_waiting'] = thread_st['timed_waiting']
        	metrics['jvm.thread_states.runnable'] = thread_st['runnable']
        	metrics['jvm.thread_states.blocked'] = thread_st['blocked']
        	metrics['jvm.thread_states.waiting'] = thread_st['waiting']
        	metrics['jvm.thread_states.new'] = thread_st['new']
        	metrics['jvm.thread_states.terminated'] = thread_st['terminated']

        	metrics['jvm.daemon_thread_count'] = jvm['daemon_thread_count']
        	metrics['jvm.thread_count'] = jvm['thread_count']
        	metrics['jvm.fd_usage'] = jvm['fd_usage']

        	metrics['jvm.thread_states.timed_waiting'] = thread_st['timed_waiting']
        	metrics['jvm.thread_states.runnable'] = thread_st['runnable']
        	metrics['jvm.thread_states.blocked'] = thread_st['blocked']
        	metrics['jvm.thread_states.waiting'] = thread_st['waiting']
        	metrics['jvm.thread_states.new'] = thread_st['new']
        	metrics['jvm.thread_states.terminated'] = thread_st['terminated']

	# Timed metrics
	if self.config['timed']:
		for stat in self.config['timed'].split(" "):
			for resource in result[stat]:
				metrics['.'.join([stat, resource, "duration", "median"])] = result[stat][resource]['duration']['median']
				metrics['.'.join([stat, resource, "duration", "p98"])] = result[stat][resource]['duration']['p98']
				metrics['.'.join([stat, resource, "rate", "1MinuteRate"])] = result[stat][resource]['rate']['m1']

        for key in metrics:
            self.publish(key, metrics[key])
