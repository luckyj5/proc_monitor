import time
import logging
import os


class ProcMonitor(object):

    def __init__(self, stats, sink, interval, metrics, normalize_metrics):
        '''
        @metrics: dict object
        {
            'system': True,
            'topn_process': n,
            'processes': [process name, process name, ...],
        }
        @normalize_metrics: function object to convert metric from stats to
        sink accepted format
        '''

        self._stats = stats
        self._sink = sink
        self._metrics = metrics
        self._interval = interval
        self._normalize_metrics = normalize_metrics
        self._done = False

    def run(self):
        while not self._done:
            try:
                self._do_run()
            except Exception:
                logging.exception('failed to collect metrics')
                time.sleep(1)

    def _do_run(self):
        platform_info = self._stats.get_platform_info()

        events = []
        while not self._done:
            if 'system' in self._metrics:
                sys_metrics = self._stats.collect_system_stats()
                events.extend(
                    self._normalize_metrics(platform_info, sys_metrics))

            if 'network_usage' in self._metrics:
                nw_metrics = self._stats.collect_network_io_stats()
                events.extend(
                    self._normalize_metrics(platform_info, nw_metrics))

            if 'topn_process' in self._metrics:
                topn_process_metrics = self._stats.collect_topn_process_stats(
                    self._metrics['topn_process'])
                events.extend(
                    self._normalize_metrics(
                        platform_info, topn_process_metrics))

            if 'processes' in self._metrics:
                process_metrics = self._stats.collect_process_stats(
                    self._metrics['processes'])
                events.extend(self._normalize_metrics(
                    platform_info, process_metrics))

            self._sink.write(events)
            del events[:]
            time.sleep(self._interval)

    def stop(self):
        self._done = True


def monitor_proc(config):
    '''
    @config: dict object
    {
        'hec_url': <url>,
        'hec_token': <token>,
        'interval': <interval>,
        'metrics': {
            'processes': [process name, ...],
        },
    }
    '''

    import pytop
    import splunk_hec

    def normalize_metrics(platform_info, metrics):
        metric_list = metrics
        if not isinstance(metrics, list):
            metric_list = [metrics]

        events = []
        for metric in metric_list:
            event = {
                'host': platform_info['hostname'],
                'source': 'proc_monitor',
                'sourcetype': 'metric',
                'event': metric,
            }
            events.append(event)
        return events

    sink = splunk_hec.HECWriter(config['hec_url'], config['hec_token'])
    monitor = ProcMonitor(
        pytop, sink, config['interval'], config['metrics'], normalize_metrics)
    monitor.run()


if __name__ == '__main__':
    config = {
        'hec_url': os.environ.get('SPLUNK_HOST', 'https://localhost:8088'),
        'hec_token': os.environ.get('SPLUNK_TOKEN', '00000000-0000-0000-0000-000000000000'),
        'interval': 2,
        'metrics': {
            'system': True,
            'network_usage': True,
            'processes': os.environ.get('TARGETS', 'splunk-firehose-nozzle').split(','),
        }
    }

    monitor_proc(config)
