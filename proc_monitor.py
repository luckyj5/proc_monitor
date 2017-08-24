import time
import logging


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

    def run(self):
        while 1:
            try:
                self._do_run()
            except Exception:
                logging.exception('failed to collect metrics')
                break

    def _do_run(self):
        platform_info = self._stats.get_platform_info()

        events = []
        while 1:
            if 'system' in self._metrics:
                sys_metrics = self._stats.collect_system_stats()
                events.extend(
                    self._normalize_metrics(platform_info, sys_metrics))

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
        'hec_url': 'https://10.16.29.64:8088',
        'hec_token': '1CB57F19-DC23-419A-8EDA-BA545DD3674D',
        'interval': 2,
        'metrics': {
            'system': True,
            'processes': ['systemd', 'init'],
        }
    }
    monitor_proc(config)
