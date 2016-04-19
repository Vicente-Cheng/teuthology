# maybe run pcp role?
# capture time at start
# capture time at stop
# record time range + hosts
# locate pcp archive
# generate graphs
import logging
import os
import requests
import time
import urllib

from . import Task

log = logging.getLogger(__name__)


class Grapher(object):
    base_url = "http://pcp.front.sepia.ceph.com:44323/graphite/render"

    defaults = dict(
        width='1200',
        height='300',
        hideLegend='false',
        format='png',
    )

    def __init__(self, remotes, time_from, time_until):
        self.remotes = remotes
        self.time_from = time_from
        self.time_until = time_until

    def get_graph_url(self, metric):
        config = dict(self.defaults)
        config.update({
            'from': self.time_from,
            'until': self.time_until,
            # urlencode with doseq=True encodes each item as a separate
            # 'target=' arg
            'target': self.get_target_globs(metric),
        })
        args = urllib.urlencode(config, doseq=True)
        template = "{base_url}?{args}"
        return template.format(base_url=self.base_url, args=args)

    def get_target_globs(self, metric=''):
        globs = ['*{}*'.format(rem.shortname) for rem in self.remotes]
        if metric:
            globs = ['{}.{}'.format(glob, metric) for glob in globs]
        return globs


class PCP(Task):
    metrics = [
        'kernel.all.load.1 minute',
        'mem.util.free',
        'network.interface.*.bytes.*',
        'disk.all.read_bytes',
        'disk.all.write_bytes',
    ]

    def __init__(self, ctx, config):
        super(PCP, self).__init__(ctx, config)
        self.log = log

    def setup(self):
        super(PCP, self).setup()

    def begin(self):
        self.start_time = int(time.time())
        log.debug("cluster: %s", self.cluster)
        log.debug("start_time: %s", self.start_time)

    def end(self):
        self.stop_time = int(time.time())
        log.debug("stop_time: %s", self.start_time)
        self.get_graph_urls()
        self.download_graphs()

    def get_graph_urls(self):
        self.grapher = Grapher(
            remotes=self.cluster.remotes.keys(),
            time_from=self.start_time,
            time_until=self.stop_time,
        )
        self.graphs = dict()
        for metric in self.metrics:
            self.graphs[metric] = dict(
                url=self.grapher.get_graph_url(metric),
            )

    def download_graphs(self):
        if not self.ctx.archive:
            return
        graphs_dir = os.path.join(
            self.ctx.archive,
            'pcp_graphs',
        )
        os.mkdir(graphs_dir)
        for metric in self.graphs.keys():
            url = self.graphs[metric]['url']
            filename = self._sanitize_metric_name(metric) + '.png'
            self.graphs[metric]['file'] = graph_path = os.path.join(
                graphs_dir,
                filename,
            )
            resp = requests.get(url)
            if not resp.ok:
                log.warn(
                    "Graph download failed with error %s %s: %s",
                    resp.status_code,
                    resp.reason,
                    url,
                )
                continue
            with open(graph_path, 'wb') as f:
                f.write(resp.content)

    @staticmethod
    def _sanitize_metric_name(metric):
        result = metric
        replacements = [
            (' ', '_'),
            ('*', '_all_'),
        ]
        for rep in replacements:
            result = result.replace(rep[0], rep[1])
        return result


task = PCP
