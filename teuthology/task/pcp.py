# maybe run pcp role?
# capture time at start
# capture time at stop
# record time range + hosts
# locate pcp archive
# generate graphs
import jinja2
import logging
import os
import requests
import time
import urllib

from teuthology.config import config as teuth_config

from . import Task

log = logging.getLogger(__name__)


class Grapher(object):
    def __init__(self, hosts, time_from, time_until='now'):
        self.base_url = "{host}{endpoint}".format(
            host=teuth_config.pcp_host,
            endpoint=self._endpoint,
        )
        self.hosts = hosts
        self.time_from = time_from
        self.time_until = time_until


class GraphiteGrapher(Grapher):
    defaults = dict(
        width='1200',
        height='300',
        hideLegend='false',
        format='png',
    )
    _endpoint = '/graphite/render'

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
        globs = ['*{}*'.format(host) for host in self.hosts]
        if metric:
            globs = ['{}.{}'.format(glob, metric) for glob in globs]
        return globs


class PCP(Task):
    metrics = [
        'kernel.all.load.1 minute',
        'mem.util.free',
        'mem.util.used',
        'network.interface.*.bytes.*',
        'disk.all.read_bytes',
        'disk.all.write_bytes',
    ]

    def __init__(self, ctx, config):
        super(PCP, self).__init__(ctx, config)
        self.log = log
        # until the job stops, we may want to render graphs reflecting the most
        # current data
        self.stop_time = 'now'

    def setup(self):
        super(PCP, self).setup()
        if not self.ctx.archive:
            return
        self.out_dir = os.path.join(
            self.ctx.archive,
            'pcp_graphs',
        )
        os.mkdir(self.out_dir)

    def begin(self):
        self.start_time = int(time.time())
        log.debug("cluster: %s", self.cluster)
        log.debug("start_time: %s", self.start_time)
        self.build_graph_urls()
        self.write_html()

    def end(self):
        self.stop_time = int(time.time())
        log.debug("stop_time: %s", self.stop_time)
        self.build_graph_urls()
        self.download_graphs()
        self.write_html(mode='static')

    def build_graph_urls(self):
        hosts = [rem.shortname for rem in self.cluster.remotes.keys()]
        self.grapher = GraphiteGrapher(
            hosts=hosts,
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
        for metric in self.graphs.keys():
            url = self.graphs[metric]['url']
            filename = self._sanitize_metric_name(metric) + '.png'
            self.graphs[metric]['file'] = graph_path = os.path.join(
                self.out_dir,
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

    def write_html(self, mode='dynamic'):
        if not self.ctx.archive:
            return
        generated_html = self.generate_html(mode=mode)
        html_path = os.path.join(self.out_dir, 'pcp.html')
        with open(html_path, 'w') as f:
            f.write(generated_html)

    def generate_html(self, mode='dynamic'):
        cwd = os.path.dirname(__file__)
        loader = jinja2.loaders.FileSystemLoader(cwd)
        env = jinja2.Environment(loader=loader)
        template = env.get_template('pcp.j2')
        log.debug(str(self.ctx.config))
        data = template.render(
            job_id=self.ctx.config.get('job_id'),
            graphs=self.graphs,
            mode=mode,
        )
        return data


task = PCP
