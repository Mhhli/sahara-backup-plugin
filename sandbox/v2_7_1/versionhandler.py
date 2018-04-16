# coding=utf-8

# Copyright (c) 2015 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from oslo_config import cfg
from oslo_log import log as logging

from sahara import conductor
from sahara import context
from sahara.plugins import utils
from sahara.plugins.sandbox import abstractversionhandler as avm
from sahara.plugins.sandbox.hadoop2 import config as c
from sahara.plugins.sandbox.hadoop2 import keypairs
from sahara.plugins.sandbox.hadoop2 import recommendations_utils as ru
from sahara.plugins.sandbox.hadoop2 import run_scripts as run
from sahara.plugins.sandbox.hadoop2 import scaling as sc
from sahara.plugins.sandbox.hadoop2 import starting_scripts as s_scripts
from sahara.plugins.sandbox.hadoop2 import utils as u
from sahara.plugins.sandbox.hadoop2 import validation as vl
from sahara.plugins.sandbox import utils as vu
from sahara.plugins.sandbox.v2_7_1 import config_helper as c_helper
from sahara.plugins.sandbox.v2_7_1 import edp_engine
from sahara.swift import swift_helper
from sahara.utils import cluster as cluster_utils


conductor = conductor.API
CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class VersionHandler(avm.AbstractVersionHandler):
    def __init__(self):
        self.pctx = {  # 解析后的所有的配置选项，其中，all_configs包含上面的内容
            'env_confs': c_helper.get_env_configs(),
            'all_confs': c_helper.get_plugin_configs()  # 包含ENV_CONF
        }

    def get_plugin_configs(self):
        return self.pctx['all_confs']  # get all plugin configs form xml or object which built by default

    def get_node_processes(self):
        return {
            "Hadoop": [],
            "MapReduce": ["historyserver"],
            "HDFS": ["namenode", "datanode", "secondarynamenode"],
            "YARN": ["resourcemanager", "nodemanager"],
            "JobFlow": ["oozie"],
            "Hive": ["hiveserver"],
            "Spark": ["spark history server", "master", "slave"]  # added Spark process displayed on the dashboard
        }

    def validate(self, cluster):
        vl.validate_cluster_creating(self.pctx, cluster)

    def update_infra(self, cluster):
        pass

    def configure_cluster(self, cluster):
        c.configure_cluster(self.pctx, cluster)

    def start_cluster(self, cluster):
        keypairs.provision_keypairs(cluster)
        des = cluster.description
        backup = None
        if len(des) > 5 and des[-6:] == "backup":
            backup = "backup"

        # 配置环境变量
        s_scripts.config_user_env(cluster)

        s_scripts.start_namenode(cluster, backup)
        s_scripts.start_secondarynamenode(cluster)
        s_scripts.start_resourcemanager(cluster)

        run.start_dn_nm_processes(utils.get_instances(cluster))
        run.await_datanodes(cluster)

        s_scripts.start_historyserver(cluster)
        s_scripts.start_oozie(self.pctx, cluster, backup)
        s_scripts.start_hiveserver(self.pctx, cluster, backup)

        # swift_helper.install_ssl_certs(cluster_utils.get_instances(cluster))

        # start spark nodes, newly added 'swift_helper.install_ssl_certs' after 'start_spark'
        s_scripts.start_spark(cluster)
        swift_helper.install_ssl_certs(cluster_utils.get_instances(cluster))

        # 新增的方法，配置用户的环境变量
        # s_scripts.config_user_env(cluster)
        # newly added
        LOG.info('Cluster has been started successfully --by HXH')
        self._set_cluster_info(cluster)

    def decommission_nodes(self, cluster, instances):
        sc.decommission_nodes(self.pctx, cluster, instances)

    def validate_scaling(self, cluster, existing, additional):
        vl.validate_additional_ng_scaling(cluster, additional)
        vl.validate_existing_ng_scaling(self.pctx, cluster, existing)

    def scale_cluster(self, cluster, instances):
        keypairs.provision_keypairs(cluster, instances)
        sc.scale_cluster(self.pctx, cluster, instances)

    def _set_cluster_info(self, cluster):
        nn = vu.get_namenode(cluster)
        rm = vu.get_resourcemanager(cluster)
        hs = vu.get_historyserver(cluster)
        oo = vu.get_oozie(cluster)
        sp = vu.get_spark_history_server(cluster)
        sp_master = vu.get_spark_master(cluster)

        info = {}

        if rm:
            info['YARN'] = {
                'Web UI': 'http://%s:%s' % (rm.management_ip, '8088'),
                'ResourceManager': 'http://%s:%s' % (
                    rm.management_ip, '8032')
            }

        if nn:
            info['HDFS'] = {
                'Web UI': 'http://%s:%s' % (nn.management_ip, '50070'),
                'NameNode': 'hdfs://%s:%s' % (nn.hostname(), '9000')
            }

        if oo:
            info['JobFlow'] = {
                'Oozie': 'http://%s:%s' % (oo.management_ip, '11000')
            }

        if hs:
            info['MapReduce JobHistory Server'] = {
                'Web UI': 'http://%s:%s' % (hs.management_ip, '19888')
            }

        if sp:
            info['Apache Spark'] = {
                'Spark UI': 'http://%s:%s' % (sp.management_ip, '4040'),
                'Spark History Server UI':
                    'http://%s:%s' % (sp.management_ip, '18080')
            }

        if sp_master:
            info['Spark'] = {
                'Web UI': 'http://%s:%s' % (
                    sp_master.management_ip, '8080')
            }

        ctx = context.ctx()
        conductor.cluster_update(ctx, cluster, {'info': info})

    def get_edp_engine(self, cluster, job_type):
        if job_type in edp_engine.EdpOozieEngine.get_supported_job_types():
            return edp_engine.EdpOozieEngine(cluster)
        if job_type in edp_engine.EdpSparkEngine.get_supported_job_types():
            return edp_engine.EdpSparkEngine(cluster)

        return None

    def get_edp_job_types(self):
        return (edp_engine.EdpOozieEngine.get_supported_job_types() +
                edp_engine.EdpSparkEngine.get_supported_job_types())

    def get_edp_config_hints(self, job_type):
        return edp_engine.EdpOozieEngine.get_possible_job_config(job_type)

    def on_terminate_cluster(self, cluster):
        u.delete_oozie_password(cluster)
        keypairs.drop_key(cluster)

    def get_open_ports(self, node_group):
        return c.get_open_ports(node_group)

    def recommend_configs(self, cluster, scaling):
        ru.recommend_configs(cluster, self.get_plugin_configs(), scaling)
