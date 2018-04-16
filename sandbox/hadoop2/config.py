# coding=utf-8

# Copyright (c) 2014 Mirantis Inc.
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
import os

from oslo_config import cfg
from oslo_log import log as logging
import six

from sahara import context
from sahara.i18n import _
from sahara.i18n import _LW
from sahara.plugins import utils
from sahara.plugins.sandbox.hadoop2 import config_helper as c_helper
from sahara.plugins.sandbox.hadoop2 import oozie_helper as o_helper
from sahara.plugins.sandbox.hadoop2 import utils as u
from sahara.plugins.sandbox import utils as vu
from sahara.service.castellan import utils as key_manager
from sahara.swift import swift_helper as swift
from sahara.topology import topology_helper as th
from sahara.utils import cluster_progress_ops as cpo
from sahara.utils import configs as s_cfg
from sahara.utils import files as f
from sahara.utils import proxy
from sahara.utils import xmlutils as x

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

HADOOP_CONF_DIR = '/opt/hadoop/etc/hadoop'
OOZIE_CONF_DIR = '/opt/oozie/conf'
HIVE_CONF_DIR = '/opt/hive/conf'
HADOOP_USER = 'hadoop'
HADOOP_GROUP = 'hadoop'

PORTS_MAP = {
    "namenode": [50070, 9000],
    "secondarynamenode": [50090],
    "resourcemanager": [8088, 8032],
    "historyserver": [19888],
    "datanode": [50010, 50075, 50020],
    "nodemanager": [8042],
    "oozie": [11000],
    "hiveserver": [9999, 10000]
}


def configure_cluster(pctx, cluster):
    LOG.debug("Configuring cluster")
    if (CONF.use_identity_api_v3 and CONF.use_domain_for_proxy_users and
            vu.get_hiveserver(cluster) and
            c_helper.is_swift_enabled(pctx, cluster)):
        cluster = proxy.create_proxy_user_for_cluster(cluster)

    instances = utils.get_instances(cluster)
    configure_instances(pctx, instances)
    configure_topology_data(pctx, cluster)
    configure_spark(cluster)


def configure_spark(cluster):
    extra = _extract_spark_configs_to_extra(cluster)
    _push_spark_configs_to_node(cluster, extra)


def _push_spark_configs_to_node(cluster, extra):
    """
    spark_master = vu.get_spark_history_server(cluster)
    if spark_master:
        _push_spark_configs_to_existing_node(spark_master, cluster, extra)
        _push_cleanup_job(spark_master, extra)
        with spark_master.remote() as r:
            r.execute_command('sudo su - -c "mkdir /tmp/spark-events" hadoop')
    """
    # newly added line
    spark_master = vu.get_spark_master(cluster)
    spark_slaves = vu.get_spark_slaves(cluster)
    spark_history_server = vu.get_spark_history_server(cluster)

    if (spark_master and spark_slaves) or spark_history_server:
        all_instances = utils.get_instances(cluster)
        # cpo.add_provisioning_step(
        #    all_instances[0].cluster_id, _("Push spark configs to nodes"), len(all_instances))

        for instance in all_instances:
            _push_spark_configs_to_new_node(instance, cluster, extra)
            _push_cleanup_job(instance, extra)
            with instance.remote() as r:
                r.execute_command('sudo su - -c "mkdir /tmp/spark-events" hadoop')


# new method for config the spark
def _push_spark_configs_to_new_node(instance, cluster, extra):

    sp_home = c_helper.get_spark_home(cluster)
    files = {
        os.path.join(sp_home,
                     'conf/spark-env.sh'): extra['sp_master'],
        os.path.join(sp_home, 'conf/slaves'): extra['sp_slaves'],
        os.path.join(
            sp_home,
            'conf/spark-defaults.conf'): extra['sp_defaults']
        }

    with instance.remote() as r:
        r.write_files_to(files, run_as_root=True)


def _push_spark_configs_to_existing_node(spark_master, cluster, extra):

    sp_home = c_helper.get_spark_home(cluster)
    files = {
        os.path.join(sp_home,
                     'conf/spark-env.sh'): extra['sp_master'],
        os.path.join(
            sp_home,
            'conf/spark-defaults.conf'): extra['sp_defaults']
        }

    with spark_master.remote() as r:
        r.write_files_to(files, run_as_root=True)


def _push_cleanup_job(instance, extra):
    node_processes = instance.node_group.node_processes
    if 'master' in node_processes:
        with instance.remote() as r:
            if extra['job_cleanup']['valid']:
                r.write_file_to('/opt/hadoop/tmp-cleanup.sh',
                                extra['job_cleanup']['script'],
                                run_as_root=True)
                r.execute_command("sudo chmod 755 /opt/hadoop/tmp-cleanup.sh")
                cmd = 'sudo sh -c \'echo "%s" > /etc/cron.d/spark-cleanup\''
                r.execute_command(cmd % extra['job_cleanup']['cron'])
            else:
                r.execute_command("sudo rm -f /opt/hadoop/tmp-cleanup.sh")
                r.execute_command("sudo rm -f /etc/cron.d/spark-cleanup")


def _extract_spark_configs_to_extra(cluster):

    # sp_master = utils.get_instance(cluster, "spark history server")

    # 新加增的配置项：master 和 slave， 更改spark history server变量名
    sp_master = utils.get_instance(cluster, "master")
    sp_slaves = utils.get_instances(cluster, "slave")
    # 这个选项可有可没有
    sp_history = utils.get_instance(cluster, "spark history server")
    extra = dict()

    config_master = config_slaves = ''
    if sp_master is not None:
        config_master = c_helper.generate_spark_env_configs(cluster)

    if sp_slaves is not None:
        slavenames = []
        for slave in sp_slaves:
            slavenames.append(slave.hostname())
        config_slaves = c_helper.generate_spark_slaves_configs(slavenames)
    else:
        config_slaves = "\n"

    # Any node that might be used to run spark-submit will need
    # these libs for swift integration
    config_defaults = c_helper.generate_spark_executor_classpath(cluster)

    extra['job_cleanup'] = c_helper.generate_job_cleanup_config(cluster)
    extra['sp_master'] = config_master
    extra['sp_slaves'] = config_slaves
    extra['sp_defaults'] = config_defaults

    # if c_helper.is_data_locality_enabled(cluster):
    #     topology_data = th.generate_topology_map(cluster, CONF.enable_hypervisor_awareness)
    #     extra['topology_data'] = "\n".join([k + " " + v for k, v in topology_data.items()]) + "\n"

    return extra


def configure_instances(pctx, instances):
    if len(instances) == 0:
        return

    cpo.add_provisioning_step(  # 添加配置步骤信息
        instances[0].cluster_id, _("Configure instances"), len(instances))

    for instance in instances:
        with context.set_current_instance_id(instance.instance_id):
            _configure_instance(pctx, instance)


@cpo.event_wrapper(True)
def _configure_instance(pctx, instance):
    _provisioning_configs(pctx, instance)
    _post_configuration(pctx, instance)


def _provisioning_configs(pctx, instance):
    xmls, env = _generate_configs(pctx, instance)
    _push_xml_configs(instance, xmls)
    _push_env_configs(instance, env)


def _generate_configs(pctx, instance):
    hadoop_xml_confs = _get_hadoop_configs(pctx, instance)
    user_xml_confs, user_env_confs = _get_user_configs(
        pctx, instance.node_group)
    xml_confs = s_cfg.merge_configs(user_xml_confs, hadoop_xml_confs)
    env_confs = s_cfg.merge_configs(pctx['env_confs'], user_env_confs)

    return xml_confs, env_confs


def _get_hadoop_configs(pctx, instance):  # 配置hadoop
    cluster = instance.node_group.cluster
    nn_hostname = vu.get_instance_hostname(vu.get_namenode(cluster))
    dirs = _get_hadoop_dirs(instance)
    confs = {
        'Hadoop': {
            'fs.defaultFS': 'hdfs://%s:9000' % nn_hostname
        },
        'HDFS': {
            'dfs.namenode.name.dir': ','.join(dirs['hadoop_name_dirs']),
            'dfs.datanode.data.dir': ','.join(dirs['hadoop_data_dirs']),
            'dfs.hosts': '%s/dn-include' % HADOOP_CONF_DIR,
            'dfs.hosts.exclude': '%s/dn-exclude' % HADOOP_CONF_DIR
        }
    }

    res_hostname = vu.get_instance_hostname(vu.get_resourcemanager(cluster))
    if res_hostname:
        confs['YARN'] = {
            'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
            'yarn.resourcemanager.hostname': '%s' % res_hostname,
            'yarn.resourcemanager.nodes.include-path': '%s/nm-include' % (
                HADOOP_CONF_DIR),
            'yarn.resourcemanager.nodes.exclude-path': '%s/nm-exclude' % (
                HADOOP_CONF_DIR)
        }
        confs['MapReduce'] = {
            'mapreduce.framework.name': 'yarn'
        }
        hs_hostname = vu.get_instance_hostname(vu.get_historyserver(cluster))
        if hs_hostname:
            confs['MapReduce']['mapreduce.jobhistory.address'] = (
                "%s:10020" % hs_hostname)

    oozie = vu.get_oozie(cluster)
    if oozie:
        hadoop_cfg = {
            'hadoop.proxyuser.hadoop.hosts': '*',
            'hadoop.proxyuser.hadoop.groups': 'hadoop'
        }
        confs['Hadoop'].update(hadoop_cfg)  # 在字典后面增加 hadoop_cfg 字典的内容

        oozie_cfg = o_helper.get_oozie_required_xml_configs(HADOOP_CONF_DIR)
        if c_helper.is_mysql_enabled(pctx, cluster):
            oozie_cfg.update(o_helper.get_oozie_mysql_configs(cluster))

        confs['JobFlow'] = oozie_cfg

    if c_helper.is_swift_enabled(pctx, cluster):
        swift_configs = {}
        for config in swift.get_swift_configs():
            swift_configs[config['name']] = config['value']

        confs['Hadoop'].update(swift_configs)

    if c_helper.is_data_locality_enabled(pctx, cluster):
        confs['Hadoop'].update(th.TOPOLOGY_CONFIG)
        confs['Hadoop'].update({"topology.script.file.name":
                                HADOOP_CONF_DIR + "/topology.sh"})

    hive_hostname = vu.get_instance_hostname(vu.get_hiveserver(cluster))
    if hive_hostname:
        hive_pass = u.get_hive_password(cluster)

        hive_cfg = {
            'hive.warehouse.subdir.inherit.perms': True,
            'javax.jdo.option.ConnectionURL':
            'jdbc:derby:;databaseName=/opt/hive/metastore_db;create=true'
        }

        if c_helper.is_mysql_enabled(pctx, cluster):
            hive_cfg.update({
                'javax.jdo.option.ConnectionURL':
                'jdbc:mysql://%s/metastore' % hive_hostname,
                'javax.jdo.option.ConnectionDriverName':
                'com.mysql.jdbc.Driver',
                'javax.jdo.option.ConnectionUserName': 'hive',
                'javax.jdo.option.ConnectionPassword': hive_pass,
                'datanucleus.autoCreateSchema': 'false',
                'datanucleus.fixedDatastore': 'true',
                'hive.metastore.uris': 'thrift://%s:9083' % hive_hostname,
            })

        proxy_configs = cluster.cluster_configs.get('proxy_configs')
        if proxy_configs and c_helper.is_swift_enabled(pctx, cluster):
            hive_cfg.update({
                swift.HADOOP_SWIFT_USERNAME: proxy_configs['proxy_username'],
                swift.HADOOP_SWIFT_PASSWORD: key_manager.get_secret(
                    proxy_configs['proxy_password']),
                swift.HADOOP_SWIFT_TRUST_ID: proxy_configs['proxy_trust_id'],
                swift.HADOOP_SWIFT_DOMAIN_NAME: CONF.proxy_user_domain_name
            })

        confs['Hive'] = hive_cfg

    return confs


def _get_user_configs(pctx, node_group):
    ng_xml_confs, ng_env_confs = _separate_configs(node_group.node_configs,
                                                   pctx['env_confs'])
    cl_xml_confs, cl_env_confs = _separate_configs(
        node_group.cluster.cluster_configs, pctx['env_confs'])

    xml_confs = s_cfg.merge_configs(cl_xml_confs, ng_xml_confs)
    env_confs = s_cfg.merge_configs(cl_env_confs, ng_env_confs)
    return xml_confs, env_confs


def _separate_configs(configs, all_env_configs):  # 前边是用户规定的配置项，后面是提取的默认配置 xml 文件
    xml_configs = {}
    env_configs = {}
    for service, params in six.iteritems(configs):
        for param, value in six.iteritems(params):
            if all_env_configs.get(service, {}).get(param):
                if not env_configs.get(service):
                    env_configs[service] = {}
                env_configs[service][param] = value
            else:
                if not xml_configs.get(service):
                    xml_configs[service] = {}
                xml_configs[service][param] = value

    return xml_configs, env_configs


def _generate_xml(configs):
    xml_confs = {}
    for service, confs in six.iteritems(configs):
        xml_confs[service] = x.create_hadoop_xml(confs)

    return xml_confs


def _push_env_configs(instance, configs):
    nn_heap = configs['HDFS']['NameNode Heap Size']
    snn_heap = configs['HDFS']['SecondaryNameNode Heap Size']
    dn_heap = configs['HDFS']['DataNode Heap Size']
    rm_heap = configs['YARN']['ResourceManager Heap Size']
    nm_heap = configs['YARN']['NodeManager Heap Size']
    hs_heap = configs['MapReduce']['JobHistoryServer Heap Size']

    with instance.remote() as r:
        r.replace_remote_string(
            '%s/hadoop-env.sh' % HADOOP_CONF_DIR,
            'export HADOOP_NAMENODE_OPTS=.*',
            'export HADOOP_NAMENODE_OPTS="-Xmx%dm"' % nn_heap)
        r.replace_remote_string(
            '%s/hadoop-env.sh' % HADOOP_CONF_DIR,
            'export HADOOP_SECONDARYNAMENODE_OPTS=.*',
            'export HADOOP_SECONDARYNAMENODE_OPTS="-Xmx%dm"' % snn_heap)
        r.replace_remote_string(
            '%s/hadoop-env.sh' % HADOOP_CONF_DIR,
            'export HADOOP_DATANODE_OPTS=.*',
            'export HADOOP_DATANODE_OPTS="-Xmx%dm"' % dn_heap)
        r.replace_remote_string(
            '%s/yarn-env.sh' % HADOOP_CONF_DIR,
            '\\#export YARN_RESOURCEMANAGER_HEAPSIZE=.*',
            'export YARN_RESOURCEMANAGER_HEAPSIZE=%d' % rm_heap)
        r.replace_remote_string(
            '%s/yarn-env.sh' % HADOOP_CONF_DIR,
            '\\#export YARN_NODEMANAGER_HEAPSIZE=.*',
            'export YARN_NODEMANAGER_HEAPSIZE=%d' % nm_heap)
        r.replace_remote_string(
            '%s/mapred-env.sh' % HADOOP_CONF_DIR,
            'export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=.*',
            'export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=%d' % hs_heap)


def _push_xml_configs(instance, configs):
    xmls = _generate_xml(configs)
    service_to_conf_map = {
        'Hadoop': '%s/core-site.xml' % HADOOP_CONF_DIR,
        'HDFS': '%s/hdfs-site.xml' % HADOOP_CONF_DIR,
        'YARN': '%s/yarn-site.xml' % HADOOP_CONF_DIR,
        'MapReduce': '%s/mapred-site.xml' % HADOOP_CONF_DIR,
        'JobFlow': '%s/oozie-site.xml' % OOZIE_CONF_DIR,
        'Hive': '%s/hive-site.xml' % HIVE_CONF_DIR
    }
    xml_confs = {}
    for service, confs in six.iteritems(xmls):
        if service not in service_to_conf_map.keys():
            continue

        xml_confs[service_to_conf_map[service]] = confs

    _push_configs_to_instance(instance, xml_confs)


def _push_configs_to_instance(instance, configs):
    LOG.debug("Push configs to instance {instance}".format(
        instance=instance.instance_name))
    with instance.remote() as r:
        for fl, data in six.iteritems(configs):
            r.write_file_to(fl, data, run_as_root=True)


def _post_configuration(pctx, instance):
    dirs = _get_hadoop_dirs(instance)
    args = {
        'hadoop_user': HADOOP_USER,
        'hadoop_group': HADOOP_GROUP,
        'hadoop_conf_dir': HADOOP_CONF_DIR,
        'oozie_conf_dir': OOZIE_CONF_DIR,
        'hadoop_name_dirs': " ".join(dirs['hadoop_name_dirs']),
        'hadoop_data_dirs': " ".join(dirs['hadoop_data_dirs']),
        'hadoop_log_dir': dirs['hadoop_log_dir'],
        'hadoop_secure_dn_log_dir': dirs['hadoop_secure_dn_log_dir'],
        'yarn_log_dir': dirs['yarn_log_dir']
    }
    post_conf_script = f.get_file_text(
        'plugins/sandbox/hadoop2/resources/post_conf.template')
    post_conf_script = post_conf_script.format(**args)

    with instance.remote() as r:
        r.write_file_to('/tmp/post_conf.sh', post_conf_script)
        r.execute_command('chmod +x /tmp/post_conf.sh')
        r.execute_command('sudo /tmp/post_conf.sh')

        if c_helper.is_data_locality_enabled(pctx,
                                             instance.cluster):
            t_script = HADOOP_CONF_DIR + '/topology.sh'
            r.write_file_to(t_script, f.get_file_text(
                            'plugins/sandbox/hadoop2/resources/topology.sh'),
                            run_as_root=True)
            r.execute_command('chmod +x ' + t_script, run_as_root=True)


def _get_hadoop_dirs(instance):
    dirs = {}
    storage_paths = instance.storage_paths()
    dirs['hadoop_name_dirs'] = _make_hadoop_paths(
        storage_paths, '/hdfs/namenode')
    dirs['hadoop_data_dirs'] = _make_hadoop_paths(
        storage_paths, '/hdfs/datanode')
    dirs['hadoop_log_dir'] = _make_hadoop_paths(
        storage_paths, '/hadoop/logs')[0]
    dirs['hadoop_secure_dn_log_dir'] = _make_hadoop_paths(
        storage_paths, '/hadoop/logs/secure')[0]
    dirs['yarn_log_dir'] = _make_hadoop_paths(
        storage_paths, '/yarn/logs')[0]

    return dirs


def _make_hadoop_paths(paths, hadoop_dir):
    return [path + hadoop_dir for path in paths]


@cpo.event_wrapper(
    True, step=_("Configure topology data"), param=('cluster', 1))
def configure_topology_data(pctx, cluster):
    if c_helper.is_data_locality_enabled(pctx, cluster):
        LOG.warning(_LW("Node group awareness is not implemented in YARN yet "
                        "so enable_hypervisor_awareness set to False "
                        "explicitly"))
        tpl_map = th.generate_topology_map(cluster, is_node_awareness=False)
        topology_data = "\n".join(
            [k + " " + v for k, v in tpl_map.items()]) + "\n"
        for ng in cluster.node_groups:
            for i in ng.instances:
                i.remote().write_file_to(HADOOP_CONF_DIR + "/topology.data",
                                         topology_data, run_as_root=True)


def get_open_ports(node_group):
    ports = []
    for key in PORTS_MAP:
        if key in node_group.node_processes:
            ports += PORTS_MAP[key]
    return ports
