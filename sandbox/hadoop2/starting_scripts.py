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

from sahara.plugins import utils
from sahara.plugins.sandbox.hadoop2 import run_scripts as run
from sahara.plugins.sandbox import utils as vu
from sahara.utils import cluster_progress_ops as cpo
from sahara.utils import files


def start_namenode(cluster, backup=None):
    nn = vu.get_namenode(cluster)
    _start_namenode(nn, backup)


@cpo.event_wrapper(
    True, step=utils.start_process_event_message('NameNode'))
def _start_namenode(nn, backup=None):
    if backup is None:
        run.format_namenode(nn)
    run.start_hadoop_process(nn, 'namenode')


def start_secondarynamenode(cluster):
    snn = vu.get_secondarynamenode(cluster)
    if snn:
        _start_secondarynamenode(snn)


@cpo.event_wrapper(
    True, step=utils.start_process_event_message("SecondaryNameNodes"))
def _start_secondarynamenode(snn):
    run.start_hadoop_process(snn, 'secondarynamenode')


def start_resourcemanager(cluster):
    rm = vu.get_resourcemanager(cluster)
    if rm:
        _start_resourcemanager(rm)


@cpo.event_wrapper(
    True, step=utils.start_process_event_message('ResourceManager'))
def _start_resourcemanager(snn):
    run.start_yarn_process(snn, 'resourcemanager')


def start_historyserver(cluster):
    hs = vu.get_historyserver(cluster)
    if hs:
        run.start_historyserver(hs)


def start_oozie(pctx, cluster, backup=None):
    oo = vu.get_oozie(cluster)
    if oo:
        run.start_oozie_process(pctx, oo, backup)


def start_hiveserver(pctx, cluster, backup=None):
    hiveserver = vu.get_hiveserver(cluster)
    if hiveserver:
        run.start_hiveserver_process(pctx, hiveserver, backup)


def start_spark(cluster):
    """
    spark = vu.get_spark_history_server(cluster)
    if spark:
        run.start_spark_history_server(spark)
    """
    sm_instance = utils.get_instance(cluster, "master")
    sp_history_server = vu.get_spark_history_server(cluster)

    if sm_instance:
        run.start_spark(sm_instance)

    if sp_history_server:
        run.start_spark_history_server(sp_history_server)


# 新增的方法，用来配置用户的环境变量
# @cpo.event_wrapper(
#     True, step=utils.start_process_event_message('Config user ENV'))
def config_user_env(cluster):
    instances = utils.get_instances(cluster)
    user_env = files.get_file_text('plugins/sandbox/hadoop2/resources/user_env.template')
    for instance in instances:
        run.config_env(instance, user_env)
        # with instance.remote() as r:
        #     r.append_to_file('/etc/profile', user_env, run_as_root=True)
        #     r.execute_command('source /etc/profile', run_as_root=True)
