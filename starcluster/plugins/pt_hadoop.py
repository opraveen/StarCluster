# Copyright 2009-2014 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

import posixpath

from starcluster import threadpool
from starcluster import clustersetup
from starcluster.logger import log

core_site_templ = """\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<!-- In: conf/core-site.xml -->

<property>
  <name>fs.default.name</name>
  <value>hdfs://%(master)s:54310</value>
</property>

</configuration>
"""

yarn_site_templ = """\
<?xml version="1.0"?>

<!-- Site specific YARN configuration properties -->

<configuration>

  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property> 
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>%(master)s</value>
  </property>

</configuration>
"""

hdfs_site_templ = """\
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->

<configuration>
<!-- In: conf/hdfs-site.xml -->
<property>
  <name>dfs.permissions</name>
  <value>false</value>
</property>
<property>
  <name>dfs.replication</name>
  <value>%(replication)d</value>
  <description>Default block replication.
  The actual number of replications can be specified when the file is created.
  The default is used if replication is not specified in create time.
  </description>
</property>
<property>
  <name>dfs.%(node_type)s.name.dir</name>
  <value>/home/ubuntu/hadoop/hadoop-dist/target/hadoop-2.7.2/hadoop_data/hdfs/%(node_type)s</value>
</property>
</configuration>
"""

mapred_site_templ = """\
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
  <property>
    <name>mapreduce.jobtracker.address</name>
    <value>%(master)s:54311</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
"""

class PT_Hadoop(clustersetup.ClusterSetup):
    """
    Configures Hadoop using Cloudera packages on StarCluster
    """

    def __init__(self, hadoop_tmpdir='/mnt/hadoop', map_to_proc_ratio='1.0',
                 reduce_to_proc_ratio='0.3'):
        self.hadoop_tmpdir = hadoop_tmpdir
        self.hadoop_home = '/home/ubuntu/hadoop/hadoop-dist/target/hadoop-2.7.2'
        self.hadoop_conf = '/home/ubuntu/config/hadoop.conf'
        self.centos_java_home = '/usr/lib/jvm/java'
        self.centos_alt_cmd = 'alternatives'
        self.ubuntu_javas = ['/usr/lib/jvm/java-8-oracle/jre'
                            ]
        self.ubuntu_alt_cmd = 'update-alternatives'
        self.map_to_proc_ratio = float(map_to_proc_ratio)
        self.reduce_to_proc_ratio = float(reduce_to_proc_ratio)
        self._pool = None

    @property
    def pool(self):
        if self._pool is None:
            self._pool = threadpool.get_thread_pool(20, disable_threads=False)
        return self._pool

    def _get_java_home(self, node):
        # check for CentOS, otherwise default to Ubuntu 10.04's JAVA_HOME
        if node.ssh.isfile('/etc/redhat-release'):
            return self.centos_java_home
        for java in self.ubuntu_javas:
            if node.ssh.isdir(java):
                return java
        raise Exception("Cant find JAVA jre")

    def _get_alternatives_cmd(self, node):
        # check for CentOS, otherwise default to Ubuntu 10.04
        if node.ssh.isfile('/etc/redhat-release'):
            return self.centos_alt_cmd
        return self.ubuntu_alt_cmd

    def _setup_hadoop_user(self, node, user):
        log.info("Skipping setup-hadoop-user...")
        #node.ssh.execute('gpasswd -a %s hadoop' % user)

    def _install_empty_conf(self, node):
        log.info("Skipping install-empty-conf...")

    def _configure_env(self, node):
	# node.ssh.execute('sudo -s')
	# node.ssh.execute('source /home/ubuntu/.profile')
        env_file_sh = posixpath.join(self.hadoop_conf, 'hadoop-env.sh')
        node.ssh.remove_lines_from_file(env_file_sh, 'JAVA_HOME')
        env_file = node.ssh.remote_file(env_file_sh, 'a')
        env_file.write('export JAVA_HOME=%s\n' % self._get_java_home(node))
        env_file.close()

    def _configure_mapreduce_site(self, node, cfg):
        mapred_site_xml = posixpath.join(self.hadoop_conf, 'mapred-site.xml')
        mapred_site = node.ssh.remote_file(mapred_site_xml)
        # Hadoop default: 2 maps, 1 reduce
        # AWS EMR uses approx 1 map per proc and .3 reduce per proc
        map_tasks_max = max(
            2,
            int(self.map_to_proc_ratio * node.num_processors))
        reduce_tasks_max = max(
            1,
            int(self.reduce_to_proc_ratio * node.num_processors))
        cfg.update({
            'map_tasks_max': map_tasks_max,
            'reduce_tasks_max': reduce_tasks_max})
        mapred_site.write(mapred_site_templ % cfg)
        mapred_site.close()

    def _configure_core(self, node, cfg):
        core_site_xml = posixpath.join(self.hadoop_conf, 'core-site.xml')
        core_site = node.ssh.remote_file(core_site_xml)
        core_site.write(core_site_templ % cfg)
        core_site.close()

    def _configure_yarn(self, node, cfg):
        yarn_site_xml = posixpath.join(self.hadoop_conf, 'yarn-site.xml')
        yarn_site = node.ssh.remote_file(yarn_site_xml)
        yarn_site.write(yarn_site_templ % cfg)
        yarn_site.close()

    def _configure_hdfs_site(self, node, cfg):
        hdfs_site_xml = posixpath.join(self.hadoop_conf, 'hdfs-site.xml')
        hdfs_site = node.ssh.remote_file(hdfs_site_xml)
        hdfs_site.write(hdfs_site_templ % cfg)
        hdfs_site.close()
	# Create the directory 'sudo mkdir -p $HADOOP_HOME/hadoop_data/hdfs/namenode'
	# data_dir = 
	node.ssh.execute('sudo mkdir -p /home/ubuntu/hadoop/hadoop-dist/target/hadoop-2.7.2/hadoop_data/hdfs/' + cfg['node_type'])

    def _configure_masters(self, node, master):
        masters_file = posixpath.join(self.hadoop_conf, 'masters')
        masters_file = node.ssh.remote_file(masters_file)
        masters_file.write(master.alias)
        masters_file.close()

    def _configure_slaves(self, node, node_aliases):
        slaves_file = posixpath.join(self.hadoop_conf, 'slaves')
        slaves_file = node.ssh.remote_file(slaves_file)
        slaves_file.write('\n'.join(node_aliases))
        slaves_file.close()

    def _setup_hdfs(self, node, user):
        self._setup_hadoop_dir(node, self.hadoop_tmpdir, 'hdfs', 'hadoop')
        mapred_dir = posixpath.join(self.hadoop_tmpdir, 'hadoop-mapred')
        self._setup_hadoop_dir(node, mapred_dir, 'mapred', 'hadoop')
        userdir = posixpath.join(self.hadoop_tmpdir, 'hadoop-%s' % user)
        self._setup_hadoop_dir(node, userdir, user, 'hadoop')
        hdfsdir = posixpath.join(self.hadoop_tmpdir, 'hadoop-hdfs')
        if not node.ssh.isdir(hdfsdir):
            node.ssh.execute("su hdfs -c 'hadoop namenode -format'")
        self._setup_hadoop_dir(node, hdfsdir, 'hdfs', 'hadoop')

    def _setup_dumbo(self, node):
        if not node.ssh.isfile('/etc/dumbo.conf'):
            f = node.ssh.remote_file('/etc/dumbo.conf')
            f.write('[hadoops]\nstarcluster: %s\n' % self.hadoop_home)
            f.close()

    def _configure_hadoop(self, master, nodes, user):
        log.info("Configuring Hadoop...")
        log.info("Adding user %s to hadoop group" % user)
        for node in nodes:
            self.pool.simple_job(self._setup_hadoop_user, (node, user),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        node_aliases = map(lambda n: n.alias, nodes)
        cfg = {'master': master.alias, 'replication': 3,
               'hadoop_tmpdir': posixpath.join(self.hadoop_tmpdir,
                                               'hadoop-${user.name}'), 'node_type': 'namenode'}
        log.info("Installing configuration templates...")
        # for node in nodes:
        #     self.pool.simple_job(self._install_empty_conf, (node,),
        #                          jobid=node.alias)
        # self.pool.wait(numtasks=len(nodes))
        log.info("Configuring environment...")
        for node in nodes:
            self.pool.simple_job(self._configure_env, (node,),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring Core Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_core, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring YARN Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_yarn, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring MapReduce Site...")
        for node in nodes:
            self.pool.simple_job(self._configure_mapreduce_site, (node, cfg),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        

	log.info("Configuring HDFS Site...")
        for node in nodes:
	    if not node.is_master():
		cfg.update({'node_type': 'datanode'})
		
            self.pool.simple_job(self._configure_hdfs_site, (node, cfg),
                                 jobid=node.alias)

        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring masters file...")
        for node in nodes:
            self.pool.simple_job(self._configure_masters, (node, master),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        log.info("Configuring slaves file...")
        for node in nodes:
            self.pool.simple_job(self._configure_slaves, (node, node_aliases),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))
        
	# log.info("Configuring HDFS...")
        # for node in nodes:
        #     self.pool.simple_job(self._setup_hdfs, (node, user),
        #                          jobid=node.alias)
        # self.pool.wait(numtasks=len(nodes))
        log.info("Configuring dumbo...")
        for node in nodes:
            self.pool.simple_job(self._setup_dumbo, (node,), jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

    def _setup_hadoop_dir(self, node, path, user, group, permission="775"):
        if not node.ssh.isdir(path):
            node.ssh.mkdir(path)
        node.ssh.execute("chown -R %s:hadoop %s" % (user, path))
        node.ssh.execute("chmod -R %s %s" % (permission, path))

    def _start_datanode(self, node):
        node.ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode')

    def _start_tasktracker(self, node):
        node.ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start tasktracker')

    def _start_hadoop(self, master, nodes):
        log.info("Formatting namenode...")
        master.ssh.execute('source /home/ubuntu/.pt_hadoop.sh && hdfs namenode -format')
        log.info("Starting HDFS daemons...")
        master.ssh.execute('source /home/ubuntu/.pt_hadoop.sh && $HADOOP_HOME/sbin/start-dfs.sh')
	# TODO: start-dfs.sh does not seem to start a SecobdaryNameNode, why?
        log.info("Starting YARN...")
	master.ssh.execute('source /home/ubuntu/.pt_hadoop.sh && $HADOOP_HOME/sbin/start-yarn.sh')
        
	log.info("Starting MapReduce JobHistory...")
	master.ssh.execute('source /home/ubuntu/.pt_hadoop.sh && $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver')
        
	## NO LONGER needed!!!?????
	# log.info("Starting secondary namenode...")
        
	# master.ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start secondarynamenode')
        # log.info("Starting datanode on all nodes...")
        # for node in nodes:
        #     self.pool.simple_job(self._start_datanode, (node,),
        #                          jobid=node.alias)
        # self.pool.wait()
        # log.info("Starting jobtracker...")
        # master.ssh.execute('$HADOOP_HOME/sbin/hadoop-daemon.sh start jobtracker')
        # log.info("Starting tasktracker on all nodes...")
        # for node in nodes:
        #     self.pool.simple_job(self._start_tasktracker, (node,),
        #                          jobid=node.alias)
        # self.pool.wait()

    def _open_ports(self, master):
        ports = [50070, 54311]
        ec2 = master.ec2
        for group in master.cluster_groups:
            for port in ports:
                has_perm = ec2.has_permission(group, 'tcp', port, port,
                                              '0.0.0.0/0')
                if not has_perm:
                    ec2.conn.authorize_security_group(group_id=group.id,
                                                      ip_protocol='tcp',
                                                      from_port=port,
                                                      to_port=port,
                                                      cidr_ip='0.0.0.0/0')

    def run(self, nodes, master, user, user_shell, volumes):
        #self._configure_hadoop(master, nodes, user)
        #self._start_hadoop(master, nodes)
        #self._open_ports(master)
        log.info("Job tracker status: http://%s:54311" % master.dns_name)
        log.info("Namenode status: http://%s:50070" % master.dns_name)
