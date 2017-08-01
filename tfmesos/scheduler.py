import os
import sys
import math
import select
import socket
import logging
import textwrap

from addict import Dict
from gen.apache.aurora.api.ttypes import AssignedTask
from gen.apache.aurora.api.constants import AURORA_EXECUTOR_NAME, \
    TASK_FILESYSTEM_MOUNT_POINT
from six import iteritems
from six.moves import urllib
from pymesos import Scheduler, MesosSchedulerDriver, encode_data
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport

from tfmesos.aurora_utils import get_job_config, create_process
from tfmesos.utils import send, recv, setup_logger
import uuid


FOREVER = 0xFFFFFFFF
logger = logging.getLogger(__name__)


class Job(object):

    def __init__(self, name, num, cpus=1.0, mem=1024.0,
                 gpus=0, cmd=None, start=0):
        self.name = name
        self.num = num
        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.cmd = cmd
        self.start = start


class Task(object):

    def __init__(self, mesos_task_id, job_name, task_index,
                 cpus=1.0, mem=1024.0, gpus=0, cmd=None,
                 volumes=None):
        self.mesos_task_id = mesos_task_id
        self.job_name = job_name
        self.task_index = task_index

        self.cpus = cpus
        self.gpus = gpus
        self.mem = mem
        self.cmd = cmd
        self.volumes = volumes
        self.offered = False

        self.addr = None
        self.connection = None
        self.initalized = False
        self.aurora_job_config = None

    def __str__(self):
        return textwrap.dedent('''
        <Task
          mesos_task_id=%s
          addr=%s
        >''' % (self.mesos_task_id, self.addr))

    def make_container(self, container_config):
        container = Dict()
        if container_config.mesos is not None:
            container.type = 'MESOS'
            mesos_container_config = container_config.mesos

            mesos_info = Dict()
            volume = Dict()

            if mesos_container_config.image is not None:
                docker_image = Dict()
                docker_image.type = 'DOCKER'
                docker_image.name = '{}:{}'.format(
                    mesos_container_config.image.docker.name,
                    mesos_container_config.image.docker.tag)

                mesos_info.image = docker_image

                volume.container_path = TASK_FILESYSTEM_MOUNT_POINT
                volume.mode = 'RO'
                volume.image = docker_image
                container.volumes = [volume]

            container.mesos = mesos_info

        else:
            container.type = 'DOCKER'
            container.docker = self._make_docker_info(container_config.docker)
            container.volumes = None

        return container


    def _make_docker_info(self, docker_container_config):
        """
            Convert MesosContainer to its mesos equivalent.
            :param mesos_container_config:
            :return:
        """
        docker_container = Dict()

        if docker_container_config.image is not None:
            docker_container.image = docker_container_config.image
        else:
            raise Exception()

        return docker_container


    def _make_assigned_task(self, task_config, offer):
        return AssignedTask(self.mesos_task_id,
                            offer.agent_id.value, offer.hostname,
                            task=task_config,
                            instanceId=0, assignedPorts={})

    def _serialize_thrift(self, thrift_struct):
        transport_out = TTransport.TMemoryBuffer()
        protocol_out = TBinaryProtocol.TBinaryProtocol(transport_out)
        thrift_struct.write(protocol_out)
        return transport_out.getvalue()

    def to_thermos_task(self, framework_id, offer):
        task_config = self.aurora_job_config.taskConfig
        ti = Dict()
        ti.task_id.value = str(self.mesos_task_id)
        ti.agent_id.value = offer.agent_id.value
        ti.name = '/job:%s/task:%s' % (self.job_name, self.task_index)
        resources = []

        for res in task_config.resources:
            resource = Dict()
            if res.numCpus is not None:
                resource.name = 'cpus'
                resource.type = 'SCALAR'
                resource.scalar.value = res.numCpus
            elif res.ramMb is not None:
                resource.name = 'mem'
                resource.type = 'SCALAR'
                resource.scalar.value = res.ramMb
            elif res.diskMb is not None:
                resource.name = 'disk'
                resource.type = 'SCALAR'
                resource.scalar.value = res.diskMb

            resources.append(resource)

        ti.resources = resources

        ti.executor = self._make_executor_info(framework_id, task_config.container)
        ti.data = encode_data(self._serialize_thrift(self._make_assigned_task(task_config, offer)))
        return ti

    def _make_executor_info(self, framework_id, container_config):
        executor_info = Dict()
        executor_info.type = 'CUSTOM'
        executor_info.executor_id = Dict()
        executor_info.executor_id.value = 'tfmesos-{}-{}'.format(self.job_name, self.mesos_task_id)
        executor_info.framework_id = framework_id
        executor_info.command = self._make_executor_command_info()
        executor_info.name = AURORA_EXECUTOR_NAME
        executor_info.container = self.make_container(container_config)
        return executor_info

    def _make_executor_command_info(self):
        uris = ['/usr/share/aurora/bin/thermos_executor.pex']
        command_info = Dict()
        command_info.value = "${MESOS_SANDBOX=.}/thermos_executor.pex"
        command_info.uris = map(lambda x: self._make_command_info(x), uris)

        return command_info

    def _make_command_info(self, uri):
        command_info = Dict()
        command_info.value = uri
        command_info.executable = True
        return command_info

    def to_task_info(self, offer, master_addr, gpu_uuids=[],
                     gpu_resource_type=None, containerizer_type='MESOS',
                     force_pull_image=False):
        ti = Dict()
        ti.task_id.value = str(self.mesos_task_id)
        ti.agent_id.value = offer.agent_id.value
        ti.name = '/job:%s/task:%s' % (self.job_name, self.task_index)
        ti.resources = resources = []

        cpus = Dict()
        resources.append(cpus)
        cpus.name = 'cpus'
        cpus.type = 'SCALAR'
        cpus.scalar.value = self.cpus

        mem = Dict()
        resources.append(mem)
        mem.name = 'mem'
        mem.type = 'SCALAR'
        mem.scalar.value = self.mem

        image = os.environ.get('DOCKER_IMAGE')

        if image is not None:
            if containerizer_type == 'DOCKER':
                ti.container.type = 'DOCKER'
                ti.container.docker.image = image
                ti.container.docker.force_pull_image = force_pull_image

                ti.container.docker.parameters = parameters = []
                p = Dict()
                p.key = 'memory-swap'
                p.value = '-1'
                parameters.append(p)

                if self.gpus and gpu_uuids:
                    hostname = offer.hostname
                    url = 'http://%s:3476/docker/cli?dev=%s' % (
                        hostname, urllib.parse.quote(
                            ' '.join(gpu_uuids)
                        )
                    )

                    try:
                        docker_args = urllib.request.urlopen(url).read()
                        for arg in docker_args.split():
                            k, v = arg.split('=')
                            assert k.startswith('--')
                            k = k[2:]
                            p = Dict()
                            parameters.append(p)
                            p.key = k
                            p.value = v
                    except Exception:
                        logger.exception(
                            'fail to determine remote device parameter,'
                            ' disable gpu resources'
                        )
                        gpu_uuids = []

            elif containerizer_type == 'MESOS':
                ti.container.type = 'MESOS'
                ti.container.mesos.image.type = 'DOCKER'
                ti.container.mesos.image.docker.name = image
                # "cached" means the opposite of "force_pull_image"
                ti.container.mesos.image.cached = not force_pull_image

            else:
                assert False, (
                    'Unsupported containerizer: %s' % containerizer_type
                )

            ti.container.volumes = volumes = []

            for path in ['/etc/passwd', '/etc/group']:
                v = Dict()
                volumes.append(v)
                v.host_path = v.container_path = path
                v.mode = 'RO'

            for src, dst in iteritems(self.volumes):
                v = Dict()
                volumes.append(v)
                v.container_path = dst
                v.host_path = src
                v.mode = 'RW'

        if self.gpus and gpu_uuids and gpu_resource_type is not None:
            if gpu_resource_type == 'SET':
                gpus = Dict()
                resources.append(gpus)
                gpus.name = 'gpus'
                gpus.type = 'SET'
                gpus.set.item = gpu_uuids
            else:
                gpus = Dict()
                resources.append(gpus)
                gpus.name = 'gpus'
                gpus.type = 'SCALAR'
                gpus.scalar.value = len(gpu_uuids)

        ti.command.shell = True

        cmd = [
            sys.executable, '-m', '%s.server' % __package__,
            str(self.mesos_task_id), master_addr
        ]
        ti.command.value = ' '.join(cmd)
        ti.command.environment.variables = variables = []
        env = Dict()
        variables.append(env)
        env.name = 'PYTHONPATH'
        env.value = ':'.join(sys.path)
        return ti


class TFMesosScheduler(Scheduler):
    MAX_FAILURE_COUNT = 3

    def __init__(self, task_spec, role=None, master=None, name=None,
                 quiet=False, volumes={}, containerizer_type=None,
                 force_pull_image=False, forward_addresses=None,
                 protocol='grpc', thermos_config=None):
        self.started = False
        self.master = master or os.environ['MESOS_MASTER']
        self.name = name or '[tensorflow] %s %s' % (
            os.path.abspath(sys.argv[0]), ' '.join(sys.argv[1:]))
        self.task_spec = task_spec
        self.containerizer_type = containerizer_type
        self.force_pull_image = force_pull_image
        self.protocol = protocol
        self.forward_addresses = forward_addresses
        self.role = role or '*'
        self.tasks = {}
        self.task_failure_count = {}
        self.job_finished = {}
        self.framework_id = None
        self.thermos_config = thermos_config

        for job in task_spec:
            self.job_finished[job.name] = 0
            for task_index in range(job.start, job.num):
                mesos_task_id = str(uuid.uuid4())
                task = \
                    Task(
                        mesos_task_id,
                        job.name,
                        task_index,
                        cpus=job.cpus,
                        mem=job.mem,
                        gpus=job.gpus,
                        cmd=job.cmd,
                        volumes=volumes
                    )
                self.tasks[mesos_task_id] = task
                self.task_failure_count[self.decorated_task_index(task)] = 0
        if not quiet:
            global logger
            setup_logger(logger)

    def resourceOffers(self, driver, offers):
        '''
        Offer resources and launch tasks
        '''
        for offer in offers:
            if all(task.offered for id, task in iteritems(self.tasks)):
                self.driver.suppressOffers()
                driver.declineOffer(offer.id, Dict(refuse_seconds=FOREVER))
                continue

            offered_cpus = offered_mem = 0.0
            offered_gpus = []
            offered_tasks = []
            gpu_resource_type = None

            for resource in offer.resources:
                if resource.name == 'cpus':
                    offered_cpus = resource.scalar.value
                elif resource.name == 'mem':
                    offered_mem = resource.scalar.value
                elif resource.name == 'gpus':
                    if resource.type == 'SET':
                        offered_gpus = resource.set.item
                    else:
                        offered_gpus = list(range(int(resource.scalar.value)))

                    gpu_resource_type = resource.type

            for id, task in iteritems(self.tasks):
                if task.offered:
                    continue

                if not (task.cpus <= offered_cpus and
                        task.mem <= offered_mem and
                        task.gpus <= len(offered_gpus)):

                    continue

                offered_cpus -= task.cpus
                offered_mem -= task.mem
                gpus = int(math.ceil(task.gpus))
                gpu_uuids = offered_gpus[:gpus]
                offered_gpus = offered_gpus[gpus:]
                task.offered = True
                offered_tasks.append(
                    task.to_thermos_task(self.framework_id, offer)
                )

            print offered_tasks
            driver.launchTasks(offer.id, offered_tasks)

    @property
    def targets(self):
        targets = {}
        for id, task in iteritems(self.tasks):
            target_name = '/job:%s/task:%s' % (task.job_name, task.task_index)
            grpc_addr = 'grpc://%s' % task.addr
            targets[target_name] = grpc_addr
        return targets

    def _start_tf_cluster(self):
        cluster_def = {}

        for id, task in iteritems(self.tasks):
            cluster_def.setdefault(task.job_name, []).append(task.addr)

        for id, task in iteritems(self.tasks):
            response = {
                'job_name': task.job_name,
                'task_index': task.task_index,
                'cpus': task.cpus,
                'mem': task.mem,
                'gpus': task.gpus,
                'cmd': task.cmd,
                'cwd': os.getcwd(),
                'cluster_def': cluster_def,
                'forward_addresses': self.forward_addresses,
                'protocol': self.protocol
            }
            send(task.connection, response)
            assert recv(task.connection) == 'ok'
            logger.info(
                'Device /job:%s/task:%s activated @ grpc://%s ',
                task.job_name,
                task.task_index,
                task.addr

            )
            task.connection.close()

    def start(self):

        def readable(fd):
            return bool(select.select([fd], [], [], 0.1)[0])

        lfd = socket.socket()
        try:
            lfd.bind(('', 0))
            self.addr = '%s:%s' % (socket.gethostname(), lfd.getsockname()[1])
            lfd.listen(10)
            framework = Dict()
            framework.user = "root"
            framework.name = self.name
            framework.hostname = socket.gethostname()
            framework.role = self.role

            for task_id, task in iteritems(self.tasks):
                task.aurora_job_config = self.get_aurora_job_config(task, self.addr)

            self.driver = MesosSchedulerDriver(
                self, framework, self.master, use_addict=True
            )
            self.driver.start()
            task_start_count = 0
            while any((not task.initalized
                       for id, task in iteritems(self.tasks))):
                if readable(lfd):
                    c, _ = lfd.accept()
                    if readable(c):
                        mesos_task_id, addr = recv(c)
                        task = self.tasks[mesos_task_id]
                        task.addr = addr
                        task.connection = c
                        task.initalized = True
                        task_start_count += 1
                        logger.info('Task %s with mesos_task_id %s has '
                                    'registered',
                                    '{}:{}'.format(task.job_name,
                                                   task.task_index),
                                    mesos_task_id)
                        logger.info('Out of %d tasks '
                                    '%d tasks have been registered',
                                    len(self.tasks), task_start_count)
                    else:
                        c.close()

            self.started = True
            self._start_tf_cluster()
        except Exception:
            self.stop()
            raise
        finally:
            lfd.close()

    def get_aurora_job_config(self, task, master_addr):
        cmd = [
            'python -m',
            'tfmesos.server',
            '{{thermos.task_id}}',
            master_addr
        ]

        self.thermos_config['command'] = ' '.join(cmd)
        self.thermos_config['container'] = {
            "type": self.containerizer_type,
            "image":os.environ.get('DOCKER_IMAGE')
        }

        self.thermos_config['cpus'] = task.cpus
        self.thermos_config['disk'] = 100
        self.thermos_config['mem'] = task.mem
        self.thermos_config['gpus'] = task.gpus

        finalizer = None

        if 'finalizer' in self.thermos_config:
            finalizer = create_process(
                cmdline=self.thermos_config['finalizer']['cmd'],
                process_name=self.thermos_config['finalizer']['name'],
                final=True)

        aurora_job_config = get_job_config(self.thermos_config,
                                           self.thermos_config['cluster'],
                                           self.thermos_config['ownerName'],
                                           self.thermos_config['environment'],
                                           'tfmesos_server',
                                           child_processes=[finalizer])

        return aurora_job_config

    def registered(self, driver, framework_id, master_info):
        logger.info(
            'Tensorflow cluster registered. '
            '( http://%s:%s/#/frameworks/%s )',
            master_info.hostname, master_info.port, framework_id.value
        )

        self.framework_id = framework_id

        if self.containerizer_type is None:
            version = tuple(int(x) for x in driver.version.split("."))
            self.containerizer_type = (
                'MESOS' if version >= (1, 0, 0) else 'DOCKER'
            )

    def statusUpdate(self, driver, update):
        logger.info('Received status update %s', str(update.state))
        mesos_task_id = update.task_id.value
        if self._is_terminal_state(update.state):
            task = self.tasks.get(mesos_task_id)
            if task is None:
                # This should be very rare and hence making this info.
                logger.info("Task not found for mesos task id {}"
                            .format(mesos_task_id))
                return
            if self.started:
                if update.state != 'TASK_FINISHED':
                    logger.error('Task failed: %s, %s with state %s', task,
                                 update.message, update.state)
                    raise RuntimeError(
                        'Task %s failed! %s with state %s' %
                        (task, update.message, update.state)
                    )
                else:
                    self.job_finished[task.job_name] += 1
            else:
                logger.warn('Task failed while launching the server: %s, '
                            '%s with state %s', task,
                            update.message, update.state)

                if task.connection:
                    task.connection.close()

                self.task_failure_count[self.decorated_task_index(task)] += 1

                if self._can_revive_task(task):
                    self.revive_task(driver, mesos_task_id, task)
                else:
                    raise RuntimeError('Task %s failed %s with state %s and '
                                       'retries=%s' %
                                       (task, update.message, update.state,
                                        TFMesosScheduler.MAX_FAILURE_COUNT))

    def revive_task(self, driver, mesos_task_id, task):
        logger.info('Going to revive task %s ', task.task_index)
        self.tasks.pop(mesos_task_id)
        task.offered = False
        task.addr = None
        task.connection = None
        new_task_id = task.mesos_task_id = str(uuid.uuid4())
        self.tasks[new_task_id] = task
        driver.reviveOffers()

    def _can_revive_task(self, task):
        return self.task_failure_count[self.decorated_task_index(task)] < \
               TFMesosScheduler.MAX_FAILURE_COUNT

    @staticmethod
    def decorated_task_index(task):
        return '{}.{}'.format(task.job_name, str(task.task_index))

    @staticmethod
    def _is_terminal_state(task_state):
        return task_state in ["TASK_FINISHED", "TASK_FAILED", "TASK_KILLED",
                              "TASK_ERROR"]

    def slaveLost(self, driver, agent_id):
        if self.started:
            logger.error('Slave %s lost:', agent_id.value)
            raise RuntimeError('Slave %s lost' % agent_id)

    def executorLost(self, driver, executor_id, agent_id, status):
        if self.started:
            logger.error('Executor %s lost:', executor_id.value)
            raise RuntimeError('Executor %s@%s lost' % (executor_id, agent_id))

    def error(self, driver, message):
        logger.error('Mesos error: %s', message)
        raise RuntimeError('Error ' + message)

    def stop(self):
        logger.debug('exit')

        if hasattr(self, 'tasks'):
            for id, task in iteritems(self.tasks):
                if task.connection:
                    task.connection.close()

            del self.tasks

        if hasattr(self, 'driver'):
            self.driver.stop()
            self.driver.join()
            del self.driver

    def finished(self):
        return any(
            self.job_finished[job.name] >= job.num for job in self.task_spec
        )
