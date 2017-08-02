from apache.aurora.client.config import AuroraConfig
from apache.aurora.config.schema.base import (
    MesosJob,
    Process,
    Task,
    Resources,
    Parameter,
    Docker, Mesos, DockerImage
)
from apache.aurora.common.clusters import (Clusters, Cluster)
from apache.thermos.config.schema_helpers import Tasks, Processes
from gen.apache.aurora.api.ttypes import MesosFetcherURI

cluster_dict = [
        {
            "name": "overwatch-dev",
            "scheduler_uri": "http://aurora.overwatch-dev.pinadmin.com:8081",
            "slave_root": "/mnt/mesos",
            "slave_run_directory": "latest"
        },
        {
            "name": "karthik-aurora",
            "zk": "karthik-zookeeper-002.ec2.pin220.com:2181",
            "zk_port": "2181",
            "scheduler_zk_path": "/aurora/scheduler",
            "slave_root": "/mnt/mesos",
            "slave_run_directory": "latest"
        },
        {
            "name": "testmesos",
            "zk": "testmesos-002.ec2.pin220.com:2181",
            "zk_port": "2181",
            "scheduler_zk_path": "/aurora/scheduler",
            "slave_root": "/mnt/mesos",
            "slave_run_directory": "latest"
        },
        {
            "name": "overwatch",
            "zk": "overwatch-002.ec2.pin220.com:2181",
            "zk_port": "2181",
            "scheduler_zk_path": "/aurora/scheduler",
            "slave_root": "/mnt/mesos",
            "slave_run_directory": "latest"
        }
    ]

CLUSTERS = Clusters(cluster_list=map(lambda x: Cluster(**x), cluster_dict))

def get_cleanup_finalizer(process_name, cmdline):
    """
    Add a finalizer process that runs after the normal
        processes have run.
    :param process_name: Name of the process
    :param cmdline: The cmd to run
    :return: A process object
    """
    return create_process(process_name=process_name,
                          cmdline=cmdline, final=True)


def add_environment_variables(environment_variables):
    """
        Add a process that writes environment variables to .thermos_profile
    :param environment_variables: list of environment vars
    :return: A process object
    """
    template = '''cat <<EOF > .thermos_profile
{}
EOF'''
    env_vars = []
    for d in environment_variables:
        env_vars.append('export {}="{}"'.format(d['name'], d['value']))
    cmd = template.format('\n'.join(env_vars))

    return create_process(cmdline=cmd, process_name='setup_env')


def _serialize(assigned_task):
    """
        Serialize assifned task to bytes
    :param assigned_task: An AssignedTask
    :return: bytes
    """
    pass

def read_environment_variables():
    """
        A process to print environment variables.
    :return: A process object.
    """
    return create_process('printenv', 'read_env')


def create_process(cmdline, process_name, daemon=False, ephemeral=False,
                   max_failures=1, min_duration=15, final=False):
    """

    :param cmdline: Command to execute
    :param process_name: Name of the process
    :param daemon: Whether process is a daemon
    :param ephemeral: Whether process is ephemeral
    :param max_failures: Max failures to tolerate before marking process as failed.
    :param min_duration: Min duration
    :param final: Whether process is a finalizer
    :return: A thermos Process object.
    """
    return Process(
        cmdline=cmdline,
        name=process_name,
        max_failures=max_failures,
        daemon=daemon,
        ephemeral=ephemeral,
        min_duration=min_duration,
        final=final
    )


def move_to_sandbox(file_name):
    """
        Move specified to the aurora sandbox directory.
    :param file_name: File name to move
    :return: Process object
    """
    return create_process(
        'mv ../{file_name} .'.format(file_name=file_name),
        'move_{file_name}'.format(file_name=file_name)
    )


def move_fetched_files_to_sandbox(fetch_configs):
    """
        Move files fetched uses the mesos fetcher to the aurora sandbox
    :param fetch_configs: Fetcher configs
    :return: List of process objects that move files into the sandbox
    """
    move_processes = []
    for fetch in fetch_configs:
        for f in fetch['files']:
            mv_process = move_to_sandbox(f)
            move_processes.append(mv_process)

    return move_processes


def create_task(processes,
                resources, constraints):
    """
    Create task object from processes and resources
    :param processes: List of processes
    :param resources: Resources for all processes
    :return: Task object
    """
    return Task(
        processes=processes,
        resources=resources,
        constraints=constraints
    )


def create_sequential_task(processes, resources):
    """
    Create sequential task object from processes and resources
    :param processes: Ordered list of processes
    :param resources: Resources for processes
    :return:
    """
    tsk = create_task(processes, resources, constraints=None)

    return Tasks.sequential(tsk)


def get_resources(json_config):
    """
    Create Resource object from json configuration
    :param json_config:
    :return: Resource object
    """
    return Resources(
        cpu=json_config['cpus'],
        ram=json_config['mem'] * 1024 * 1024,
        disk=json_config['disk'] * 1024 * 1024,
        gpu=json_config['gpus']
    )


def create_container_config(container_dict):
    """
    Create container configration
    :param container_dict:
    :return: Container object
    """
    if container_dict is None:
        return None
    else:
        image = None

        if container_dict.get('image') is not None:
            name, tag = container_dict['image'].split(':')
            image = DockerImage(name=name, tag=tag)

        if container_dict['type'] == 'DOCKER':
            return Docker(
                image=container_dict['image'],
                parameters=[] if 'parameters' not in container_dict else _create_docker_parameters(container_dict)
            )
        else:
            return Mesos(image=image)


def _create_docker_parameters(container_dict):
    """
        Create the DockerParameter object form container configs
    :param container_dict: Container config
    :return: Docker parameter object
    """
    docker_parameters = []
    for param in container_dict['parameters']:
        docker_parameters.append(Parameter(name=param['key'],
                                           value=param['value']))

    return docker_parameters


def fetch_uri(fetch_configs):
    """
        List of fetch configs
    :param fetch:
    :return:
    """
    fetcher_uris = []
    for f in fetch_configs:
        fetcher = MesosFetcherURI(f['uri'], f['extract'], f['cache'])
        fetcher_uris.append(fetcher)
    return frozenset(fetcher_uris)


def get_job_config(json_config, cluster, aurora_role,
                   aurora_environment, job_name,
                   optional_configs=None,
                   parent_process=None, child_processes=None):
    """
        Return a MesosJob from a chronos config. Fills in default values
        and creates only one process to run the command. Also creates
        a process to deal with environment variables.
    :param job_name: Name of the job
    :param json_config: chronos json config
    :param cluster: aurora cluster to use
    :param aurora_role: Aurora role to run as
    :param aurora_environment: aurora environment (dev, test or prod)
    :param optional_configs: optional aurora configs
    :param child_processes: optional list of processes to run after the cmd
    :param parent_process: optional list of procesess to run before the cmd
    :return: MesosJob
    """

    env_processes = [] if json_config.get('environmentVariables') is None \
        else [add_environment_variables(json_config['environmentVariables'])]

    parent_process = [] if parent_process is None else parent_process
    child_processes = [] if child_processes is None else child_processes

    move_processes = [] if json_config.get('fetch') is None \
        else move_fetched_files_to_sandbox(json_config['fetch'])

    constrained_processes = env_processes + parent_process + move_processes + [
        create_process(json_config['command'], job_name),
    ]

    constraints = Processes.order(*constrained_processes)

    task = create_task(constrained_processes + child_processes,
                       get_resources(json_config), constraints=constraints)

    optional_configs = {} if optional_configs is None else optional_configs

    container_mesos = create_container_config(json_config.get('container'))

    if container_mesos is not None:
        optional_configs['container'] = container_mesos

    aurora_config = _get_aurora_config(cluster, job_name, json_config['ownerName'],
                                       aurora_role, aurora_environment,
                                       task, optional_configs)

    thrift_job_config = aurora_config.job()

    thrift_job_config.taskConfig.mesosFetcherUris = [] if json_config.get('fetch') is None else fetch_uri(json_config['fetch'])
    return thrift_job_config


def _get_aurora_config(cluster, name, contact, aurora_role,
                       aurora_environment,
                       task, optional_configs=None):
    mesos_job = MesosJob(
        name=name,
        role=aurora_role,
        contact=contact,
        cluster=cluster,
        environment=aurora_environment,
        instances=1,
        task=task,
        tier='preferred',
        **optional_configs
    )

    return AuroraConfig(mesos_job)
