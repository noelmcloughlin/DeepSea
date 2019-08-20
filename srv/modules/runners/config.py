from ext_lib.utils import runner
from ext_lib.hash_dir import pillar_questioneer
from pydoc import pager
import errno
import os
import logging
import yaml
import pprint
import uuid

import signal
import sys
import salt.client
import salt.runner

log = logging.getLogger(__name__)

class DeepSeaRoles(object):
    '''
    Drives creation of /srv/pillar/ceph/minions and /srv/pillar/ceph/global.yml
    '''

    def __init__(self, roles):
        self.dir = "/srv/pillar/ceph/minions"
        self.roles = roles
        self.minions = {}
        self.dumper = yaml.SafeDumper
        self.dumper.ignore_aliases = lambda self, data: True

    def invert(self):
        ''' Converts a dict of lists to Salt files '''
        self.minions = self._invert(self.roles)
        log.info(f"Minions:\n{pprint.pformat(self.minions)}")

    def _invert(self, olddict):
        '''  '''
        inv = {}
        for key, value in olddict.items():
            for item in value:
                inv.setdefault(item, []).append(key)
        return inv

    def write(self):
        '''  '''
        if self._mkdir(self.dir):
            for minion in self.minions:
                filename = f"{self.dir}/{minion}.sls"

                contents = {'roles': self._lstrip(self.minions[minion])}
                log.info(f"Writing {filename}")
                log.info(f"Contents {contents}")
                with open(filename, "w") as yml:
                    yml.write(yaml.dump(contents, Dumper=self.dumper,
                              default_flow_style=False))
            return True
        return False

    def _mkdir(self, path):
        '''  '''
        if os.path.isdir(path):
            return True
        try:
            os.makedirs(path)
            return True
        except OSError as error:
            if error.errno == errno.EACCES:
                self.error = (
                    f"Cannot create directory {path} - verify that {self.root} "
                    f"is owned by salt"
                )
        return False

    def _lstrip(self, roles):
        '''  '''
        return [role.lstrip('role-') for role in roles]



class Policy(object):
    '''
    Loads and expands the policy.cfg
    '''

    def __init__(self):
        '''  '''
        self.filename = "/srv/pillar/ceph/proposals/policy1.cfg"
        self.error = ""
        self.raw = {}
        self.yaml = {}

    def load(self):
        ''' Read policy.cfg '''
        if not os.path.isfile(self.filename):
            self.error = f"filename {self.filename} is missing"
            return False
        with open(self.filename, 'r') as policy:
            try:
                self.raw = yaml.load(policy)
                log.info(f"Contents of {self.filename}: \n{pprint.pformat(self.raw)}")
                return True
            except yaml.YAMLError as error:
                self.error = (
                    f"syntax error in {error.problem_mark.name} "
                    f"on line {error.problem_mark.line} in position "
                    f"{error.problem_mark.column}"
                )
            return False

    def expand(self):
        ''' Expand each Salt target '''
        for role in self.raw:
            self.yaml[role] = self._expand(self.raw[role])
        log.info(f"Expanded contents:\n{pprint.pformat(self.yaml)}")

    def _expand(self, target):
        ''' Resolve Salt target '''
        local = salt.client.LocalClient()
        # When search matches no minions, salt prints to stdout.  Suppress stdout.
        _stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

        results = local.cmd(target, 'grains.get', ['id'], tgt_type="compound")
        sys.stdout = _stdout
        return sorted(results)


class DeepSeaGlobal(object):
    '''
    Writes the initial /srv/pillar/ceph/global.yml
    '''

    def __init__(self):
        '''  '''
        self.filename = "/srv/pillar/ceph/global.yml"
        self.contents = {}
        self.contents['fsid'] = str(uuid.uuid4())
        self.contents['time_server'] = f"{master_minion()}"
        self.contents['mgmt_network'] = "w.w.w.w"
        self.contents['public_network'] = "x.x.x.x"
        self.contents['cluster_network'] = "y.y.y.y"
        self.dumper = yaml.SafeDumper
        self.dumper.ignore_aliases = lambda self, data: True

    def write(self):
        '''  '''
        if os.path.exists(self.filename):
            log.info(f"File {self.filename} already exists - not overwriting")
            return False

        with open(self.filename, "w") as yml:
            yml.write(yaml.dump(self.contents, Dumper=self.dumper,
                      default_flow_style=False))
        return True


def master_minion():
    """
    Load the master modules
    """
    __master_opts__ = salt.config.client_config("/etc/salt/master")
    __master_utils__ = salt.loader.utils(__master_opts__)
    __salt_master__ = salt.loader.minion_mods(
        __master_opts__, utils=__master_utils__)

    return __salt_master__["master.minion"]()


def deploy():
    ''' Apply configuration to Salt '''

    policy = Policy()
    if not policy.load():
        print(policy.error)
        return ""
    policy.expand()

    dsr = DeepSeaRoles(policy.yaml)
    dsr.invert()
    dsr.write()

    dsg = DeepSeaGlobal()
    dsg.write()
    return ""



