import os
import tempfile
import unittest

import pytest

from rucio.client import client
from rucio.common.config import config_get, config_get_bool
from rucio.common.types import InternalAccount, InternalScope
from rucio.core.replica import add_replicas, delete_replicas
from rucio.common.utils import execute, adler32
from rucio.core.rule import add_rule
from rucio.core.transfer import get_transfer_requests_and_source_replicas
from rucio.rse import rsemanager


def get_xrd_rse_info():
    """
    Detects if containerized rses for xrootd are available in the testing environment.
    :return: A tuple (rse, prefix, hostname, port).
    """
    cmd = "rucio list-rses --expression 'test_container_xrd=True'"
    print(cmd)
    exitcode, out, err = execute(cmd)
    print(out, err)
    rses = out.split()

    if len(rses) == 0:
        return []
    else:
        output = [(lambda x: {'rse_id': x.upper(), 'hostname': x, 'prefix': '/rucio/'})(x) for x in rses]
        return output


skip_tpc_tests_without_containerized_rse = pytest.mark.skipif(len(get_xrd_rse_info()) == 0,
                                                              reason='fails if containerzed rse are absent')


def create_local_file():
    pass


def remove_local_copy():
    pass


@skip_tpc_tests_without_containerized_rse
class TestTPC(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Creating local files
        local_files = ['1_rse_remote_tpc.raw', '2_rse_remote_tpc.raw']
        cls.tmpdir = tempfile.mkdtemp()
        for filename in local_files:
            with open(f"{cls.tmpdir}/{filename}", "wb") as out:
                out.seek((1024 * 1024) - 1)  # 1 MB
                out.write(b'\0')
        print("********")
        print(os.listdir(f"{cls.tmpdir}"))

    def setUp(self):
        if config_get_bool('common', 'multi_vo', raise_exception=False, default=False):
            self.vo = {'vo': config_get('client', 'vo', raise_exception=False, default='tst')}
        else:
            self.vo = {}


        self.root = InternalAccount('root', **self.vo)

        self.xrd_rses = get_xrd_rse_info()

        self.rc = client.ReplicaClient()

        # xrd1 storage element
        self.rse_xrd1 = self.xrd_rses[0]
        self.rse_xrd1_id = self.rse_xrd1['rse_id']
        self.rse_xrd1_prefix = self.rse_xrd1['prefix']
        self.rse_xrd1_hostname = self.rse_xrd1['hostname']

        #TODO abstract port info
        protocol = rsemanager.create_protocol(rsemanager.get_rse_info(self.rse_xrd1_id), 'write')
        protocol.connect()
        self.rse_xrd1_file = '1_rse_remote_tpc.raw' #'xroot://%s:%d/%s/data.raw' % (self.rse_xrd1_hostname, '1094', self.rse_xrd1_prefix)
        self.rse_xrd1_pfn = protocol.path2pfn(self.rse_xrd1_prefix + protocol._get_path('user.%s' % self.root, self.rse_xrd1_file))
        cmd = 'xrdcp %s/data.raw %s' % (TestTPC.tmpdir, self.rse_xrd1_pfn)
        execute(cmd)

        # xrd2 storage element
        self.rse_xrd2 = self.xrd_rses[1]
        self.rse_xrd2_id = self.rse_xrd2['rse_id']
        self.rse_xrd2_prefix = self.rse_xrd2['prefix']
        self.rse_xrd2_hostname = self.rse_xrd2['hostname']
        # TODO abstract port info
        # protocol = rsemanager.create_protocol(self.rse_xrd2_id, 'write')
        #         # protocol.connect()
        #         # self.rse_xrd2_file = '2_rse_remote_tpc.raw'  # 'xroot://%s:%d/%s/data.raw' % (self.rse_xrd1_hostname, '1094', self.rse_xrd1_prefix)
        #         # self.xrd2_pfn = protocol.path2pfn(self.rse_xrd1_prefix + protocol._get_path('user.%s' % self.root, self.rse_xrd2_file))
        #         # cmd = 'xrdcp %s/data.raw %s' % (TestTPC.tmpdir, self.rse_xrd2_pfn)
        #         # execute(cmd)

        self.rse_xrd2_file = {'name': '2_rse_remote_tpc.raw', 'scope': InternalScope('test', **self.vo),
         'adler32': adler32('%s/2_rse_remote_tpc.raw' % self.tmpdir),
         'bytes': os.stat('%s/2_rse_remote_tpc.raw' % self.tmpdir)[os.path.stat.ST_SIZE]},
        # add a non-S3 storage with a replica
        # self.rsenons3 = rse_name_generator()
        # self.rsenons3_id = add_rse(self.rsenons3, **self.vo)
        # add_protocol(self.rsenons3_id, {'scheme': 'https',
        #                                 'hostname': 'somestorage.ch',
        #                                 'port': 1094,
        #                                 'prefix': '/my/prefix',
        #                                 'impl': 'rucio.rse.protocols.gfal.Default',
        #                                 'domains': {
        #                                     'lan': {'read': 1, 'write': 1, 'delete': 1},
        #                                     'wan': {'read': 1, 'write': 1, 'delete': 1, 'third_party_copy': 1}}})
        # # add_rse_attribute(rse_id=self.rsenons3_id, key='fts', value='localhost')
        # self.filenons3 = [{'scope': InternalScope('mock', **self.vo), 'name': 'file-on-storage',
        #                    'bytes': 1234, 'adler32': 'deadbeef', 'meta': {'events': 321}}]
        print('********&&&&&&&&')
        print(InternalScope(self.root.internal, **self.vo))
        print(rsemanager.get_rse_info(self.rse_xrd2_id))
        add_replicas(rse_id=rsemanager.get_rse_info(self.rse_xrd2_id)['id'], files=self.rse_xrd2_file, account=self.root)


    def tearDown(self):
        # delete_replicas(rse_id=rsemanager.get_rse_info(self.rse_xrd1_id)['id'], files=self.rse_xrd1_file)
        delete_replicas(rse_id=rsemanager.get_rse_info(self.rse_xrd2_id)['id'], files=self.rse_xrd2_file)
        # del_rse(self.rses3_id)
        # del_rse(self.rsenons3_id)

    def test_tpc_xrd1_xrd2(self):
        pass
        # expected_src_url = 'https://xrd1:1094/rucio/2_rse_remote_tpc.raw'
        # expected_dst_url = 'https://xrd2:1095/rucio/2_rse_remote_tpc.raw'
        #
        # rule_id = add_rule(dids=self.files3, account=self.root, copies=1, rse_expression=self.rsenons3,
        #                    grouping='NONE', weight=None, lifetime=None, locked=False, subscription_id=None)
        #
        # requestss = get_transfer_requests_and_source_replicas(rses=[self.rses3])
        # for requests in requestss:
        #     for request in requests:
        #         if requests[request]['rule_id'] == rule_id[0]:
        #             print("*********")
        #             print(requests[request]['sources'][0][1])
        #             print(requests[request]['sources'][0][1])
        #             assert requests[request]['dest_urls'][0] == expected_src_url
        #             # assert requests[request]['dest_urls'][0] == expected_dst_url


    def check_for_files(self):
        pass

    # def test_s3s_fts_src(self):
    #     """ S3: TPC a file from S3 to storage """
    #
    #     expected_src_url = 's3s://fake-rucio.s3-eu-south-8.amazonaws.com:443/mock/69/3b/file-on-aws'
    #     expected_dst_url = 'https://somestorage.ch:1094/my/prefix/mock/69/3b/file-on-aws'
    #
    #     rule_id = add_rule(dids=self.files3, account=self.root, copies=1, rse_expression=self.rsenons3,
    #                        grouping='NONE', weight=None, lifetime=None, locked=False, subscription_id=None)
    #
    #     requestss = get_transfer_requests_and_source_replicas(rses=[self.rses3])
    #     for requests in requestss:
    #         for request in requests:
    #             if requests[request]['rule_id'] == rule_id[0]:
    #                 assert requests[request]['sources'][0][1] == expected_src_url
    #                 assert requests[request]['dest_urls'][0] == expected_dst_url
    #
    # def test_s3s_fts_dst(self):
    #     """ S3: TPC a file from storage to S3 """
    #
    #     expected_src_url = 'https://somestorage.ch:1094/my/prefix/mock/ab/01/file-on-storage?copy_mode=push'
    #     expected_dst_url = 's3s://fake-rucio.s3-eu-south-8.amazonaws.com:443/mock/ab/01/file-on-storage'
    #
    #     rule_id = add_rule(dids=self.filenons3, account=self.root, copies=1, rse_expression=self.rses3,
    #                        grouping='NONE', weight=None, lifetime=None, locked=False, subscription_id=None)
    #
    #     requestss = get_transfer_requests_and_source_replicas(rses=[self.rses3])
    #     for requests in requestss:
    #         for request in requests:
    #             if requests[request]['rule_id'] == rule_id[0]:
    #                 assert requests[request]['sources'][0][1] == expected_src_url
    #                 assert requests[request]['sources'][0][1] == expected_src_url
    #                 assert requests[request]['dest_urls'][0] == expected_dst_url
