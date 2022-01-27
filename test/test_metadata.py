#!/usr/bin/env python

import json
import logging
import unittest
from urllib.parse import quote

import aiorgwadmin
from aiorgwadmin.utils import get_environment_creds, id_generator
from . import create_bucket

logging.basicConfig(level=logging.WARNING)


class MetadataTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.rgw = aiorgwadmin.RGWAdmin(**get_environment_creds())
        aiorgwadmin.RGWAdmin.set_connection(self.rgw)

    async def test_get_metadata(self):
        bucket_name = id_generator()
        self.assertTrue(bucket_name not in await self.rgw.get_metadata('bucket'))
        await create_bucket(self.rgw, bucket=bucket_name)
        self.assertTrue(bucket_name in await self.rgw.get_metadata('bucket'))
        await self.rgw.remove_bucket(bucket=bucket_name, purge_objects=True)

    async def test_put_metadata(self):
        bucket_name = id_generator()
        self.assertTrue(bucket_name not in await self.rgw.get_metadata('bucket'))
        await create_bucket(self.rgw, bucket=bucket_name)

        ret_json = await self.rgw.get_metadata('bucket', key=bucket_name)
        self.assertEqual(ret_json['data']['bucket']['name'], bucket_name)
        json_str = json.dumps(ret_json)

        await self.rgw.put_metadata('bucket', key=bucket_name, json_string=json_str)
        await self.rgw.remove_bucket(bucket=bucket_name, purge_objects=True)

    async def test_metadata_lock_unlock(self):
        bucket_name = id_generator()
        await create_bucket(self.rgw, bucket=bucket_name)
        await self.rgw.lock_metadata('bucket', key=bucket_name, lock_id='abc',
                               length=5)
        await self.rgw.unlock_metadata('bucket', key=bucket_name, lock_id='abc')
        await self.rgw.remove_bucket(bucket=bucket_name, purge_objects=True)

    async def test_invalid_metadata_unlock(self):
        with self.assertRaises(aiorgwadmin.exceptions.NoSuchKey):
            key = id_generator()
            await self.rgw.unlock_metadata('bucket', key=key, lock_id='abc')

    async def test_metadata_type_valid(self):
        with self.assertRaises(Exception):
            await self.rgw.get_metadata('bucketttt')

    async def test_get_bucket_instances(self):
        bucket_name = id_generator()
        await create_bucket(self.rgw, bucket=bucket_name)
        instances = await self.rgw.get_bucket_instances()
        bucket = await self.rgw.get_bucket(bucket_name)
        expected_instance = '%s:%s' % (bucket_name, bucket['id'])
        self.assertTrue(expected_instance in instances)
        await self.rgw.remove_bucket(bucket=bucket_name, purge_objects=True)

    def test_metadata_marker(self):
        self.assertEqual('default.345%20-5', quote('default.345 -5'))


if __name__ == '__main__':
    unittest.main()
