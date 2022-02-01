#!/usr/bin/env python

import json
import logging
import unittest
import uuid
from urllib.parse import quote

import aiorgwadmin
from . import create_bucket, get_environment_creds

logging.basicConfig(level=logging.WARNING)


class MetadataTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rgw = aiorgwadmin.RGWAdmin(**get_environment_creds())
        aiorgwadmin.RGWAdmin.set_connection(self.rgw)

        self.user = f"user-{uuid.uuid4()}"
        await self.rgw.create_user(uid=self.user, display_name=f"Unit Test {self.user}")

    async def asyncTearDown(self):
        await self.rgw.remove_user(uid=self.user, purge_data=True)

    async def test_get_metadata(self):
        bucket_name = f"bucket-{uuid.uuid4()}"
        self.assertTrue(bucket_name not in await self.rgw.get_metadata('bucket'))

        await create_bucket(bucket_name, owner=self.user)
        self.assertTrue(bucket_name in await self.rgw.get_metadata('bucket'))

    async def test_put_metadata(self):
        bucket_name = f"bucket-{uuid.uuid4()}"
        await create_bucket(bucket_name, owner=self.user)

        ret_json = await self.rgw.get_metadata('bucket', key=bucket_name)
        self.assertEqual(ret_json['data']['bucket']['name'], bucket_name)
        json_str = json.dumps(ret_json)

        await self.rgw.put_metadata('bucket', key=bucket_name, json_string=json_str)

    async def test_metadata_lock_unlock(self):
        bucket_name = f"bucket-{uuid.uuid4()}"
        await create_bucket(bucket_name, owner=self.user)

        await self.rgw.lock_metadata('bucket', key=bucket_name, lock_id='abc',
                                     length=5)
        await self.rgw.unlock_metadata('bucket', key=bucket_name, lock_id='abc')

    async def test_invalid_metadata_unlock(self):
        with self.assertRaises(aiorgwadmin.exceptions.NoSuchKey):
            key = f"bucket-{uuid.uuid4()}"
            await self.rgw.unlock_metadata('bucket', key=key, lock_id='abc')

    async def test_metadata_type_valid(self):
        with self.assertRaises(Exception):
            await self.rgw.get_metadata('bucketttt')

    async def test_get_bucket_instances(self):
        bucket_name = f"bucket-{uuid.uuid4()}"
        await create_bucket(bucket_name, owner=self.user)

        instances = await self.rgw.get_bucket_instances()
        bucket = await self.rgw.get_bucket(bucket_name)
        expected_instance = '%s:%s' % (bucket_name, bucket['id'])
        self.assertTrue(expected_instance in instances)

    def test_metadata_marker(self):
        self.assertEqual('default.345%20-5', quote('default.345 -5'))


if __name__ == '__main__':
    unittest.main()
