#!/usr/bin/env python

import logging
import unittest
import uuid
import random

import aiorgwadmin
from . import create_bucket, get_environment_creds

logging.basicConfig(level=logging.WARNING)


class RGWAdminTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rgw = aiorgwadmin.RGWAdmin(pool_connections=True,
                                        **get_environment_creds())
        self.user1 = 'foo1209'
        self.user2 = 'foo1213'
        self.user3 = 'bar3142'
        self.secret = aiorgwadmin.RGWAdmin.gen_secret_key()
        user1 = await self.rgw.create_user(uid=self.user1,
                                           email='%s@example.com' % self.user1,
                                           display_name='Unit Test %s' % self.user1,
                                           secret_key=self.secret)
        user2 = await self.rgw.create_user(uid=self.user2,
                                           display_name='Unit Test %s' % self.user2,
                                           secret_key=self.secret)
        self.user1_obj = user1
        self.user2_obj = user2
        self.assertTrue(user1['user_id'] == self.user1)
        self.assertTrue(user2['user_id'] == self.user2)

    async def asyncTearDown(self):
        await self.rgw.remove_user(uid=self.user1, purge_data=True)
        await self.rgw.remove_user(uid=self.user2, purge_data=True)
        await self.rgw.close()

    async def test_modify_user(self):
        before_keys = self.user1_obj['keys']
        user = await self.rgw.modify_user(uid=self.user1,
                                          email='%s@test.com' % self.user1)
        self.assertTrue(user['email'] == '%s@test.com' % self.user1)

        # ensure that generate_key is False and that new keys are not
        # being created by default when modifying the user.
        self.assertEqual(before_keys, user['keys'])

        # generate_key=True should add an extra pair of keys
        user = await self.rgw.modify_user(uid=self.user1, generate_key=True)
        self.assertNotEqual(before_keys, user['keys'])
        self.assertEqual(len(before_keys) + 1, len(user['keys']))

        # modifying a user doesn't automatically un-suspend them
        # setup - suspend them first
        user = await self.rgw.modify_user(uid=self.user1, suspended=True)
        self.assertEqual(user['suspended'], 1)

        # test - modify their username without unsuspending them
        user = await self.rgw.modify_user(uid=self.user1,
                                          email='%s@test2.com' % self.user1)
        self.assertTrue(user['email'] == '%s@test2.com' % self.user1)
        self.assertEqual(user['suspended'], 1)

        # unsuspend the user to allow other tests to function
        user = await self.rgw.modify_user(uid=self.user1, suspended=False)
        self.assertEqual(user['suspended'], 0)

    async def test_duplicate_email(self):
        with self.assertRaises(aiorgwadmin.exceptions.EmailExists):
            await self.rgw.create_user(uid=self.user3,
                                       email='%s@example.com' % self.user1,
                                       display_name='Unit Test %s' % self.user3,
                                       secret_key=aiorgwadmin.RGWAdmin.gen_secret_key())

    async def test_get_user(self):
        user = await self.rgw.get_user(uid=self.user2)
        self.assertTrue(user['display_name'] == 'Unit Test %s' % self.user2)
        self.assertTrue('stats' not in user)

        user = await self.rgw.get_user(uid=self.user2, stats=True)
        stats = user['stats']
        self.assertEqual(stats['size'], 0)
        self.assertEqual(stats['size_actual'], 0)

    async def test_get_users(self):
        users = await self.rgw.get_users()
        self.assertTrue(self.user1 in users)
        self.assertTrue(self.user2 in users)

    async def test_user_quota(self):
        size = random.randint(1000, 1000000)
        await self.rgw.set_user_quota(uid=self.user1, quota_type='user',
                                      max_size_kb=size, enabled=True)
        user1_quota_info = await self.rgw.get_user_quota(uid=self.user1)
        self.assertTrue(size == user1_quota_info['max_size_kb'])

    async def test_bucket_quota(self):
        size = random.randint(1000, 1000000)
        bucket_name = f"bucket-{uuid.uuid4()}"
        await create_bucket(name=bucket_name, owner=self.user1)
        await self.rgw.set_bucket_quota(uid=self.user1, bucket=bucket_name,
                                  max_size_kb=size, enabled=True)
        bucket = await self.rgw.get_bucket(bucket=bucket_name)
        self.assertTrue(bucket['bucket_quota']['max_size_kb'] == size)

    async def test_bucket(self):
        bucket_name = f"bucket-{uuid.uuid4()}"
        await create_bucket(name=bucket_name, owner=self.user1)
        bucket = await self.rgw.get_bucket(bucket=bucket_name)
        await self.rgw.link_bucket(bucket=bucket_name, bucket_id=bucket['id'],
                                   uid=self.user1)
        await self.rgw.get_bucket(uid=self.user1, bucket=bucket_name)
        await self.rgw.get_policy(bucket=bucket_name)
        await self.rgw.remove_bucket(bucket=bucket_name, purge_objects=True)
        self.assertTrue(True)

    async def test_get_usage(self):
        summary = await self.rgw.get_usage(show_summary=True)
        self.assertTrue('summary' in summary)

    async def test_subuser(self):
        await self.rgw.create_subuser(uid=self.user2,
                                      subuser='swift',
                                      key_type='swift',
                                      secret_key=self.secret)
        await self.rgw.modify_subuser(uid=self.user2,
                                      subuser='swift',
                                      access='write')
        subuser = await self.rgw.get_user(uid=self.user2)
        await self.rgw.remove_subuser(uid=self.user2, subuser='swift')
        for subs in subuser['subusers']:
            if subs['id'] == '%s:%s' % (self.user2, 'swift'):
                self.assertTrue(subs['permissions'] == 'write')

    async def test_s3_keys(self):
        access = aiorgwadmin.RGWAdmin.gen_secret_key(size=20)
        secret = aiorgwadmin.RGWAdmin.gen_secret_key(size=40)
        keys = await self.rgw.create_key(uid=self.user1,
                                         access_key=access,
                                         secret_key=secret)
        for key in keys:
            if key['access_key'] == access:
                self.assertTrue(key['secret_key'] == secret)

    def test_parse_rados_datestring(self):
        aiorgwadmin.RGWAdmin.parse_rados_datestring(u'2016-06-27T16:06:39.163Z')


if __name__ == '__main__':
    unittest.main()
