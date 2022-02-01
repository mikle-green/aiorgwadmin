#!/usr/bin/env python

import logging
import unittest
import uuid

import aiorgwadmin
from aiorgwadmin.user import RGWUser
from . import get_environment_creds

logging.basicConfig(level=logging.WARNING)


class RGWUserTest(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.rgw = aiorgwadmin.RGWAdmin(**get_environment_creds())
        aiorgwadmin.RGWAdmin.set_connection(self.rgw)

        self.user = f"user-{uuid.uuid4()}"

    async def asyncTearDown(self):
        users = await self.rgw.get_users()

        if self.user in users:
            await self.rgw.remove_user(uid=self.user, purge_data=True)

    async def test_create_user(self):
        display_name = f"Test Create User"
        u = await RGWUser.create(user_id=self.user, display_name=display_name)
        self.assertTrue(u.user_id == self.user and
                        u.display_name == display_name)
        await u.delete()

    async def test_user_exists(self):
        display_name = "Test User Exists"
        u = await RGWUser.create(user_id=self.user, display_name=display_name)
        self.assertTrue(await u.exists())
        await u.delete()
        self.assertFalse(await u.exists())
        await u.save()
        self.assertTrue(await u.exists())

    async def test_set_quota(self):
        display_name = "Test Set Quota"
        u = await RGWUser.create(user_id=self.user, display_name=display_name)
        u.user_quota.size = 1024000
        await u.save()
        nu = await RGWUser.fetch(u.user_id)
        self.assertTrue(u.user_quota.size == nu.user_quota.size)
        await nu.delete()


if __name__ == '__main__':
    unittest.main()
