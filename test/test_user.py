#!/usr/bin/env python

import logging
import aiorgwadmin
import unittest
import uuid

from aiorgwadmin.user import RGWUser
from . import get_environment_creds

logging.basicConfig(level=logging.WARNING)


class RGWUserTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        rgw = aiorgwadmin.RGWAdmin(**get_environment_creds())
        aiorgwadmin.RGWAdmin.set_connection(rgw)

    async def test_create_user(self):
        user_id = f"bucket-{uuid.uuid4()}"
        display_name = f"Test Create Bucket"
        u = await RGWUser.create(user_id=user_id, display_name=display_name)
        self.assertTrue(u.user_id == user_id and
                        u.display_name == display_name)
        await u.delete()

    async def test_user_exists(self):
        user_id = f"bucket-{uuid.uuid4()}"
        display_name = "Test User Exists"
        u = await RGWUser.create(user_id=user_id, display_name=display_name)
        self.assertTrue(await u.exists())
        await u.delete()
        self.assertFalse(await u.exists())
        await u.save()
        self.assertTrue(await u.exists())

    async def test_set_quota(self):
        user_id = f"bucket-{uuid.uuid4()}"
        display_name = "Test Set Quota"
        u = await RGWUser.create(user_id=user_id, display_name=display_name)
        u.user_quota.size = 1024000
        await u.save()
        nu = await RGWUser.fetch(u.user_id)
        self.assertTrue(u.user_quota.size == nu.user_quota.size)
        await nu.delete()


if __name__ == '__main__':
    unittest.main()
