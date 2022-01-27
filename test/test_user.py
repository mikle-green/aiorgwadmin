#!/usr/bin/env python

import logging
import aiorgwadmin
import unittest
from aiorgwadmin.utils import get_environment_creds, id_generator
from aiorgwadmin.user import RGWUser

logging.basicConfig(level=logging.WARNING)


class RGWUserTest(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        rgw = aiorgwadmin.RGWAdmin(**get_environment_creds())
        aiorgwadmin.RGWAdmin.set_connection(rgw)

    async def test_create_user(self):
        user_id = id_generator()
        display_name = id_generator(25)
        u = await RGWUser.create(user_id=user_id, display_name=display_name)
        self.assertTrue(u.user_id == user_id and
                        u.display_name == display_name)
        await u.delete()

    async def test_user_exists(self):
        user_id = id_generator()
        display_name = id_generator(25)
        u = await RGWUser.create(user_id=user_id, display_name=display_name)
        self.assertTrue(await u.exists())
        await u.delete()
        self.assertFalse(await u.exists())
        await u.save()
        self.assertTrue(await u.exists())

    async def test_set_quota(self):
        user_id = id_generator()
        display_name = id_generator(25)
        u = await RGWUser.create(user_id=user_id, display_name=display_name)
        u.user_quota.size = 1024000
        await u.save()
        nu = await RGWUser.fetch(u.user_id)
        self.assertTrue(u.user_quota.size == nu.user_quota.size)
        await nu.delete()


if __name__ == '__main__':
    unittest.main()
