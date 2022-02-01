# aiorgwadmin

[![Documentation Status](https://readthedocs.org/projects/rgwadmin/badge/?version=latest)](https://rgwadmin.readthedocs.io/en/latest/?badge=latest)

aiorgwadmin is fork of rgwadmin library.

aiorgwadmin is an async Python library to access the Ceph Object Storage Admin API.

http://docs.ceph.com/docs/master/radosgw/adminops/


## API Example Usage

```python
import asyncio
from aiorgwadmin import RGWAdmin

async def main():
    rgw = RGWAdmin(access_key='XXX', secret_key='XXX', server='obj.example.com')
    await rgw.create_user(
        uid='liam',
        display_name='Liam Monahan',
        email='liam@umiacs.umd.edu',
        user_caps='usage=read, write; users=read',
        max_buckets=1000)
    await rgw.set_user_quota(
        uid='liam',
        quota_type='user',
        max_size_kb=1024 * 1024,
        enabled=True)
    await rgw.remove_user(uid='liam', purge_data=True)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## User Example Usage

```python
import asyncio
from aiorgwadmin import RGWAdmin, RGWUser

async def main():
    RGWAdmin.connect(access_key='XXX', secret_key='XXX', server='obj.example.com')
    u = await RGWUser.create(user_id='test', display_name='Test User')
    u.user_quota.size = 1024 * 1024  # in bytes
    u.user_quota.enabled = True
    await u.save()
    await u.delete()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## Requirements

aiorgwadmin requires the following Python packages:

 * [aiohttp](https://docs.aiohttp.org)
 * [requests](http://python-requests.org/)
 * [requests-aws](https://github.com/tax/python-requests-aws)

Additionally, you need to have a [Ceph](http://www.ceph.org) Object Storage
instance with a user that has appropriate caps (capabilities) on the parts of
the API that you want to access.  See the
[Ceph Object Storage](http://docs.ceph.com/docs/master/radosgw/) page for more
information.

### Compatibility
aiorgwadmin implements all documented Admin API operations or recent versions of
Ceph.  We also implement some of the undocumented ones, too...

## Installation

```pip install aiorgwadmin```


## License

    rgwadmin - a Python interface to the Rados Gateway Admin API
    Copyright (C) 2015  UMIACS

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

    Email:
        github@umiacs.umd.edu
