import aioboto3
from pydantic import BaseSettings

from aiorgwadmin import RGWAdmin


class CephSettings(BaseSettings):
    server: str
    access_key: str
    secret_key: str
    verify: bool = True
    secure: bool = True

    @property
    def s3_url(self) -> str:
        protocol = "https" if self.secure else "http"
        return f"{protocol}://{self.server}"

    class Config:
        env_prefix = "ceph_"


async def create_bucket(name: str, owner: str):
    """
    This is a helper function for tests to create buckets so they can assert changes
    on the gateway based on the bucket's existence.
    """
    rgw = RGWAdmin(**get_environment_creds())
    user = await rgw.get_user(owner)
    key = user["keys"][0]

    ceph = CephSettings()

    async with aioboto3.Session().resource(
            "s3",
            endpoint_url=ceph.s3_url,
            aws_access_key_id=key["access_key"],
            aws_secret_access_key=key["secret_key"],
            verify=ceph.verify,
    ) as s3:
        bucket = await s3.Bucket(name)
        await bucket.create()


def get_environment_creds():
    ceph = CephSettings()
    return {'access_key': ceph.access_key,
            'secret_key': ceph.secret_key,
            'server': ceph.server,
            'secure': ceph.secure,
            'verify': ceph.verify}
