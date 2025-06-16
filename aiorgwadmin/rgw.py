import json
import logging
import random
import string
import time
from collections.abc import Sequence
from datetime import datetime
from http import HTTPStatus
from io import StringIO
from ssl import SSLContext
from typing import Any, ClassVar, Final
from urllib.parse import quote

from aiohttp import ClientResponse, ClientSession, ClientTimeout, TCPConnector
from awsauth import S3Auth
from requests import Request

from .exceptions import (
    RGWAdminException, AccessDenied, UserExists,
    InvalidAccessKey, InvalidSecretKey, InvalidKeyType,
    KeyExists, EmailExists, SubuserExists, InvalidAccess,
    IndexRepairFailed, BucketNotEmpty, ObjectRemovalFailed,
    BucketUnlinkFailed, BucketLinkFailed, NoSuchObject,
    IncompleteBody, InvalidCap, NoSuchCap,
    InternalError, NoSuchUser, NoSuchBucket, NoSuchKey,
    ServerDown, InvalidQuotaType, InvalidArgument, BucketAlreadyExists,
)

log: Final = logging.getLogger(__name__)
LETTERS: Final = string.ascii_letters


class RGWAdmin:
    _access_key: str
    _secret_key: str
    _server: str
    _admin: str
    _response: str
    _ssl_context: SSLContext | None
    _verify: bool
    _protocol: str
    _timeout: float | None
    _session: ClientSession | None

    connection: ClassVar['RGWAdmin']

    metadata_types: ClassVar[list[str]] = ['user', 'bucket', 'bucket.instance']

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        server: str,
        admin: str = 'admin',
        response: str = 'json',
        ssl_context: SSLContext | None = None,
        secure: bool = True,
        verify: bool = True,
        timeout: float | None = None,
        pool_connections: bool = False,
    ) -> None:
        self._access_key = access_key
        self._secret_key = secret_key
        self._server = server
        self._admin = admin
        self._response = response
        self._session = None

        # ssl support
        self._ssl_context = ssl_context
        self._verify = verify
        if secure:
            self._protocol = 'https'
        else:
            self._protocol = 'http'

        self._timeout = timeout
        self._skip_auto_headers = ["Content-Type"]

        self._auth = S3Auth(self._access_key, self._secret_key, self._server)

        if pool_connections:
            self._session = self._get_session()

    async def __aenter__(self) -> 'RGWAdmin':
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    def _get_session(self) -> ClientSession:
        if self._ssl_context:
            ssl = self._ssl_context
        else:
            ssl = self._verify

        return ClientSession(
            connector=TCPConnector(ssl=ssl),
            skip_auto_headers=self._skip_auto_headers,
            timeout=ClientTimeout(self._timeout),
        )

    @classmethod
    def connect(cls, **kwargs: Any) -> None:
        """Establish a new connection to RGWAdmin

        Only one connection can be active in any single process
        """
        cls.set_connection(cls(**kwargs))

    @classmethod
    def set_connection(cls, connection: 'RGWAdmin') -> None:
        """Set a connection for the RGWAdmin session to use."""
        cls.connection = connection

    @classmethod
    def get_connection(cls) -> 'RGWAdmin':
        """Return the RGWAdmin connection that was set"""
        return cls.connection

    def __repr__(self) -> str:
        return "%s (%s)" % (self.__class__.__name__, self.get_base_url())

    def __str__(self) -> str:
        returning = self.__repr__()
        returning += '\nAccess Key: %s\n' % self._access_key
        returning += 'Secret Key: ******\n'
        returning += 'Response Method: %s\n' % self._response
        if self._ssl_context is not None:
            returning += 'SSL Context: %s\n' % self._ssl_context
        return returning

    def get_base_url(self) -> str:
        '''Return a base URL.  I.e. https://ceph.server'''
        return '%s://%s' % (self._protocol, self._server)

    @staticmethod
    async def _load_request(r: ClientResponse) -> Any:
        '''Load the request given as JSON handling exceptions if necessary'''
        try:
            j = await r.json(content_type=None)
        except ValueError:
            # some calls in the admin API encode the info in the headers
            # instead of the body.  The code that follows is an ugly hack
            # due to the fact that there's a bug in the admin API we're
            # interfacing with.

            # set a default value for j in case we don't find json in the
            # headers below
            j = None

            # find a key with a '{', since this will hold the json response
            for k, v in r.headers.items():
                if '{' in k:
                    json_string = ":".join([k, v]).split('}')[0] + '}'
                    j = json.load(StringIO(json_string))
                    break

        if r.status == HTTPStatus.OK:
            return j
        elif r.status == HTTPStatus.NO_CONTENT:
            return None
        else:
            if j is not None:
                code = str(j.get('Code', 'InternalError'))
            else:
                raise ServerDown(r.status)

            for e in [AccessDenied, UserExists, InvalidAccessKey,
                      InvalidKeyType, InvalidSecretKey, KeyExists, EmailExists,
                      SubuserExists, InvalidAccess, InvalidArgument,
                      IndexRepairFailed, BucketNotEmpty, ObjectRemovalFailed,
                      BucketUnlinkFailed, BucketLinkFailed, NoSuchObject,
                      InvalidCap, NoSuchCap, NoSuchUser, NoSuchBucket,
                      NoSuchKey, IncompleteBody, BucketAlreadyExists,
                      InternalError]:
                if code == e.__name__:
                    raise e(j)

            raise RGWAdminException(code, raw=j)

    async def request(self, method: str, request: str, headers: dict | None = None, data: Any = None) -> Any:
        url = '%s%s' % (self.get_base_url(), request)
        log.debug('URL: %s' % url)
        log.debug('Access Key: %s' % self._access_key)
        log.debug('Verify: %s, SSL Context: %s' % (self._verify, self._ssl_context))

        # prepare headers for auth
        prepped = Request(method, url, headers=headers, auth=self._auth).prepare()
        prepped_headers = prepped.headers

        if data is not None:
            prepped_headers["Content-Length"] = str(len(data))

        request_params = {
            "method": method,
            "url": url,
            "headers": prepped_headers,
            "data": data,
        }

        if self._session:
            # use connection pool
            async with self._session.request(**request_params) as response:
                return await self._load_request(response)
        else:
            # do not use connection pool
            async with self._get_session() as session:
                async with session.request(**request_params) as response:
                    return await self._load_request(response)

    async def _request_metadata(
        self,
        method: str,
        metadata_type: str,
        params: dict | None = None,
        headers: dict | None = None,
        data: Any = None,
    ) -> Any:
        if metadata_type not in self.metadata_types:
            raise Exception("Bad metadata_type")

        if params is None:
            params = {}
        params = '&'.join(['%s=%s' % (k, v) for k, v in params.items()])
        request = '/%s/metadata/%s?%s' % (self._admin, metadata_type, params)
        return await self.request(
            method=method,
            request=request,
            headers=headers,
            data=data,
        )

    async def get_metadata(
        self,
        metadata_type: str,
        key: str | None = None,
        max_entries: int | None = None,
        marker: str | None = None,
        headers: dict | None = None,
    ) -> Any:
        ''' Returns a JSON object representation of the metadata '''
        params = {'format': self._response}
        if key is not None:
            params['key'] = key
        if marker is not None:
            params['marker'] = quote(marker)
        if max_entries is not None:
            params['max-entries'] = max_entries
        return await self._request_metadata(
            method='get',
            metadata_type=metadata_type,
            params=params,
            headers=headers,
        )

    async def put_metadata(self, metadata_type: str, key: str, json_string: str) -> Any:
        return await self._request_metadata(
            method='put',
            metadata_type=metadata_type,
            params={'key': key},
            headers={'Content-Type': 'application/json'},
            data=json_string,
        )

    # Alias for compatability:
    set_metadata = put_metadata

    async def delete_metadata(self, metadata_type: str, key: str) -> Any:
        return await self._request_metadata(
            method='delete',
            metadata_type=metadata_type,
            params={'key': key},
        )

    async def lock_metadata(self, metadata_type: str, key: str, lock_id: str, length: int) -> Any:
        params = {
            'lock': 'lock',
            'key': key,
            'lock_id': lock_id,
            'length': length,
        }
        return await self._request_metadata(
            method='post',
            metadata_type=metadata_type,
            params=params,
        )

    async def unlock_metadata(self, metadata_type: str, key: str, lock_id: str) -> Any:
        params = {
            'unlock': 'unlock',
            'key': key,
            'lock_id': lock_id,
        }
        return await self._request_metadata(
            method='post',
            metadata_type=metadata_type,
            params=params,
        )

    async def get_user(
        self,
        uid: str | None = None,
        access_key: str | None = None,
        stats: bool = False,
        sync: bool = False,
    ) -> Any:
        if uid is not None and access_key is not None:
            raise ValueError('Only one of uid and access_key is allowed')
        parameters = ''
        if uid is not None:
            parameters += '&uid=%s' % uid
        if access_key is not None:
            parameters += '&access-key=%s' % access_key
        parameters += '&stats=%s&sync=%s' % (stats, sync)
        return await self.request('get', '/%s/user?format=%s%s' % (self._admin, self._response, parameters))

    async def get_users(self) -> Any:
        return await self.get_metadata(metadata_type='user')

    async def create_user(
        self,
        uid: str,
        display_name: str,
        email: str | None = None,
        key_type: str = 's3',
        access_key: str | None = None,
        secret_key: str | None = None,
        user_caps: str | None = None,
        generate_key: bool = True,
        max_buckets: int | None = None,
        suspended: bool = False,
    ) -> Any:
        parameters = 'uid=%s&display-name=%s' % (uid, display_name)
        if email is not None:
            parameters += '&email=%s' % email
        if key_type is not None:
            parameters += '&key-type=%s' % key_type
        if access_key is not None:
            parameters += '&access-key=%s' % access_key
        if secret_key is not None:
            parameters += '&secret-key=%s' % secret_key
        if user_caps is not None:
            parameters += '&user-caps=%s' % user_caps
        parameters += '&generate-key=%s' % generate_key
        if max_buckets is not None:
            parameters += '&max-buckets=%s' % max_buckets
        parameters += '&suspended=%s' % suspended
        return await self.request('put', '/%s/user?format=%s&%s' % (self._admin, self._response, parameters))

    async def get_usage(
        self,
        uid: str | None = None,
        start: str | datetime | None = None,
        end: str | datetime | None = None,
        show_entries: bool = False,
        show_summary: bool = False,
    ) -> Any:
        parameters = ''
        if uid is not None:
            parameters += '&uid=%s' % uid
        if start is not None:
            parameters += '&start=%s' % start
        if end is not None:
            parameters += '&end=%s' % end
        parameters += '&show-entries=%s' % show_entries
        parameters += '&show-summary=%s' % show_summary
        return await self.request('get', '/%s/usage?format=%s%s' % (self._admin, self._response, parameters))

    async def trim_usage(
        self,
        uid: str | None = None,
        start: str | datetime | None = None,
        end: str | datetime | None = None,
        remove_all: bool = False,
    ) -> Any:
        parameters = ''
        if uid is not None:
            parameters += '&uid=%s' % uid
        if start is not None:
            parameters += '&start=%s' % start
        if end is not None:
            parameters += '&end=%s' % end
        parameters += '&remove-all=%s' % remove_all
        return await self.request('delete', '/%s/usage?format=%s%s' % (self._admin, self._response, parameters))

    async def modify_user(
        self,
        uid: str,
        display_name: str | None = None,
        email: str | None = None,
        key_type: str = 's3',
        access_key: str | None = None,
        secret_key: str | None = None,
        user_caps: str | None = None,
        generate_key: bool = False,
        max_buckets: int | None = None,
        suspended: bool | None = None,
    ) -> Any:
        parameters = 'uid=%s' % uid
        if display_name is not None:
            parameters += '&display-name=%s' % display_name
        if email is not None:
            parameters += '&email=%s' % email
        if key_type is not None:
            parameters += '&key-type=%s' % key_type
        if access_key is not None:
            parameters += '&access-key=%s' % access_key
        if secret_key is not None:
            parameters += '&secret-key=%s' % secret_key
        if user_caps is not None:
            parameters += '&user-caps=%s' % user_caps
        parameters += '&generate-key=%s' % generate_key
        if max_buckets is not None:
            parameters += '&max-buckets=%s' % max_buckets
        if suspended is not None:
            parameters += '&suspended=%s' % suspended
        return await self.request('post', '/%s/user?format=%s&%s' % (self._admin, self._response, parameters))

    async def get_quota(self, uid: str, quota_type: str) -> Any:
        if quota_type not in ['user', 'bucket']:
            raise InvalidQuotaType
        parameters = 'uid=%s&quota-type=%s' % (uid, quota_type)
        return await self.request('get', '/%s/user?quota&format=%s&%s' % (self._admin, self._response, parameters))

    async def get_user_quota(self, uid: str) -> Any:
        return await self.get_quota(uid=uid, quota_type='user')

    async def get_user_bucket_quota(self, uid: str) -> Any:
        '''Return the quota set on every bucket owned/created by a user'''
        return await self.get_quota(uid=uid, quota_type='bucket')

    @staticmethod
    def _quota(max_size_kb: int | None = None, max_objects: int | None = None, enabled: bool | None = None) -> str:
        quota = ''
        if max_size_kb is not None:
            quota += '&max-size-kb=%d' % max_size_kb
        if max_objects is not None:
            quota += '&max-objects=%d' % max_objects
        if enabled is not None:
            quota += '&enabled=%s' % str(enabled).lower()
        return quota

    async def set_user_quota(
        self,
        uid: str,
        quota_type: str,
        max_size_kb: int | None = None,
        max_objects: int | None = None,
        enabled: bool | None = None,
    ) -> Any:
        '''
        Set quotas on users and buckets owned by users

        If `quota_type` is user, then the quota applies to the user.  If
        `quota_type` is bucket, then the quota applies to buckets owned by
        the specified uid.

        If you want to set a quota on an individual bucket, then use
        set_bucket_quota() instead.
        '''
        if quota_type not in ['user', 'bucket']:
            raise InvalidQuotaType
        quota = self._quota(max_size_kb=max_size_kb, max_objects=max_objects, enabled=enabled)
        parameters = 'uid=%s&quota-type=%s%s' % (uid, quota_type, quota)
        return await self.request('put', '/%s/user?quota&format=%s&%s' % (self._admin, self._response, parameters))

    async def set_bucket_quota(
        self,
        uid: str,
        bucket: str,
        max_size_kb: int | None = None,
        max_objects: int | None = None,
        enabled: bool | None = None,
    ) -> Any:
        '''Set the quota on an individual bucket'''
        quota = self._quota(max_size_kb=max_size_kb, max_objects=max_objects, enabled=enabled)
        parameters = 'uid=%s&bucket=%s%s' % (uid, bucket, quota)
        return await self.request('put', '/%s/bucket?quota&format=%s&%s' % (self._admin, self._response, parameters))

    async def remove_user(self, uid: str, purge_data: bool = False) -> Any:
        parameters = 'uid=%s' % uid
        parameters += '&purge-data=%s' % purge_data
        return await self.request('delete', '/%s/user?format=%s&%s' % (self._admin, self._response, parameters))

    async def create_subuser(
        self,
        uid: str,
        subuser: str | None = None,
        secret_key: str | None = None,
        access_key: str | None = None,
        key_type: str | None = None,
        access: str | None = None,
        generate_secret: bool = False,
    ) -> Any:
        parameters = 'uid=%s' % uid
        if subuser is not None:
            parameters += '&subuser=%s' % subuser
        if secret_key is not None and access_key is not None:
            parameters += '&access-key=%s' % access_key
            parameters += '&secret-key=%s' % secret_key
        if key_type is not None and key_type.lower() in ['s3', 'swift']:
            parameters += '&key-type=%s' % key_type
        if access is not None:
            parameters += '&access=%s' % access
        parameters += '&generate-secret=%s' % generate_secret
        return await self.request('put', '/%s/user?subuser&format=%s&%s' % (self._admin, self._response, parameters))

    async def modify_subuser(
        self,
        uid: str,
        subuser: str,
        secret: str | None = None,
        key_type: str = 'swift',
        access: str | None = None,
        generate_secret: bool = False,
    ) -> Any:
        parameters = 'uid=%s&subuser=%s' % (uid, subuser)
        if secret is not None:
            parameters += '&secret=%s' % secret
        parameters += '&key-type=%s' % key_type
        if access is not None:
            parameters += '&access=%s' % access
        parameters += '&generate-secret=%s' % generate_secret
        return await self.request('post', '/%s/user?subuser&format=%s&%s' % (self._admin, self._response, parameters))

    async def remove_subuser(self, uid: str, subuser: str, purge_keys: bool = True) -> Any:
        parameters = 'uid=%s&subuser=%s&purge-keys=%s' % (uid, subuser, purge_keys)
        return await self.request('delete', '/%s/user?subuser&format=%s&%s' % (self._admin, self._response, parameters))

    async def create_key(
        self,
        uid: str,
        subuser: str | None = None,
        key_type: str = 's3',
        access_key: str | None = None,
        secret_key: str | None = None,
        generate_key: bool = True,
    ) -> Any:
        parameters = 'uid=%s' % uid
        if subuser is not None:
            parameters += '&subuser=%s' % subuser
        parameters += '&key-type=%s' % key_type
        if access_key is not None:
            parameters += '&access-key=%s' % access_key
        if secret_key is not None:
            parameters += '&secret-key=%s' % secret_key
        parameters += '&generate-key=%s' % generate_key
        return await self.request('put', '/%s/user?key&format=%s&%s' % (self._admin, self._response, parameters))

    async def remove_key(
        self,
        access_key: str,
        key_type: str | None = None,
        uid: str | None = None,
        subuser: str | None = None,
    ) -> Any:
        parameters = 'access-key=%s' % access_key
        if key_type is not None:
            parameters += '&key-type=%s' % key_type
        if uid is not None:
            parameters += '&uid=%s' % uid
        if subuser is not None:
            parameters += '&subuser=%s' % subuser
        return await self.request('delete', '/%s/user?key&format=%s&%s' % (self._admin, self._response, parameters))

    async def get_buckets(self) -> Any:
        '''Returns a list of all buckets in the radosgw'''
        return await self.get_metadata(metadata_type='bucket')

    async def get_bucket(self, bucket: str | None = None, uid: str | None = None, stats: bool = False) -> Any:
        parameters = ''
        if bucket is not None:
            parameters += '&bucket=%s' % bucket
        if uid is not None:
            parameters += '&uid=%s' % uid
        parameters += '&stats=%s' % stats
        return await self.request('get', '/%s/bucket?format=%s%s' % (self._admin, self._response, parameters))

    async def check_bucket_index(self, bucket: str, check_objects: bool = False, fix: bool = False) -> Any:
        parameters = 'bucket=%s' % bucket
        parameters += '&check-objects=%s' % check_objects
        parameters += '&fix=%s' % fix
        return await self.request('get', '/%s/bucket?index&format=%s&%s' % (self._admin, self._response, parameters))

    async def remove_bucket(self, bucket: str, purge_objects: bool = False) -> Any:
        parameters = 'bucket=%s' % bucket
        parameters += '&purge-objects=%s' % purge_objects
        return await self.request('delete', '/%s/bucket?format=%s&%s' % (self._admin, self._response, parameters))

    async def unlink_bucket(self, bucket: str, uid: str) -> Any:
        parameters = 'bucket=%s&uid=%s' % (bucket, uid)
        return await self.request('post', '/%s/bucket?format=%s&%s' % (self._admin, self._response, parameters))

    async def link_bucket(self, bucket: str, bucket_id: str, uid: str) -> Any:
        # note that even though the Ceph docs say that bucket-id is optional
        # the API call will fail (InvalidArgument) if it is omitted.
        parameters = 'bucket=%s&bucket-id=%s&uid=%s' % (bucket, bucket_id, uid)
        return await self.request('put', '/%s/bucket?format=%s&%s' % (self._admin, self._response, parameters))

    async def remove_object(self, bucket: str, object_name: str) -> Any:
        parameters = 'bucket=%s&object=%s' % (bucket, object_name)
        return await self.request('delete', '/%s/bucket?object&format=%s&%s' % (self._admin, self._response, parameters))

    async def get_policy(self, bucket: str, object_name: str | None = None) -> Any:
        parameters = 'bucket=%s' % bucket
        if object_name is not None:
            parameters += '&object=%s' % object_name
        return await self.request('get', '/%s/bucket?policy&format=%s&%s' % (self._admin, self._response, parameters))

    async def add_capability(self, uid: str, user_caps: str) -> Any:
        parameters = 'uid=%s&user-caps=%s' % (uid, user_caps)
        return await self.request('put', '/%s/user?caps&format=%s&%s' % (self._admin, self._response, parameters))

    async def remove_capability(self, uid: str, user_caps: str) -> Any:
        parameters = 'uid=%s&user-caps=%s' % (uid, user_caps)
        return await self.request('delete', '/%s/user?caps&format=%s&%s' % (self._admin, self._response, parameters))

    async def get_bucket_instances(self) -> Any:
        '''Returns a list of all bucket instances in the radosgw'''
        return await self.get_metadata(metadata_type='bucket.instance')

    @staticmethod
    def parse_rados_datestring(s: str) -> time.struct_time:
        return time.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def gen_secret_key(size: int = 40, chars: Sequence = LETTERS + string.digits) -> str:
        return ''.join(random.choice(chars) for _ in range(size))
