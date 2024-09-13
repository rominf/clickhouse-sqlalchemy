import configparser
import tempfile
from contextlib import suppress

from sqlalchemy.dialects import registry

from tests import log


registry.register(
    "clickhouse", "clickhouse_sqlalchemy.drivers.http.base", "dialect"
)
registry.register(
    "clickhouse.native", "clickhouse_sqlalchemy.drivers.native.base", "dialect"
)
registry.register(
    "clickhouse.asynch", "clickhouse_sqlalchemy.drivers.asynch.base", "dialect"
)
registry.register(
    "clickhouse.chdb", "clickhouse_sqlalchemy.drivers.chdb.base", "dialect"
)

file_config = configparser.ConfigParser()
file_config.read(['setup.cfg'])

log.configure(file_config.get('log', 'level'))

host = file_config.get('db', 'host')
port = file_config.getint('db', 'port')
http_port = file_config.getint('db', 'http_port')
database = file_config.get('db', 'database')
user = file_config.get('db', 'user')
password = file_config.get('db', 'password')

uri_template = '{schema}://{user}:{password}@{host}:{port}/{database}'

http_uri = uri_template.format(
    schema='clickhouse+http', user=user, password=password, host=host,
    port=http_port, database=database)
native_uri = uri_template.format(
    schema='clickhouse+native', user=user, password=password, host=host,
    port=port, database=database)
asynch_uri = uri_template.format(
    schema='clickhouse+asynch', user=user, password=password, host=host,
    port=port, database=database)

# *_uri should be refactored as fixtures to allow chdb_uri use tmp_path fixture
# This is quick and dirty solution to minimize the diff.
import atexit

def rm_chdb_tmp_dir():
    import shutil
    with suppress(Exception):
        shutil.rmtree(chdb_tmp_dir)

atexit.register(rm_chdb_tmp_dir)
chdb_tmp_dir = tempfile.mkdtemp("chdb-test")

chdb_uri_template = '{schema}:///{database}?path={path}'
chdb_uri = chdb_uri_template.format(
    schema='clickhouse+chdb', database=database, path=chdb_tmp_dir)


system_http_uri = uri_template.format(
    schema='clickhouse+http', user=user, password=password, host=host,
    port=http_port, database='system')
system_native_uri = uri_template.format(
    schema='clickhouse+native', user=user, password=password, host=host,
    port=port, database='system')
system_asynch_uri = uri_template.format(
    schema='clickhouse+asynch', user=user, password=password, host=host,
    port=port, database='system')
