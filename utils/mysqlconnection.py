# -*- coding: utf-8 -*-

from twisted.enterprise import adbapi

MYSQL_PARAMS = {
    'host': 'localhost',
    'port': 3306,
    'charset': 'utf8',
    'use_unicode': True,
    'user': 'root',
    'passwd': '',
}

# Shortcut maps 'setting name' -> 'parmater name'.
MYSQL_PARAMS_MAP = {
    'MYSQL_HOST': 'host',
    'MYSQL_PORT': 'port',
    'MYSQL_DBNAME': 'db',
    'MYSQL_CHARSET': 'charset',
    'MYSQL_USE_UNICODE': 'use_unicode',
    'MYSQL_USER': 'user',
    'MYSQL_PASSWD': 'passwd',
}


def get_mysql_from_settings(settings):
    """Returns a mysql client instance from given Scrapy settings object.

    This function uses ``get_client`` to instantiate the client and uses
    ``defaults.MYSQL_PARAMS`` global as defaults values for the parameters. You
    can override them using the ``MYSQL_PARAMS`` setting.

    Parameters
    ----------
    settings : Settings
        A scrapy settings object. See the supported settings below.

    Returns
    -------
    server
        mysql client instance.

    Other Parameters
    ----------------
    MYSQL_HOST : str, optional
        Server host.
    MYSQL_PORT : str, optional
        Server port.
    MYSQL_DBNAME : str, optional
        db name.
    MYSQL_CHARSET : str, optional
        charset.

    """
    params = MYSQL_PARAMS.copy()
    p = settings.getdict('MYSQL_PARAMS')
    for source, dest in MYSQL_PARAMS_MAP.items():
        val = p.get(source)
        if val:
            params[dest] = val
    return adbapi.ConnectionPool('MySQLdb', **params)


# Backwards compatible alias.
from_settings = get_mysql_from_settings
