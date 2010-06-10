#!/usr/bin/env python

# Copyright 2010 Wendall Cada (wendall911) <wendallc AT 83864 DOT com>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
See `python replicate.py --help` for usage.

"""
from couchdbkit import Database, ResourceNotFound, PreconditionFailed, RequestFailed, CouchdbResource
from restkit.util import url_quote

import sys, optparse, time

def db_missing(url, db):
    print 'Database "%s" was not found on %s\n' % (db, url),
    sys.stdout.flush()
    sys.exit(1)

def main():
    usage = '%prog [options]\n  See %prog --help for a full list of options.'
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--source-url',
        action='store',
        dest='source_url',
        help='Url of the source server.')
    parser.add_option('--target-url',
        action='store',
        dest='target_url',
        help='Url of the target server.')
    parser.add_option('--source-db',
        action='store',
        dest='source_db',
        help='Source database name.')
    parser.add_option('--target-db',
        action='store',
        dest='target_db',
        help='Target database name.')
    parser.add_option('--continuous',
        action='store_true',
        dest='continuous',
        help='Set as a continuous replication.')
    parser.add_option('--cancel',
        action='store_true',
        dest='cancel',
        help='Cancel continuous replication.')
    parser.add_option('--create-dbs',
        action='store_true',
        dest='create_dbs',
        help='Create databases if missing.')

    options, args = parser.parse_args()

    if not options.target_url or (not options.source_url) or (not options.source_db) or (not options.target_db):
        parser.error('--source-url, --target-url, --source-db and --target-db are required.')
        sys.exit(1)

    start = time.time()

    if options.source_url.endswith('/'):
        options.source_url = options.source_url.rstrip('/')
    if options.target_url.endswith('/'):
        options.target_url = options.target_url.rstrip('/')

    if options.create_dbs:
        source_db = Database(options.source_url + '/' + options.source_db, create=True)
        target_db = Database(options.target_url + '/' + options.target_db, create=True)
    else:
        try:
            source_db = Database(options.source_url + '/' + options.source_db)
        except ResourceNotFound:
            db_missing(options.source_url, options.source_db)
        try:
            target_db = Database(options.target_url + '/' + options.target_db)
        except ResourceNotFound:
            db_missing(options.target_url, options.target_db)

    print 'Starting replication...\n',
    sys.stdout.flush()

    continuous = options.continuous or False
    cancel = options.cancel or False
    body = {
        "source": url_quote(options.source_url) + '/' + options.source_db,
        "target": url_quote(options.target_url) + '/' + options.target_db,
        "continuous": continuous,
        "cancel": cancel
    }
    res = CouchdbResource(uri=url_quote(options.source_url))
    try:
        resp = res.request('POST', '/_replicate', payload=body)
        print resp.json_body
        print 'Replication time: %.1f s\n' % (time.time() - start)
    except ResourceNotFound:
        print dict(status=404, msg='Replication task not found.')
    except PreconditionFailed:
        print dict(status=412, msg='Precondition not met.')
    except RequestFailed, e:
        print dict(status=400, msg='Request failed.')
    except Exception, e:
        print e
    
if __name__ == '__main__':
    main()
