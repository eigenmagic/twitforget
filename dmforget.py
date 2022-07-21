#!/usr/bin/env python3
# Delete old dms
# Copyright Justin Warren <justin@eigenmagic.com>

import pdb
import sys
import os.path
import argparse
import configparser
from more_itertools import chunked
import arrow

import zipfile
import json

import tweepy
import time

import sqlite3

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('dmsforget')

import pprint

OLD_SQLDATE_FMT = 'ddd MMM DD hh:mm:ss Z YYYY'

DMS_DATAFILE = 'data/direct-messages.js'

class TweetCache(object):
    """
    An abstracted class for storing a cache of tweet information.

    This version is a shim in front of a SQLite3 database.
    """
    def __init__(self, filename):
        log.debug("Opening database file: %s", filename)
        self.conn = sqlite3.connect(filename)
        self.conn.row_factory = sqlite3.Row

        c = self.conn.cursor()
        # Does the schema exist? If not, create it.
        c.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='dms'""")
        result = c.fetchone()
        if result is None:
            self.create_schema()

    def create_schema(self):
        c = self.conn.cursor()
        c.execute("""CREATE TABLE dms
        (id integer UNIQUE,
            screen_name text,
            created_at datetime,
            conversation_id text,
            sender_id integer,
            recipient_id integer,
            content_text text,
            deleted bool)""")
        self.conn.commit()

    def __len__(self):
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM dms")
        result = c.fetchone()[0]
        log.debug("There are %d dms in the cache.", result)
        return result

    def get_deleted_count(self):
        """
        Find out how many dms in the cache are deleted.
        """
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM dms WHERE deleted = ?", (True,))
        result = c.fetchone()[0]
        log.debug("There are %d deleted dms in the cache.", result)
        return result

    def __delitem__(self, tweetid):
        log.debug("Deleting dm id %d from cache...", tweetid)

        c = self.conn.cursor()
        c.execute("DELETE FROM dms WHERE id = ?", (tweetid,))
        self.conn.commit()
        return

    def mark_deleted(self, tweetid):
        # Don't actually delete dm, just mark it as deleted
        # This is so we can maintain a local archive, but know that
        # the public state of the dm is deleted.
        log.debug("Marking dm id %id as deleted in cache...", tweetid)
        c = self.conn.cursor()
        c.execute("UPDATE dms SET deleted = ? WHERE id = ?", (True, tweetid,))
        self.conn.commit()
        return

    def save_dms(self, username, dms):
        c = self.conn.cursor()

        valset = []
        for item in dms:
            # created_at = str(arrow.get(item.created_at))

            valset.append(
                (item['id'],
                username,
                item['createdAt'],
                item['senderId'],
                item['recipientId'],
                item['text'],
                False,
                )
            )
        c.executemany("""INSERT OR IGNORE INTO dms
            (id, screen_name, created_at, sender_id, recipient_id, content_text, deleted)
            VALUES (?, ?, ?, ?, ?, ?, ?)""", valset)
        self.conn.commit()
        log.debug("Saved %d dms into database.", len(valset))

    def get_min_id(self, undeleted=False, ignoreids=None):
        """
        Get the minimum dm id in the cache.
        Ignore deleted dms if undeleted is True.
        """
        c = self.conn.cursor()
        if undeleted:
            log.debug("Ignoring dms: %s", ignoreids)
            # SQLite doesn't support lists for parameters, apparently, so we need to be
            # careful here lest we introduce a SQL-injection
            idlist = ','.join([ '%d' % x for x in ignoreids ])
            log.debug(idlist)

            c.execute("SELECT min(id) FROM dms WHERE deleted = ? AND id NOT IN (%s)" % idlist, (False,) )
        else:
            c.execute("SELECT min(id) FROM dms")
        res = c.fetchone()[0]
        log.debug("minimum id is: %d", res)
        return res

    def get_max_id(self, undeleted=False):
        """
        Get the maximum dm id in the cache.
        Ignore deleted messages if undeleted is True.
        """
        c = self.conn.cursor()
        if undeleted:
            c.execute("SELECT max(id) FROM dms WHERE deleted = ?", (True,))
        else:
            c.execute("SELECT max(id) FROM dms")
        res = c.fetchone()[0]
        log.debug("max id is: %d", res)
        return res

    def get_destroy_set_keepnum(self, keepnum, deletemax=None):
        """
        Find dm destroy list, using keepnum method.

        Returns a set of dms sorted by id, keeping keepnum of the newest ones.
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM dms
        WHERE id NOT IN
        (SELECT id FROM dms ORDER BY id DESC LIMIT ?)
        AND deleted IS NOT ?
        ORDER BY id ASC
        """
        PARAMS = [keepnum, True]
        if deletemax is not None:
            QUERY += " LIMIT ?"
            PARAMS.append(deletemax)
        c.execute(QUERY, PARAMS)
        result = c.fetchall()
        return result

    def get_destroy_set_beforedays(self, beforedays, deletemax=None):
        """
        Find dms destroy list, using beforedays method.

        Returns a set of up to deletenum dms sorted by age, from at least beforedays ago.
        """
        # Because SQLite doesn't do dates properly, we will
        # use Python's date handing instead.
        c = self.conn.cursor()
        QUERY = """SELECT * FROM dms
        WHERE
          deleted IS NOT ?
          AND created_at IS NOT NULL
        ORDER BY id ASC
        """
        PARAMS = [True]

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        # Find up to deletenum dms with dates before 'beforedays' days ago
        beforedate = arrow.now().shift(days=-beforedays)
        destroy_set = []
        for i, row in enumerate(result):
            try:
                tweet_date = arrow.get(row['created_at'], OLD_SQLDATE_FMT)
            except arrow.parser.ParserError:
                tweet_date = arrow.get(row['created_at'])

            if tweet_date < beforedate:
                destroy_set.append(row)
            # enumerate() starts at 0
            if deletemax and i+1 >= deletemax:
                break
        return destroy_set

    def get_destroy_set_dates(self, date_before, date_after=None, deletemax=None):
        """
        Find tweet destroy list, using dates method.

        Returns a set of up to deletenum dms sorted by age,
        with created_at date of earlier than date_before
        but later than date_after (if set).
        """
        # Because SQLite doesn't do dates properly, we will
        # use Python's date handing instead.
        c = self.conn.cursor()
        QUERY = """SELECT * FROM dms
        WHERE
          deleted IS NOT ?
          AND created_at IS NOT NULL
        ORDER BY id ASC
        """
        PARAMS = [True]

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        # Find up to deletenum dms with dates between
        # date_before and date_after
        destroy_set = []
        for i, row in enumerate(result):
            try:
                tweet_date = arrow.get(row['created_at'], OLD_SQLDATE_FMT)
            except arrow.parser.ParserError:
                tweet_date = arrow.get(row['created_at'])

            if tweet_date < date_before:
                # Also check date_after if it's set
                if date_after:
                    if tweet_date > date_after:
                        destroy_set.append(row)
                else:
                    destroy_set.append(row)

            # enumerate() starts at 0
            if deletemax and i+1 >= deletemax:
                break
        return destroy_set

    def get_destroy_nodate(self, keepnum, deletemax=None):
        """
        Find dm destroy list, using missing date.

        If we can't get the date for a tweet, such as dms we dmd
        before the user blocked us, or got banned, or for some other
        reason the tweet is now invisible to us, we can't augment the
        Twitter archive info with the date of the tweet we dmd. But
        we might still want to delete these dms.
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM dms
        WHERE created_at IS NULL
        AND deleted IS NOT ?
        ORDER BY id ASC
        """
        PARAMS = [True,]
        if deletemax is not None:
            QUERY += " LIMIT ?"
            PARAMS.append(deletemax)
        c.execute(QUERY, PARAMS)
        result = c.fetchall()
        return result

    def migrate_datetimes(self):
        """ Migrate all datetimes from old format to ISO-8601 format
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM dms
        WHERE
            created_at LIKE '%+0000 2%'
        ORDER BY id ASC
        """
        PARAMS = []

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        UPDATE_QUERY = """UPDATE dms
        SET created_at = ?
        WHERE id = ?
        """

        for row in result:
            orig_date = arrow.get(row['created_at'], OLD_SQLDATE_FMT)

            log.debug("Original date is: %s", orig_date)
            log.debug("Updating status %s", row['id'])
            c.execute(UPDATE_QUERY, (str(orig_date), row['id']))
        self.conn.commit()    

def load_tweetcache(args):
    """
    If we already have a cache of dms, load it.

    Passes an empty dictionary if the cache doesn't exist.
    """
    tweetcache = TweetCache(os.path.expanduser(args.tweetcache))
    return tweetcache

def fetch_all_dms(tw, args, tweetcache):
    """Find dms so we can delete them."""
    username = args.userids[0]

    # Get recent dms
    tweetcache = get_recent_dms(tw, username, args, tweetcache)

    return tweetcache

def get_recent_dms(tw, username, args, tweetcache):
    """
    Get dms that are newer than what's in the cache.
    """
    fetching = True
    log.debug("Fetching new dms...")
    next_cursor = None
    while(fetching):
        # Twitter only returns DMs from the last 30 days
        # sorted in reverse chronological order
        # Fetch with a cursor, if we need to
        if next_cursor is not None:
            log.debug("Fetching with cursor: %s", next_cursor)
            result = tw.get_direct_messages(
                count=args.batchsize,
                cursor=next_cursor
            )
        else:
            # First time fetch format
            result = tw.get_direct_messages(count=args.batchsize)

        if 'next_cursor' not in result:
            log.debug("No more recent dms to fetch.")
            # Stop fetching
            fetching = False
        else:
            next_cursor = result['next_cursor']
            log.debug("Found cursor for next time: %s", next_cursor)
            
        # Parse the DM event format
        dms = []
        for dm in result:
            if dm.type in [ u'message_create',]:
                # Returned created_timestamp is a unix seconds since epoch
                # format or something, so we need to convert it
                dm = {
                    'id': dm.id,
                    'createdAt': str(arrow.get(int(dm.created_timestamp) / 1000.0)),
                    'senderId': dm.message_create['sender_id'],
                    'recipientId': dm.message_create['target']['recipient_id'],
                    'text': dm.message_create['message_data']['text'],
                }
                dms.append(dm)

        log.debug("Fetched %d dms.", len(dms))

        tweetcache.save_dms(username, dms)

        if (fetching):
            # API limit is 15 per 15 minute window
            sleeptime = 15 * 60 / 15
            log.debug("sleeping for %s seconds...", sleeptime)
            time.sleep(sleeptime)

    return tweetcache

def get_destroy_set(args):
    """ Find the set of dms to destroy
    """
    # Priority order for the way we find dms to delete
    #   (if more than one is specified) is:
    # 1. Missing created_at mode
    # 2. Date based mode (--date-before and --date-after)
    # 3. Days before based mode
    # 4. Number of dms to keep mode
    if args.delete_nodate:
        log.debug("Using NULL created_at mode.")
        destroyset = tweetcache.get_destroy_nodate(args.deletemax)

    elif args.date_before is not None:
        log.debug("Using date based mode.")
        destroyset = tweetcache.get_destroy_set_dates(args.date_before, args.date_after, args.deletemax)

    elif args.beforedays is not None:
        log.debug("Using days before mode.")
        destroyset = tweetcache.get_destroy_set_beforedays(args.beforedays, args.deletemax)

    else:
        log.debug("Using number to keep mode. Keeping %d.", args.keepnum)
        destroyset = tweetcache.get_destroy_set_keepnum(args.keepnum, args.deletemax)

    return destroyset

def destroy_dms(tw, args, tweetcache):
    """Destroy dms based on method chosen in args"""

    # Delete dms older than the number we're going to keep
    destroyset = get_destroy_set(args)

    log.debug("Need to destroy %d dms.", len(destroyset))

    # What is my userid?
    user_info = tw.get_user(screen_name=args.userids[0])
    userid = user_info.id

    for idx, item in enumerate(destroyset):
        log.debug("Destroying %s dm [%s to %s] id %s: %s", userid, item['sender_id'], item['recipient_id'], item['id'], item['content_text'])
        try:
            # Don't delete certain specific dms
            if args.keeplist is not None and item['id'] in args.keeplist:
                log.debug("Not deleting DM: %d", item['id'])
                continue

            if item['sender_id'] != userid:
                log.debug("Sender isn't me, so I can't actually delete this.")
                if not args.dryrun:
                    tweetcache.mark_deleted(item['id'])
                continue

            if not args.dryrun:
                tw.delete_direct_message(item['id'])
                tweetcache.mark_deleted(item['id'])
            else:
                log.debug("DM not actually deleted.")

        except tweepy.errors.HTTPException as e:
            log.debug("Response: %s", e.response)

            errors = e.api_codes
            log.debug("errors: %s", errors)
            if len(errors) == 1:
                if errors[0] == 144:
                    log.warn("DM with this id doesn't exist. Possibly stale cache entry. Removing.")
                    tweetcache.mark_deleted(item['id'])
                
                elif errors[0] == 179:
                    log.warn("Not authorised to delete DM: [%s] %s", item['id'], item['content_text'])
                    log.info("Probably a tweet that got deleted by original author. Stale cache entry. Removing.")
                    tweetcache.mark_deleted(item['id'])

                elif errors[0] == 34:
                    log.warn("Page doesn't exist for: [%s] %s", item['id'], item['content_text'])
                    log.info("Probably a DM that got deleted by original author. Stale cache entry. Removing.")
                    tweetcache.mark_deleted(item['id'])

                elif errors[0] == 63:
                    log.warn("User you retweeted got suspended. Removing cache entry.")
                    tweetcache.mark_deleted(item['id'])

                else:
                    log.critical("Unhandled response from Twitter for: [%s] %s", item['id'], item['content_text'])
                    raise
            else:
                log.critical("Unhandled response from Twitter for: [%s] %s", item['id'], item['content_text'])
                raise

        log.info("Tweet %d of %d destroyed.", idx+1, len(destroyset))
        del_sleeptime = 60 / args.deletelimit
        log.debug("sleeping for %s seconds...", del_sleeptime)
        time.sleep(del_sleeptime)

    return tweetcache

def import_twitter_archive(tw, args, tweetcache):
    """Load dms into tweetcache from an archive downloaded from Twitter"""

    # Load dms from the dms JSON file in the archive
    log.info("Importing twitter archive from %s", args.importfile)

    # Open the archive zipfile
    with zipfile.ZipFile(args.importfile) as ark:

        # The datafile provided is a Javascript variable assignment
        # of a JSON datastructure, which is a little frustrating to parse
        with ark.open(DMS_DATAFILE, 'r') as twdf:
            log.debug("Importing dms from archive...")
            jsdata = twdf.readlines()
            # Edit the first line to strip out the variable assignment
            jsdata[0] = jsdata[0][ jsdata[0].index('['): ]
            # The data is a list of 'conversations', each containing
            # a list of messages, which are the DMs we need
            for conversation in json.loads(''.join(jsdata)):
                conv = conversation['dmConversation']
                conv_id = conv['conversationId']
                dmset = [ x['messageCreate'] for x in conv['messages'] ]
                #pprint.pprint(dmset)

                # Decorate every DM item with the conversation ID
                # This essentially de-normalises the data ready for storing
                # it in a single table in the tweetcache
                map(lambda x: x.update({'conversationId': conv_id}), dmset)

                # Augment the tweetcache with this data
                tweetcache.save_dms(args.userids[0], dmset)

    return tweetcache

def decorate_with_tweetdate(tw, args, tweetcache, dmset):
    """Take a set of dms and decorate them with the created_at attribute"""
    # The archive Twitter provides doesn't contain the created_at
    # time for the dms that have been dmd, and we want it for
    # limiting what we delete, so we will go fetch it for these imported dms.
    log.info("Decorating dms with created time for %d dms...", len(dmset))

    # API limit is 300 calls per 15 minute window, for apps
    # User limit is 900 calls per 15 minute window
    sleeptime = 15 * 60 / 900

    # Fetch in batches of 100, which is API maximum we're allowed
    for dmbatch in chunked(dmset, 100):
        log.debug("Fetching data for batch of %d...", len(dmbatch))
        dmdict = {x['tweetId']: x for x in dmbatch}

        tweet_ids = ','.join([x['tweetId'] for x in dmbatch])
        result = tw.statuses.lookup(_id=tweet_ids)

        for res in result:
            dmdict[res['id_str']]['created_at'] = res['created_at']

        # Augment the tweetcache with this data
        tweetcache.save_dms(args.userids[0], dmdict.values())

        # Don't breach API rate-limit
        log.debug("sleeping for %s seconds...", sleeptime)
        time.sleep(sleeptime)

    log.debug("Completed data fetching.")

    return dmdict.values()

def augment_args(args):
    """Augment commandline arguments with config file parameters"""
    cp = configparser.ConfigParser()
    cp.read(os.path.expanduser(args.config))
    try:
        keeplist = cp.get('twitter', 'keepdms')
        keeplist = [int(x) for x in keeplist.split()]
        log.debug('keeplist: %s', keeplist)

        if args.keeplist is not None:
            args.keeplist.extend(keeplist)
        else:
            args.keeplist = keeplist
        log.debug('args: %s', args.keeplist)

    except configparser.NoOptionError:
        log.debug("No such option '[twitter] keepdms'")
        pass

    return args

def authenticate(args):
    """
    Authenticate with Twitter and return an authenticated
    Twitter() object to use for API calls
    """
    # import the config file
    cp = configparser.ConfigParser()
    cp.read(os.path.expanduser(args.config))

    access_token = cp.get('twitter', 'access_token')
    access_token_secret = cp.get('twitter', 'access_token_secret')
    consumer_secret = cp.get('twitter', 'consumer_secret')
    consumer_key = cp.get('twitter', 'consumer_key')

    auth = tweepy.OAuth1UserHandler(
        consumer_key, consumer_secret,
        access_token, access_token_secret
        )

    tw = tweepy.API(auth)

    return tw

def valid_date(s):
    """Parse a string to see if it's a valid date or not"""
    try:
        return arrow.get(s)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

if __name__ == '__main__':

    ap = argparse.ArgumentParser(description="Delete old dms",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument('userids', nargs='+', help="User id")

    ap.add_argument('-c', '--config', default='~/.twitrc', help="Config file")
    ap.add_argument('-b', '--batchsize', type=int, default=40, help="Fetch this many dms per API call (max is Twitter API max, currently 200)")
    ap.add_argument('-d', '--deletemax', type=int, help="Only delete this many dms.")
    ap.add_argument('-k', '--keeplist', type=int, nargs='+', help="Don't delete dms in this list.")

    ap.add_argument('-B', '--date-before', help="Delete dms before this date.", type=valid_date)
    ap.add_argument('-A', '--date-after', help="Delete dms after this date.", type=valid_date)
    ap.add_argument('-K', '--keepnum', type=int, default=500, help="How many dms to keep.")
    ap.add_argument('--delete-nodate', action='store_true', help="Delete dms with NULL created_at datetime.")

    ap.add_argument('--beforedays', type=int, help="Delete dms from before this many days ago.")

    ap.add_argument('-i', '--importfile', default=None, help="Import dms from Twitter archive.")

    ap.add_argument('--tweetcache', default='~/.tweetcache.db', help="File to store cache of tweet/date IDs")

    ap.add_argument('--nodelete', action='store_true', help="Skip the delete stage.")
    ap.add_argument('--nofetch', action='store_true', help="Skip the fetch stage.")

    ap.add_argument('--dryrun', action='store_true', help="Don't actually delete dms, but do populate cache.")
    ap.add_argument('--loglevel', choices=['debug', 'info', 'warning', 'error', 'critical'], help="Set log output level.")

    ap.add_argument('--searchlimit', type=int, default=5, help="Max number of searches per minute.")
    ap.add_argument('--deletelimit', type=int, default=60, help="Max number of deletes per minute.")

    ap.add_argument('--migrate', action='store_true', dest='migrate_datetimes', help="Migrate datetimes in tweetcache to ISO-8601 format.")

    args = ap.parse_args()

    if args.loglevel is not None:
        levelname = args.loglevel.upper()
        log.setLevel(getattr(logging, levelname))

    # Safety feature: If you specific --after-date, force to also
    # provide --before-date to prevent accidental deletion of all dms.
    if args.date_after is not None:
        if args.date_before is None:
            ap.error("Safety feature: Need to provide --date-before as well as --date-after.")

    args = augment_args(args)

    tweetcache = load_tweetcache(args)
    log.debug("tweetcache loaded.")

    if args.migrate_datetimes:
        log.info('Migrating old dates...')
        tweetcache.migrate_datetimes()

    tw = authenticate(args)

    if args.importfile:
        # Import dms from a Twitter archive file you asked for
        # This reads in the data Twitter sends you when you ask
        # for an archive of your dms, and stores it in the local tweetcache.
        # This is a handy way to bootstrap your archive rather than using the
        # slow and painful feed traversal mechanism.
        tweetcache = import_twitter_archive(tw, args, tweetcache)

    if not args.nofetch:
        tweetcache = fetch_all_dms(tw, args, tweetcache)

    if tweetcache is None:
        raise ValueError("Unable to load any dms for this user.")

    if not args.nodelete:
        tweetcache = destroy_dms(tw, args, tweetcache)

    log.debug("Done.")
