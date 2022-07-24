#!/usr/bin/env python3
# Delete old likes
# Copyright Justin Warren <justin@eigenmagic.com>

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
log = logging.getLogger('likesforget')

OLD_SQLDATE_FMT = 'ddd MMM DD hh:mm:ss Z YYYY'

LIKES_DATAFILE = 'data/like.js'

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
        c.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='likes'""")
        result = c.fetchone()
        if result is None:
            self.create_schema()

    def create_schema(self):
        c = self.conn.cursor()
        c.execute("""CREATE TABLE likes
        (id integer UNIQUE, screen_name text, created_at datetime, content_text text, deleted bool)""")
        self.conn.commit()

    def __len__(self):
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM likes")
        result = c.fetchone()[0]
        log.debug("There are %d likes in the cache.", result)
        return result

    def get_deleted_count(self):
        """
        Find out how many likes in the cache are deleted.
        """
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM likes WHERE deleted = ?", (True,))
        result = c.fetchone()[0]
        log.debug("There are %d deleted likes in the cache.", result)
        return result

    def __delitem__(self, tweetid):
        log.debug("Deleting like id %d from cache...", tweetid)

        c = self.conn.cursor()
        c.execute("DELETE FROM likes WHERE id = ?", (tweetid,))
        self.conn.commit()
        return

    def mark_deleted(self, tweetid):
        # Don't actually delete like, just mark it as deleted
        # This is so we can maintain a local archive, but know that
        # the public state of the like is deleted.
        log.debug("Marking like id %s as deleted in cache...", tweetid)
        c = self.conn.cursor()
        c.execute("UPDATE likes SET deleted = ? WHERE id = ?", (True, tweetid,))
        self.conn.commit()
        return

    def save_likes(self, username, likes):
        c = self.conn.cursor()

        valset = []
        for item in likes:
            # This format is when loading from archive
            if hasattr(item, 'fullText'):
                # Sometimes a like is for a tweet that's missing
                if not hasattr(item, 'created_at'):
                    valset.append(
                        (item['tweetId'],
                        username,
                        None,
                        item['fullText'],
                        False,
                        )
                    )
                else:
                    # Use Python to format the date string for SQLite to use
                    created_at = str(arrow.get(item.created_at))
                    valset.append(
                        (item['tweetId'],
                        username,
                        created_at,
                        item['fullText'],
                        False,
                        )
                    )
            # This format returned by the Twitter API
            else:
                # Use Python to format the date string for SQLite to use

                created_at = str(arrow.get(item.created_at))

                valset.append(
                    (item.id,
                    username,
                    created_at,
                    item.text,
                    False,
                    )
                )

        c.executemany("""INSERT OR IGNORE INTO likes
            (id, screen_name, created_at, content_text, deleted)
            VALUES (?, ?, ?, ?, ?)""", valset)
        self.conn.commit()
        log.debug("Saved %d likes into database.", len(valset))

    def get_min_id(self, undeleted=False, ignoreids=None):
        """
        Get the minimum like id in the cache.
        Ignore deleted likes if undeleted is True.
        """
        c = self.conn.cursor()
        if undeleted:
            query = "SELECT min(id) FROM likes WHERE deleted = False"

            if ignoreids is not None:
                log.debug("Ignoring likes: %s", ignoreids)
                # SQLite doesn't support lists for parameters, apparently, so we need to be
                # careful here lest we introduce a SQL-injection
                idlist = ','.join([ '%d' % x for x in ignoreids ])
                log.debug(idlist)

                query += " AND id NOT IN (%s)" % idlist
            c.execute(query)
        else:
            c.execute("SELECT min(id) FROM likes")

        res = c.fetchone()[0]
        log.debug("minimum id is: %d", res)
        return res

    def get_max_id(self, undeleted=False):
        """
        Get the maximum like id in the cache.
        Ignore deleted messages if undeleted is True.
        """
        c = self.conn.cursor()
        if undeleted:
            c.execute("SELECT max(id) FROM likes WHERE deleted = ?", (True,))
        else:
            c.execute("SELECT max(id) FROM likes")
        res = c.fetchone()[0]
        log.debug("max id is: %d", res)
        return res

    def get_destroy_set_keepnum(self, keepnum, deletemax=None):
        """
        Find like destroy list, using keepnum method.

        Returns a set of likes sorted by id, keeping keepnum of the newest ones.
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM likes
        WHERE id NOT IN
        (SELECT id FROM likes ORDER BY id DESC LIMIT ?)
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
        Find likes destroy list, using beforedays method.

        Returns a set of up to deletenum likes sorted by age, from at least beforedays ago.
        """
        # Because SQLite doesn't do dates properly, we will
        # use Python's date handing instead.
        c = self.conn.cursor()
        QUERY = """SELECT * FROM likes
        WHERE
          deleted IS NOT ?
          AND created_at IS NOT NULL
        ORDER BY id ASC
        """
        PARAMS = [True]

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        # Find up to deletenum likes with dates before 'beforedays' days ago
        beforedate = arrow.now().shift(days=-beforedays)
        destroy_set = []
        for i, row in enumerate(result):
            if arrow.get(row['created_at']) < beforedate:
            # if arrow.get(row['created_at'], SQLDATE_FMT) < beforedate:
                destroy_set.append(row)
            # enumerate() starts at 0
            if deletemax and i+1 >= deletemax:
                break
        return destroy_set

    def get_destroy_set_dates(self, date_before, date_after=None, deletemax=None):
        """
        Find tweet destroy list, using dates method.

        Returns a set of up to deletenum likes sorted by age,
        with created_at date of earlier than date_before
        but later than date_after (if set).
        """
        # Because SQLite doesn't do dates properly, we will
        # use Python's date handing instead.
        c = self.conn.cursor()
        QUERY = """SELECT * FROM likes
        WHERE
          deleted IS NOT ?
          AND created_at IS NOT NULL
        ORDER BY id ASC
        """
        PARAMS = [True]

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        # Find up to deletenum likes with dates between
        # date_before and date_after
        destroy_set = []
        for i, row in enumerate(result):
            # tweet_date = arrow.get(row['created_at'], SQLDATE_FMT)
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
        Find like destroy list, using missing date.

        If we can't get the date for a tweet, such as likes we liked
        before the user blocked us, or got banned, or for some other
        reason the tweet is now invisible to us, we can't augment the
        Twitter archive info with the date of the tweet we liked. But
        we might still want to delete these likes.
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM likes
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
        QUERY = """SELECT * FROM likes
        WHERE
            created_at LIKE '%+0000 2%'
        ORDER BY id ASC
        """
        PARAMS = []

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        UPDATE_QUERY = """UPDATE likes
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
    If we already have a cache of likes, load it.

    Passes an empty dictionary if the cache doesn't exist.
    """
    tweetcache = TweetCache(os.path.expanduser(args.tweetcache))
    return tweetcache

def fetch_all_likes(tw, args, tweetcache):
    """Find likes so we can delete them."""
    username = args.userids[0]

    # Get older likes
    tweetcache = get_old_likes(tw, username, args, tweetcache)

    # Get newer likes
    tweetcache = get_new_likes(tw, username, args, tweetcache)

    return tweetcache

def get_new_likes(tw, username, args, tweetcache):
    """
    Get likes that are newer than what's in the cache.
    """
    fetching = True
    log.debug("Fetching new likes...")
    while(fetching):
        # Get latest like id
        # We want to include likes marked as deleted, because they
        # are in the cache and will affect the max id possible.
        # Because we delete old likes, the oldest deleted id will
        # usually (always?) be smaller than the latest tweet id.
        known_max_id = tweetcache.get_max_id(undeleted=False)
        log.debug("Getting likes since %s ...", known_max_id)

        likes = tw.get_favorites(screen_name=username,
                                    count=args.batchsize,
                                    since_id=known_max_id,
                                    include_entities=False,
                                )
        log.debug("Fetched %d likes.", len(likes))
        if likes == []:
            log.debug("No more recent likes to fetch.")
            # Stop fetching
            fetching = False
            break
        else:
            tweetcache.save_likes(username, likes)

        sleeptime = 60 / args.searchlimit
        log.debug("sleeping for %s seconds...", sleeptime)
        time.sleep(sleeptime)

    return tweetcache

def get_old_likes(tw, username, args, tweetcache):
    """
    Get likes that are older than what's in the cache.
    """
    fetching = True
    known_min_id = None
    while(fetching):

        if len(tweetcache) == 0:
            log.debug("Fetching first set of %d likes...", args.batchsize)
            likes = tw.get_favorites(screen_name=username,
                                    count=args.batchsize,
                                    include_entities=False,
            )
        else:
            # Keeping a cache of both deleted and undeleted likes creates issues.
            # Once old likes are deleted, we might never fetch new ones if we
            # only look for likes older than the earliest like in the cache,
            # so we need to exclude deleted likes when looking for old likes.
            log.debug("There are %d likes, %d deleted", len(tweetcache), tweetcache.get_deleted_count())
            # Get earliest like id that hasn't been deleted
            min_id = tweetcache.get_min_id(undeleted=True, ignoreids=args.keeplist)
            if known_min_id == min_id:
                log.debug("Didn't find any new likes. All done.")
                break
            known_min_id = min_id

            log.debug("Fetching %d likes before tweet id: %s ...", args.batchsize, known_min_id - 1)
            likes = tw.get_favorites(screen_name=username,
                                    count=args.batchsize,
                                    max_id=known_min_id - 1,
                                    include_entities=False,
                                    )
        log.debug("Fetched %d likes.", len(likes))
        if likes == []:
            log.debug("No more old likes to fetch.")
            # Stop fetching
            fetching = False
            break
        else:
            tweetcache.save_likes(username, likes)

        sleeptime = 60 / args.searchlimit
        log.debug("sleeping for %s seconds...", sleeptime)
        time.sleep(sleeptime)

    return tweetcache

def get_destroy_set(args):
    """ Find the set of likes to destroy
    """
    # Priority order for the way we find likes to delete
    #   (if more than one is specified) is:
    # 1. Missing created_at mode
    # 2. Date based mode (--date-before and --date-after)
    # 3. Days before based mode
    # 4. Number of likes to keep mode
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

def destroy_likes(tw, args, tweetcache):
    """Destroy likes based on method chosen in args"""

    # Delete likes older than the number we're going to keep
    destroyset = get_destroy_set(args)

    log.debug("Need to destroy %d likes.", len(destroyset))

    for idx, item in enumerate(destroyset):
        log.debug("Destroying like id %s: %s", item['id'], item['content_text'])
        try:

            # Don't delete certain specific likes
            if args.keeplist is not None and item['id'] in args.keeplist:
                log.debug("Not deleting like: %s", item['id'])
                continue

            if not args.dryrun:
                gone_item = tw.destroy_favorite(item['id'])
                tweetcache.mark_deleted(item['id'])
            else:
                # Try fetching the item we would delete
                log.debug("Like not actually deleted.")

        except tweepy.errors.HTTPException as e:
            errors = e.api_codes
            log.debug("errors: %s", errors)
            if len(errors) == 1:
                if errors[0] == 144:
                    log.warning("Tweet with this id doesn't exist. Possibly stale cache entry. Removing.")
                    tweetcache.mark_deleted(item['id'])
                
                elif errors[0] == 179:
                    log.warning("Not authorised to delete like: [%s] %s", item['id'], item['content_text'])
                    log.info("Probably a tweet that got deleted by original author. Stale cache entry. Removing.")
                    tweetcache.mark_deleted(item['id'])

                elif errors[0] == 34:
                    log.warning("Page doesn't exist for: [%s] %s", item['id'], item['content_text'])
                    log.info("Probably a tweet that got deleted by original author. Stale cache entry. Removing.")
                    tweetcache.mark_deleted(item['id'])

                elif errors[0] == 63:
                    log.warning("User you retweeted got suspended. Removing cache entry.")
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
    """Load likes into tweetcache from an archive downloaded from Twitter"""

    # Load likes from the likes JSON file in the archive
    log.info("Importing twitter archive from %s", args.importfile)

    # Open the archive zipfile
    with zipfile.ZipFile(args.importfile) as ark:

        # The datafile provided is a Javascript variable assignment
        # of a JSON datastructure, which is a little frustrating to parse
        with ark.open(LIKES_DATAFILE, 'r') as twdf:
            log.debug("Importing likes from archive...")
            jsdata = twdf.readlines()
            # Edit the first line to strip out the variable assignment
            jsdata[0] = jsdata[0][ jsdata[0].index('['): ]
            likeset = [ x['like'] for x in json.loads(''.join(jsdata)) ]

            # Likes from the archive don't have a created_at attribute,
            # so we need to fetch it from twitter.
            decorate_with_tweetdate(tw, args, tweetcache, likeset)

    return tweetcache

def decorate_with_tweetdate(tw, args, tweetcache, likeset):
    """Take a set of likes and decorate them with the created_at attribute"""
    # The archive Twitter provides doesn't contain the created_at
    # time for the likes that have been liked, and we want it for
    # limiting what we delete, so we will go fetch it for these imported likes.
    log.info("Decorating likes with created time for %d likes...", len(likeset))

    # API limit is 300 calls per 15 minute window, for apps
    # User limit is 900 calls per 15 minute window
    sleeptime = 15 * 60 / 900

    # Fetch in batches of 100, which is API maximum we're allowed
    for likebatch in chunked(likeset, 100):
        log.debug("Fetching data for batch of %d...", len(likebatch))
        likedict = {x['tweetId']: x for x in likebatch}

        tweet_ids = ','.join([x['tweetId'] for x in likebatch])
        result = tw.lookup_statuses(tweet_ids)

        for res in result:
            likedict[res['id_str']]['created_at'] = res['created_at']

        # Augment the tweetcache with this data
        tweetcache.save_likes(args.userids[0], likedict.values())

        # Don't breach API rate-limit
        log.debug("sleeping for %s seconds...", sleeptime)
        time.sleep(sleeptime)

    log.debug("Completed data fetching.")

    return likedict.values()

def augment_args(args):
    """Augment commandline arguments with config file parameters"""
    cp = configparser.ConfigParser()
    cp.read(os.path.expanduser(args.config))
    try:
        keeplist = cp.get('twitter', 'keeplikes')
        keeplist = [int(x) for x in keeplist.split()]
        log.debug('keeplist: %s', keeplist)

        if args.keeplist is not None:
            args.keeplist.extend(keeplist)
        else:
            args.keeplist = keeplist
        log.debug('args: %s', args.keeplist)

    except configparser.NoOptionError:
        log.debug("No such option.")
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

    ap = argparse.ArgumentParser(description="Delete old likes",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument('userids', nargs='+', help="User id")

    ap.add_argument('-c', '--config', default='~/.twitrc', help="Config file")
    ap.add_argument('-b', '--batchsize', type=int, default=200, help="Fetch this many likes per API call (max is Twitter API max, currently 200)")
    ap.add_argument('-d', '--deletemax', type=int, help="Only delete this many likes.")
    ap.add_argument('-k', '--keeplist', type=int, nargs='+', help="Don't delete likes in this list.")

    ap.add_argument('-B', '--date-before', help="Delete likes before this date.", type=valid_date)
    ap.add_argument('-A', '--date-after', help="Delete likes after this date.", type=valid_date)
    ap.add_argument('-K', '--keepnum', type=int, default=500, help="How many likes to keep.")
    ap.add_argument('--delete-nodate', action='store_true', help="Delete likes with NULL created_at datetime.")

    ap.add_argument('--beforedays', type=int, help="Delete likes from before this many days ago.")

    ap.add_argument('-i', '--importfile', default=None, help="Import likes from Twitter archive.")

    ap.add_argument('--tweetcache', default='~/.tweetcache.db', help="File to store cache of tweet/date IDs")

    ap.add_argument('--nodelete', action='store_true', help="Skip the delete stage.")
    ap.add_argument('--nofetch', action='store_true', help="Skip the fetch stage.")

    ap.add_argument('--dryrun', action='store_true', help="Don't actually delete likes, but do populate cache.")
    ap.add_argument('--loglevel', choices=['debug', 'info', 'warning', 'error', 'critical'], help="Set log output level.")

    ap.add_argument('--searchlimit', type=int, default=5, help="Max number of searches per minute.")
    ap.add_argument('--deletelimit', type=int, default=60, help="Max number of deletes per minute.")

    ap.add_argument('--migrate', action='store_true', dest='migrate_datetimes', help="Migrate datetimes in tweetcache to ISO-8601 format.")

    args = ap.parse_args()

    if args.loglevel is not None:
        levelname = args.loglevel.upper()
        log.setLevel(getattr(logging, levelname))

    # Safety feature: If you specific --after-date, force to also
    # provide --before-date to prevent accidental deletion of all likes.
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
        # Import likes from a Twitter archive file you asked for
        # This reads in the data Twitter sends you when you ask
        # for an archive of your likes, and stores it in the local tweetcache.
        # This is a handy way to bootstrap your archive rather than using the
        # slow and painful feed traversal mechanism.
        tweetcache = import_twitter_archive(tw, args, tweetcache)

    if not args.nofetch:
        tweetcache = fetch_all_likes(tw, args, tweetcache)

    if tweetcache is None:
        raise ValueError("Unable to load any likes for this user.")

    if not args.nodelete:
        tweetcache = destroy_likes(tw, args, tweetcache)

    log.debug("Done.")
