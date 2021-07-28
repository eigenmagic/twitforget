#!/usr/bin/env python
# Delete old tweets
# Copyright Justin Warren <justin@eigenmagic.com>

import sys
import os.path
import argparse
import ConfigParser
import arrow

#import csv
import zipfile
import json

import twitter
import time

import sqlite3

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('twitforget')

SQLDATE_FMT = 'ddd MMM DD hh:mm:ss Z YYYY'

TWEET_DATAFILE = 'data/tweet.js'

class NoMoreTweets(Exception):
    """
    Raised if there are no more tweets to fetch.
    """

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
        c.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='tweets'""")
        result = c.fetchone()
        if result is None:
            self.create_schema()

    def create_schema(self):
        c = self.conn.cursor()
        c.execute("""CREATE TABLE tweets
        (id integer UNIQUE, screen_name text, created_at datetime, content_text text, deleted bool)""")
        self.conn.commit()

    def __len__(self):
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM tweets")
        result = c.fetchone()[0]
        log.debug("There are %d tweets in the cache.", result)
        return result

    def get_deleted_count(self):
        """
        Find out how many tweets in the cache are deleted.
        """
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM tweets WHERE deleted = ?", (True,))
        result = c.fetchone()[0]
        log.debug("There are %d deleted tweets in the cache.", result)
        return result

    def __delitem__(self, tweetid):
        log.debug("Deleting tweet id %d from cache...", tweetid)

        c = self.conn.cursor()
        c.execute("DELETE FROM tweets WHERE id = ?", (tweetid,))
        self.conn.commit()
        return

    def mark_deleted(self, tweetid):
        # Don't actually delete tweet, just mark it as deleted
        # This is so we can maintain a local archive, but know that
        # the public state of the tweet is deleted.
        log.debug("Marking tweet id %id as deleted in cache...", tweetid)
        c = self.conn.cursor()
        c.execute("UPDATE tweets SET deleted = ? WHERE id = ?", (True, tweetid,))
        self.conn.commit()
        return

    def save_tweets(self, username, tweets):
        c = self.conn.cursor()

        valset = []
        for twt in tweets:
            if 'full_text' in twt:
                valset.append(
                    (twt['id'],
                    username,
                    twt['created_at'],
                    twt['full_text'],
                    False,
                    )
                )
            else:
                valset.append(
                    (twt['id'],
                    username,
                    twt['created_at'],
                    twt['text'],
                    False,
                    )
                )

        c.executemany("""INSERT OR IGNORE INTO tweets
            (id, screen_name, created_at, content_text, deleted)
            VALUES (?, ?, ?, ?, ?)""", valset)
        self.conn.commit()
        log.debug("Saved %d tweets into database.", len(valset))

    def get_min_id(self, undeleted=False, ignoreids=None):
        """
        Get the minimum tweet id in the cache.
        Ignore deleted messages if undeleted is True.
        """
        c = self.conn.cursor()
        if undeleted:
            log.debug("Ignoring tweets: %s", ignoreids)
            # SQLite doesn't support lists for parameters, apparently, so we need to be
            # careful here lest we introduce a SQL-injection
            idlist = ','.join([ '%d' % x for x in ignoreids ])
            log.debug(idlist)

            c.execute("SELECT min(id) FROM tweets WHERE deleted = ? AND id NOT IN (%s)" % idlist, (False,) )
        else:
            c.execute("SELECT min(id) FROM tweets")
        res = c.fetchone()[0]
        log.debug("minimum id is: %d", res)
        return res

    def get_max_id(self, undeleted=False):
        """
        Get the maximum tweet id in the cache.
        Ignore deleted messages if undeleted is True.
        """
        c = self.conn.cursor()
        if undeleted:
            c.execute("SELECT max(id) FROM tweets WHERE deleted = ?", (True,))
        else:
            c.execute("SELECT max(id) FROM tweets")
        res = c.fetchone()[0]
        log.debug("max id is: %d", res)
        return res

    def get_destroy_set_keepnum(self, keepnum, deletemax=None):
        """
        Find tweet destroy list, using keepnum method.

        Returns a set of tweets sorted by id, keeping keepnum of the newest ones.
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM tweets
        WHERE id NOT IN
        (SELECT id FROM tweets ORDER BY id DESC LIMIT ?)
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
        Find tweet destroy list, using beforedays method.

        Returns a set of up to deletenum tweets sorted by age, from at least beforedays ago.
        """
        # Because SQLite doesn't do dates properly, we will
        # use Python's date handing instead.
        c = self.conn.cursor()
        QUERY = """SELECT * FROM tweets
        WHERE
          deleted IS NOT ?
        ORDER BY id ASC
        """
        PARAMS = [True]

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        # Find up to deletenum tweets with dates before 'beforedays' days ago
        beforedate = arrow.now().shift(days=-beforedays)
        destroy_set = []
        for i, row in enumerate(result):
            if arrow.get(row['created_at'], SQLDATE_FMT) < beforedate:
                destroy_set.append(row)
            # enumerate() starts at 0
            if deletemax and i+1 >= deletemax:
                break
        return destroy_set

    def get_destroy_set_dates(self, date_before, date_after=None, deletemax=None):
        """
        Find tweet destroy list, using dates method.

        Returns a set of up to deletenum tweets sorted by age,
        with created_at date of earlier than date_before
        but later than date_after (if set).
        """
        # Because SQLite doesn't do dates properly, we will
        # use Python's date handing instead.
        c = self.conn.cursor()
        QUERY = """SELECT * FROM tweets
        WHERE
          deleted IS NOT ?
        ORDER BY id ASC
        """
        PARAMS = [True]

        c.execute(QUERY, PARAMS)
        result = c.fetchall()

        # Find up to deletenum tweets with dates between
        # date_before and date_after
        destroy_set = []
        for i, row in enumerate(result):
            tweet_date = arrow.get(row['created_at'], SQLDATE_FMT)

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

def load_tweetcache(args):
    """
    If we already have a cache of tweets, load it.

    Passes an empty dictionary if the cache doesn't exist.
    """
    tweetcache = TweetCache(os.path.expanduser(args.tweetcache))
    return tweetcache

def fetch_all_tweets(tw, args, tweetcache):
    """
    Find tweet IDs so we can delete them.

    Twitter's search API makes it really hard to find old tweets.
    So instead, we first search for the oldest tweet we can find,
    and then slowly iterate backwards to find the ID of the last
    tweet in the range that we want to delete, so we stay within
    the rate limits.

    We save a cache of tweet ID and date so we can restart later
    without having to trawl the entire tweet stream again.

    Once we have the list of all the IDs within the date range we
    need, we can delete them one by one, starting with the oldest
    one.
    """
    username = args.userids[0]

    # Get older tweets
    tweetcache = get_old_tweets(tw, username, args, tweetcache)

    # Get newer tweets
    tweetcache = get_new_tweets(tw, username, args, tweetcache)

    return tweetcache

def get_new_tweets(tw, username, args, tweetcache):
    """
    Get tweets that are newer than what's in the cache.
    """
    fetching = True
    log.debug("Fetching new tweets...")
    while(fetching):
        # Get latest tweet id
        # We want to include tweets marked as deleted, because they
        # are in the cache and will affect the max id possible.
        # Because we delete old tweets, the oldest deleted id will
        # usually (always?) be smaller than the latest tweet id.
        known_max_id = tweetcache.get_max_id(undeleted=False)
        log.debug("Getting tweets since %s ...", known_max_id)

        tweets = tw.statuses.user_timeline(screen_name=username,
                                           count=args.batchsize,
                                           since_id=known_max_id,
                                           trim_user=True,
                                           #exclude_replies=True,
                                       )
        log.debug("Fetched %d tweets.", len(tweets))
        if tweets == []:
            log.debug("No more recent tweets to fetch.")
            # Stop fetching
            fetching = False
            break
        else:
            tweetcache.save_tweets(username, tweets)

        sleeptime = 60 / args.searchlimit
        log.debug("sleeping for %s seconds...", sleeptime)
        time.sleep(sleeptime)

    return tweetcache

def get_old_tweets(tw, username, args, tweetcache):
    """
    Get tweets that are older than what's in the cache.
    """
    fetching = True
    known_min_id = None
    while(fetching):

        if len(tweetcache) == 0:
            log.debug("Fetching first set of %d tweets...", args.batchsize)
            tweets = tw.statuses.user_timeline(screen_name=username,
                                               count=args.batchsize,
                                               trim_user=True,
                                               #exclude_replies=True,
            )
        else:
            # Keeping a cache of both deleted and undeleted tweets creates issues.
            # Once old tweets are deleted, we might never fetch new ones if we
            # only look for tweets older than the earliest tweet in the cache,
            # so we need to exclude deleted tweets when looking for old tweets.
            log.debug("There are %d tweets, %d deleted", len(tweetcache), tweetcache.get_deleted_count())
            # Get earliest tweet id that hasn't been deleted
            min_id = tweetcache.get_min_id(undeleted=True, ignoreids=args.keeplist)
            if known_min_id == min_id:
                log.debug("Didn't find any new tweets. All done.")
                break
            known_min_id = min_id

            log.debug("Fetching %d tweets before tweet id: %s ...", args.batchsize, known_min_id - 1)
            tweets = tw.statuses.user_timeline(screen_name=username,
                                           count=args.batchsize,
                                           max_id=known_min_id - 1,
                                           trim_user=True,
                                           #exclude_replies=True,
                                       )
        
        # Save the tweets we've found so far
        log.debug("Fetched %d tweets.", len(tweets))
        if tweets == []:
            log.debug("No more old tweets to fetch.")
            # Stop fetching
            fetching = False
            break
        else:
            tweetcache.save_tweets(username, tweets)

        sleeptime = 60 / args.searchlimit
        log.debug("sleeping for %s seconds...", sleeptime)
        time.sleep(sleeptime)

    return tweetcache

def get_destroy_set(args):
    """ Find the set of tweets to destroy
    """
    # Priority order for the way we find tweets to delete is:
    # 1. Date based mode (--date-before and --date-after)
    # 2. Days before based mode
    # 3. Number of tweets to keep mode
    if args.date_before is not None:
        log.debug("Using date based mode.")
        destroy_tweetset = tweetcache.get_destroy_set_dates(args.date_before, args.date_after, args.deletemax)

    elif args.beforedays is not None:
        log.debug("Using days before mode.")
        destroy_tweetset = tweetcache.get_destroy_set_beforedays(args.beforedays, args.deletemax)

    else:
        log.debug("Using number to keep mode. Keeping %d.", args.keepnum)
        destroy_tweetset = tweetcache.get_destroy_set_keepnum(args.keepnum, args.deletemax)

    return destroy_tweetset

def destroy_tweets(tw, args, tweetcache):
    """Destroy tweets based on method chosen in args"""
    #tweetset = sort_tweets(tweetcache)

    # Delete tweets older than the number we're going to keep
    destroy_tweetset = get_destroy_set(args)

    log.debug("Need to destroy %d tweets.", len(destroy_tweetset))

    for idx, twt in enumerate(destroy_tweetset):
        log.debug("Destroying tweet id %s [%s]: %s", twt['id'], twt['created_at'], twt['content_text'])
        try:

            # Don't delete certain specific tweets
            if twt['id'] in args.keeplist:
                log.debug("Not deleting tweet: %d", twt['id'])
                continue

            if not args.dryrun:
                gone_twt = tw.statuses.destroy(id=twt['id'])
                tweetcache.mark_deleted(twt['id'])
                log.debug("Gone tweet %s: %s", gone_twt['id'], gone_twt['text'])
            else:
                log.debug("Tweet not actually deleted.")

        except twitter.api.TwitterHTTPError, e:
            log.debug("Response: %s", e.response_data)
            errors = e.response_data['errors']
            log.debug("errors: %s", errors)
            if len(errors) == 1:
                if errors[0]['code'] == 144:
                    log.warn("Tweet with this id doesn't exist. Possibly stale cache entry. Removing.")
                    tweetcache.mark_deleted(twt['id'])
                
                elif errors[0]['code'] == 179:
                    log.warn("Not authorised to delete tweet: [%s] %s", twt['id'], twt['content_text'])
                    log.info("Probably a RT that got deleted by original author. Stale cache entry. Removing.")
                    tweetcache.mark_deleted(twt['id'])

                elif errors[0]['code'] == 34:
                    log.warn("Page doesn't exist for: [%s] %s", twt['id'], twt['content_text'])
                    log.info("Probably a RT that got deleted by original author. Stale cache entry. Removing.")
                    tweetcache.mark_deleted(twt['id'])

                elif errors[0]['code'] == 63:
                    log.warn("User you retweeted got suspended. Removing cache entry.")
                    tweetcache.mark_deleted(twt['id'])

                else:
                    log.critical("Unhandled response from Twitter for: [%s] %s", twt['id'], twt['content_text'])
                    raise
            else:
                log.critical("Unhandled response from Twitter for: [%s] %s", twt['id'], twt['content_text'])
                raise

        log.info("Tweet %d of %d destroyed.", idx+1, len(destroy_tweetset))
        del_sleeptime = 60 / args.deletelimit
        log.debug("sleeping for %s seconds...", del_sleeptime)
        time.sleep(del_sleeptime)

    return tweetcache

def import_twitter_archive(tw, args, tweetcache):
    """Load tweets into tweetcache from an archive downloaded from Twitter"""

    # Load tweets from the tweet JSON file in the archive
    log.info("Importing twitter archive from %s", args.importfile)

    # Open the archive zipfile
    with zipfile.ZipFile(args.importfile) as ark:

        # The datafile provided is a Javascript variable assignment
        # of a JSON datastructure, which is a little frustrating to parse
        # Why can't Twitter just have the tweets be in a plain JSON file?
        with ark.open(TWEET_DATAFILE, 'r') as twdf:
            log.debug("Importing tweets from archive...")
            jsdata = twdf.readlines()
            # Edit the first line to strip out the variable assignment
            jsdata[0] = jsdata[0][ jsdata[0].index('['): ]
            tweetset = [ x['tweet'] for x in json.loads(''.join(jsdata)) ]

            # Augment the tweetcache with this data
            tweetcache.save_tweets(args.userids[0], tweetset)

    return tweetcache

def augment_args(args):
    """Augment commandline arguments with config file parameters"""
    cp = ConfigParser.SafeConfigParser()
    cp.read(os.path.expanduser(args.config))
    try:
        keeplist = cp.get('twitter', 'keeptweets')
        keeplist = [int(x) for x in keeplist.split()]
        log.debug('keeplist: %s', keeplist)

        if args.keeplist is not None:
            args.keeplist.extend(keeplist)
        else:
            args.keeplist = keeplist
        log.debug('args: %s', args.keeplist)

    except ConfigParser.NoOptionError:
        log.debug("No such option.")
        pass

    return args

def authenticate(args):
    """
    Authenticate with Twitter and return an authenticated
    Twitter() object to use for API calls
    """
    # import the config file
    cp = ConfigParser.SafeConfigParser()
    cp.read(os.path.expanduser(args.config))

    token = cp.get('twitter', 'token')
    token_key = cp.get('twitter', 'token_key')
    con_secret = cp.get('twitter', 'con_secret')
    con_secret_key = cp.get('twitter', 'con_secret_key')

    tw = twitter.Twitter(auth=twitter.OAuth(token,
                                            token_key,
                                            con_secret,
                                            con_secret_key))
    return tw

def valid_date(s):
    """Parse a string to see if it's a valid date or not"""
    try:
        return arrow.get(s)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

if __name__ == '__main__':

    ap = argparse.ArgumentParser(description="Delete old tweets",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument('userids', nargs='+', help="User id")

    ap.add_argument('-c', '--config', default='~/.twitrc', help="Config file")
    ap.add_argument('-b', '--batchsize', type=int, default=200, help="Fetch this many tweets per API call (max is Twitter API max, currently 200)")
    ap.add_argument('-d', '--deletemax', type=int, help="Only delete this many tweets.")
    ap.add_argument('-k', '--keeplist', type=int, nargs='+', help="Don't delete tweets in this list.")

    ap.add_argument('-B', '--date-before', help="Delete tweets before this date.", type=valid_date)
    ap.add_argument('-A', '--date-after', help="Delete tweets after this date.", type=valid_date)
    ap.add_argument('-K', '--keepnum', type=int, default=5000, help="How many tweets to keep.")

    ap.add_argument('--beforedays', type=int, help="Delete tweets from before this many days ago.")

    ap.add_argument('-i', '--importfile', default=None, help="Import tweets from Twitter archive.")

    ap.add_argument('--tweetcache', default='~/.tweetcache.db', help="File to store cache of tweet/date IDs")

    ap.add_argument('--nodelete', action='store_true', help="Skip the delete stage.")
    ap.add_argument('--nofetch', action='store_true', help="Skip the fetch stage.")

    ap.add_argument('--dryrun', action='store_true', help="Don't actually delete tweets, but do populate cache.")
    ap.add_argument('--loglevel', choices=['debug', 'info', 'warning', 'error', 'critical'], help="Set log output level.")

    ap.add_argument('--searchlimit', type=int, default=5, help="Max number of searches per minute.")
    ap.add_argument('--deletelimit', type=int, default=60, help="Max number of deletes per minute.")

    args = ap.parse_args()

    if args.loglevel is not None:
        levelname = args.loglevel.upper()
        log.setLevel(getattr(logging, levelname))

    # Safety feature: If you specific --after-date, force to also
    # provide --before-date to prevent accidental deletion of all tweets.
    if args.date_after is not None:
        if args.date_before is None:
            ap.error("Safety feature: Need to provide --date-before as well as --date-after.")

    args = augment_args(args)

    tw = authenticate(args)
    tweetcache = load_tweetcache(args)
    log.debug("tweetcache loaded.")

    if args.importfile:
        # Import tweets from a Twitter archive file you asked for
        # This reads in the data Twitter sends you when you ask
        # for an archive of your tweets, and stores it in the local tweetcache.
        # This is a handy way to bootstrap your archive rather than using the
        # slow and painful feed traversal mechanism.
        tweetcache = import_twitter_archive(tw, args, tweetcache)

    if not args.nofetch:
        tweetcache = fetch_all_tweets(tw, args, tweetcache)

    if tweetcache is None:
        raise ValueError("Unable to load any tweets for this user.")

    if not args.nodelete:
        tweetcache = destroy_tweets(tw, args, tweetcache)

    log.debug("Done.")
