#!/usr/bin/env python
# Delete old tweets
# Copyright Justin Warren <justin@eigenmagic.com>

import sys
import os.path
import argparse
import ConfigParser
from itertools import izip_longest
import datetime

import csv

import twitter
import time

import sqlite3

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('twitforget')

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
        (id integer, screen_name text, created_at datetime, content_text text)""")
        self.conn.commit()

    def __len__(self):
        c = self.conn.cursor()
        c.execute("SELECT count(*) FROM tweets")
        result = c.fetchone()[0]
        log.debug("There are %d tweets.", result)
        return result

    def __delitem__(self, tweetid):
        log.debug("Deleting tweet id %d from cache...", tweetid)
        
        c = self.conn.cursor()
        c.execute("DELETE FROM tweets WHERE id = ?", (tweetid,))
        self.conn.commit()
        return

    def save_tweets(self, username, tweets):
        c = self.conn.cursor()
        valset = [ ( twt['id'],
                     username,
                     twt['created_at'],
                     twt['text'],
                 ) for twt in tweets ]
        c.executemany("""INSERT INTO tweets
            (id, screen_name, created_at, content_text)
            VALUES (?, ?, ?, ?)""", valset)
        self.conn.commit()
        log.debug("Tweets saved in database.")

    def get_min_id(self):
        c = self.conn.cursor()
        c.execute("SELECT min(id) FROM tweets")
        res = c.fetchone()[0]
        log.debug("minimum id is: %d", res)
        return res

    def get_max_id(self):
        c = self.conn.cursor()
        c.execute("SELECT max(id) FROM tweets")
        res = c.fetchone()[0]
        log.debug("max id is: %d", res)
        return res
    
    def get_destroy_set(self, keepnum, deletenum=None):
        """
        Return a list of tweets to destroy.

        Returns a set of tweets sorted by age, keeping keepnum of the newest ones.
        """
        c = self.conn.cursor()
        QUERY = """SELECT * FROM tweets
        WHERE id NOT IN
        (SELECT id FROM tweets ORDER BY id DESC LIMIT ?)
        ORDER BY id ASC
        """
        PARAMS = [keepnum, ]
        if deletenum is not None:
            QUERY += " LIMIT ?"
            PARAMS.append(deletenum)
            
        c.execute(QUERY, PARAMS)
        result = c.fetchall()
        return result
        
def save_tweetcache(args, tweetcache):
    """
    Save fetched tweets into the tweetcache.
    """
    log.debug("tweetcache passed in has %d records.", len(tweetcache))
    
    if args.no_tweetcache:
        log.debug("tweetcache disabled. Not saving tweets.")
        return
        
    fieldnames = ('id', 'screen_name', 'created_at', 'content_text')
    
    with open(os.path.expanduser(args.tweetcache), 'w') as ofd:
        csv_writer = csv.DictWriter(ofd, fieldnames)
        csv_writer.writeheader()
        tweetset = sort_tweets(tweetcache)
        for item in tweetset:
            log.debug("Writing row: %s", item)
            csv_writer.writerow(item)
            pass
        pass
    pass
    
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
        known_max_id = tweetcache.get_max_id()
        log.debug("Getting tweets since %s ...", known_max_id)
        
        tweets = tw.statuses.user_timeline(screen_name=username,
                                           count=args.batchsize,
                                           since_id=known_max_id,
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
    Get tweets that are newer than what's in the cache.
    """
    fetching = True
    known_min_id = None
    while(fetching):

        if len(tweetcache) == 0:
            log.debug("Fetching first set of %d tweets...", args.batchsize)
            tweets = tw.statuses.user_timeline(screen_name=username,
                                               count=args.batchsize,
                                               #exclude_replies=True,
            )
        else:
            # Get earliest tweet id
            min_id = tweetcache.get_min_id()
            if known_min_id == min_id:
                log.debug("Didn't find any new tweets. All done.")
                break
            known_min_id = min_id
            
            log.debug("Fetching %d tweets before tweet id: %s ...", args.batchsize, known_min_id - 1)
            tweets = tw.statuses.user_timeline(screen_name=username,
                                           count=args.batchsize,
                                           max_id=known_min_id - 1,
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

def destroy_tweets(tw, args, tweetcache):
    
    #tweetset = sort_tweets(tweetcache)

    # Delete tweets older than the number we're going to keep
    #log.debug("First 5 tweets: %s", tweetset[:5])
    log.debug("Keeping %d tweets...", args.keep)
    destroy_tweetset = tweetcache.get_destroy_set(args.keep, args.delete)

    log.debug("Need to destroy %d tweets.", len(destroy_tweetset))
    
    for idx, twt in enumerate(destroy_tweetset):
        log.debug("Destroying tweet id %s [%s]: %s", twt['id'], twt['created_at'], twt['content_text'])
        try:
            if not args.dryrun:
                gone_twt = tw.statuses.destroy(id=twt['id'])
                del tweetcache[twt['id']]
                log.debug("Gone tweet %s: %s", gone_twt['id'], gone_twt['text'])
            else:
                log.debug("Tweet not actually deleted.")

        except twitter.api.TwitterHTTPError, e:
            log.debug("Response: %s", e.response_data)
            errors = e.response_data['errors']
            if len(errors) == 1 and errors[0]['code'] == 144:
                log.warn("Tweet with this id doesn't exist. Possibly stale cache entry. Removing.")
                try:
                    del tweetcache[twt['id']]
                except KeyError:
                    raise
            else:
                raise

        log.info("Tweet %d of %d destroyed.", idx+1, len(destroy_tweetset))
        del_sleeptime = 60 / args.deletelimit
        log.debug("sleeping for %s seconds...", del_sleeptime)
        time.sleep(del_sleeptime)
        
    return tweetcache

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
    
if __name__ == '__main__':

    ap = argparse.ArgumentParser(description="Delete old tweets",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument('userids', nargs='+', help="User id")
    
    ap.add_argument('-c', '--config', default='~/.twitrc', help="Config file")
    ap.add_argument('-b', '--batchsize', type=int, default=200, help="Fetch this many tweets per API call (max is Twitter API max, currently 200)")
    ap.add_argument('-k', '--keep', type=int, default=5000, help="How many tweets to keep.")
    ap.add_argument('-d', '--delete', type=int, help="Only delete this many tweets.")

    ap.add_argument('--tweetcache', default='~/.tweetcache.db', help="File to store cache of tweet/date IDs")

    ap.add_argument('--fetchonly', action='store_true', help="Just run the fetch stage and then exit.")
    ap.add_argument('--nofetch', action='store_true', help="Skip the fetch stage.")
    ap.add_argument('--dryrun', action='store_true', help="Don't actually delete tweets, but do populate cache.")
    ap.add_argument('--loglevel', choices=['debug', 'info', 'warning', 'error', 'critical'], help="Set log output level.")

    ap.add_argument('--searchlimit', type=int, default=5, help="Max number of searches per minute.")
    ap.add_argument('--deletelimit', type=int, default=5, help="Max number of deletes per minute.")
    
    args = ap.parse_args()

    if args.loglevel is not None:
        levelname = args.loglevel.upper()
        log.setLevel(getattr(logging, levelname))

    tw = authenticate(args)
    tweetcache = load_tweetcache(args)
    log.debug("tweetcache loaded.")
    if not args.nofetch:
        tweetcache = fetch_all_tweets(tw, args, tweetcache)

    if tweetcache is None:
        raise ValueError("Unable to load any tweets for this user.")

    if not args.fetchonly:
        tweetcache = destroy_tweets(tw, args, tweetcache)

    log.debug("Done.")    
