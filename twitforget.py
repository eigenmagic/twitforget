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

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('twitforget')

class NoMoreTweets(Exception):
    """
    Raised if there are no more tweets to fetch.
    """

def sort_tweets(tweetcache):
    """
    Sorts a sorted list of tweets, ordered by 'created_at'
    """
    # Sort tweets by created_at
    # FIXME: Timezones. Le sigh.
    return sorted(tweetcache.values(), key=lambda x: datetime.datetime.strptime(x['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))    

def save_tweetcache(args, tweetcache):
    """
    Save fetched tweets into the tweetcache.
    """
    if args.no_tweetcache:
        log.debug("tweetcache disabled. Not saving tweets.")
        return
    
    fieldnames = ('id', 'screen_name', 'created_at', 'content_text')
    
    with open(os.path.expanduser(args.tweetcache), 'w') as ofd:
        csv_writer = csv.DictWriter(ofd, fieldnames)
        csv_writer.writeheader()
        tweetset = sort_tweets(tweetcache)
        for item in tweetset:
            #log.debug("Writing row: %s", item)
            csv_writer.writerow(item)
            pass
        pass
    pass
    
def load_tweetcache(args):
    """
    If we already have a cache of tweets, load it.

    Passes an empty dictionary if the cache doesn't exist.
    """
    tweetcache = {}

    if args.no_tweetcache:
        log.debug("tweetcache disabled.")
        return tweetcache
        
    try:
        fd = open(os.path.expanduser(args.tweetcache), 'r')
        csv_reader = csv.DictReader(fd)

        for row in csv_reader:
            tweetcache[int(row['id'])] = { 'id': int(row['id']),
                                      'screen_name': row['screen_name'],
                                      'created_at': row['created_at'],
                                      # Don't decode this when we read it in. We want it in utf-8
                                      #'content_text': unicode(row['content_text'], 'utf-8'),
                                      'content_text': row['content_text'],
            }
            pass
        
    except IOError, e:
        if e.errno == 2:
            log.debug("No tweetcache exists yet.")
        else:
            raise

    log.debug("Loaded %d entries from cache.", len(tweetcache))
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

    # Get a list of tweets for the user no older than the
    # specified time ago.
    #first_search = tw.search.tweets(q='@%s' % username, until='2014-01-01')
    #log.debug("search tweets: %s", first_search)

    while(1):
        try:
            tweetcache = update_user_cache(tw, username, args, tweetcache)
            # Write the tweet cache out every iteration
            # Yes, this is slow, noisy, inefficient, etc. but it'll work for
            # now and it'll save a lot of time if something breaks halfway
            # through a long run.
            save_tweetcache(args, tweetcache)

            log.debug("sleeping for %s seconds...", args.sleeptime)
            time.sleep(args.sleeptime)
        except NoMoreTweets, e:
            log.info("No more tweets available: %s", e)
            return tweetcache

def update_user_cache(tw, username, args, tweetcache):

    # Find the smallest tweet id in the tweet cache,
    # and try to fetch another batch of tweets earlier than that
    max_id = None    
    if tweetcache == {}:
        log.debug("Fetching first set of %d tweets...", args.batchsize)
        tweets = tw.statuses.user_timeline(screen_name=username,
                                           count=args.batchsize,
                                           #exclude_replies=True,
                                       )
    else:
        max_id = min(tweetcache.keys()) - 1
        log.debug("Fetching %d tweets before tweet id: %s ...", args.batchsize, max_id)
        tweets = tw.statuses.user_timeline(screen_name=username,
                                           max_id=max_id,
                                           count=args.batchsize,
                                           #exclude_replies=True,
        )

    if tweets == []:
        raise NoMoreTweets("No tweets returned from Twitter.")

    log.debug("Fetched %d tweets.", len(tweets))

    if 2 == 1:
        twt = tw.statuses.show(id=tweets[0]['id'])
        log.debug("Only this tweet returned: %s: %s", twt['id'], twt['text'])
        log.debug("   trying to fetch older again...")

        newmax = sort_tweets(tweetcache)[5]['id']
        log.debug("Trying 5th oldest tweet id: %s", newmax)

        tweets = tw.statuses.user_timeline(screen_name=username,
                                           max_id=newmax)
        log.debug("  got %s", len(tweets))
    
    for twt in tweets:
        tweetcache[twt['id']] = { 'id': twt['id'],
                                  'screen_name': username,
                                  'created_at': twt['created_at'],
                                  'content_text': twt['text'].encode('utf-8'),
                              }
        pass

    # If we didn't fetch any older tweets, then stop
    if max_id == min(tweetcache.keys()):
        raise NoMoreTweets("Reached oldest tweet that Twitter will let us have.")
        
    return tweetcache

def destroy_tweets(tw, args, tweetcache):
    
    tweetset = sort_tweets(tweetcache)

    # Delete tweets older than the number we're going to keep
    #log.debug("First 5 tweets: %s", tweetset[:5])
    destroy_tweetset = tweetset[:-args.keep]

    log.debug("Need to destroy %d tweets.", len(destroy_tweetset))
    for idx, twt in enumerate(destroy_tweetset):
        log.debug("Destroying tweet id %s [%s]: %s", twt['id'], twt['created_at'], unicode(twt['content_text'], 'utf-8'))
        try:
            if not args.dryrun:
                gone_twt = tw.statuses.destroy(id=twt['id'])
                del tweetcache[twt['id']]
                #log.debug("Gone tweet %s: %s", gone_twt['id'], gone_twt['text'])

        except twitter.api.TwitterHTTPError, e:
            log.debug("Response: %s", e.response_data)
            errors = e.response_data['errors']
            if len(errors) == 1 and errors[0]['code'] == 144:
                log.info("Tweet with this id doesn't exist. Possibly stale cache entry. Removing.")
                try:
                    del tweetcache[twt['id']]
                except KeyError:
                    log.debug("tweetcache ids: %s", tweetcache.keys())
                    raise
            else:
                raise
        
        finally:
            save_tweetcache(args, tweetcache)
            pass

        log.info("Tweet %d of %d destroyed.", idx, len(destroy_tweetset))
    save_tweetcache(args, tweetcache)

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
    ap.add_argument('-k', '--keep', type=int, default=2000, help="How many tweets to keep.")
    ap.add_argument('-s', '--sleeptime', type=int, default=170/15, help="Time to sleep between API accesses.")    

    ap.add_argument('--tweetcache', default='~/.tweetcache', help="File to store cache of tweet/date IDs")
    ap.add_argument('--no-tweetcache', action='store_true', default=True, help="Disable tweetcache.")

    ap.add_argument('--dryrun', action='store_true', help="Don't actually delete tweets, but do populate cache.")
    
    args = ap.parse_args()

    tw = authenticate(args)
    tweetcache = load_tweetcache(args)
    tweetcache = fetch_all_tweets(tw, args, tweetcache)

    if tweetcache is None:
        raise ValueError("Unable to load any tweets for this user.")

    tweetcache = destroy_tweets(tw, args, tweetcache)

    log.info("Done.")    
