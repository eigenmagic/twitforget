# twitforget
*Delete your tweet history*

Twitforget uses the Twitter API to delete your tweet history.

## Setup

You need to authorise the app with a Twitter access token for your
Twitter account. Follow the instructions here:
https://dev.twitter.com/oauth/overview/application-owner-access-tokens

Add the access token to a configuration file, (defaults to ~/.twitrc) like
this:

```
[twitter]
token = <token>
token_key = <token_key>
con_secret = <con_secret>
con_secret_key = <con_secret_key>
```

## How it works
twitforget fetches a list of your tweet history, and deletes all but your
last *n* tweets. You can tell twitforget how many tweets to keep using the
```-k``` flag.

If you just want to test things without actually deleting any tweets,
use the ```--dryrun``` flag.

## Limitations

Twitforget has a limitation imposed by Twitter: Twitter only lets you see your
last 3200 tweets, so if you've tweeted more than that, you can't delete
tweets earlier than that unless you can somehow find their tweet id.

This means that tweets older than your last 3200 tweets cannot be deleted by
twitforget, but if you set up a regular schedule of deleting tweets, you
can keep your tweet history from getting any bigger.

## The tweetcache
The tweetcache code is still present, but it's disabled. This is because it's
actually counter-productive, because of the way the Twitter API works.

The idea was that you'd cache tweets in case something went wrong as you
fetched tens (or hundreds) of thousands of tweets the first time. We figured
that if Twitter only let you grab the last 3200 tweets, if you deleted a
block of, say 500 tweets, from within that 3200, then the start of the 3200
tweet window would go further back in time, to earlier tweets. You'd thus be
able to look up those earlier tweets ids, and delete them.

In that way, we could gradually work our way backwards through your tweetstream
until we'd deleted all the tweets.

Alas, it doesn't work that way.

Twitter seems to keep track of deleted tweets in some way (possibly related to
Politwoops? http://www.csmonitor.com/Technology/2016/0108/Twitter-revives-Politwoops-the-tool-that-preserves-politicians-deleted-tweets) and includes them in the history count. The 3200 tweet
limit thus seems to apply to tweets you sent, including any deleted ones.

That's a drag, because it means we can't figure out a way for you to go back
in time and delete your own tweets from early on in your Twitter stream, unless
you keep a record of your tweet IDs somewhere else.


