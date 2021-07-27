# twitforget
*Delete your tweet history*

Twitforget uses the Twitter API to delete your tweet history.

## Getting Started

### Authorising the app

You need to authorise the app with a Twitter access token for your
Twitter account. Follow the instructions here:
https://dev.twitter.com/oauth/overview/application-owner-access-tokens

Add the access token to a configuration file, (defaults to ```~/.twitrc```) like
this:

```
[twitter]
token = <token>
token_key = <token_key>
con_secret = <con_secret>
con_secret_key = <con_secret_key>
```

### Loading your tweet archive

You can bootstrap twitforget by [asking Twitter for an archive of all your tweets](https://help.twitter.com/en/managing-your-account/how-to-download-your-twitter-archive).
Twitforget can then read in this archive and use it to keep track of your tweets
from that point onwards, deleting your history as you go.

To request your Twitter archive, [follow the instructions here](https://help.twitter.com/en/managing-your-account/how-to-download-your-twitter-archive).

Once you've downloaded the archive ZIP file, load it with twitforget like this:

```
./twitforget.py -i <zipfile> --nofetch --nodelete <your_twitter_handle>
```
This will just load the data from the archive and won't do anything else.

You're now ready to start deleting your tweets!

## How it works
Twitforget fetches a list of your tweet history, and then deletes tweets using a
few different methods, depending on how you want to maintain your history.

If you just want to test things without actually deleting any tweets,
use the ```--dryrun``` flag.

### ```-K``` Keep the last _n_ tweets

The default method twitforget uses is to keep all but your last _n_ tweets. You
can tell twitforget how many tweets to keep using the ```-K``` flag. It defaults to keeping the last 5000 tweets.

### Keep the last _n_ days of tweets

Use the ```--before-days``` flag to tell twitforget to delete tweets sent more
than _n_ days ago. Use it to keep anything more recent.

### Delete tweets in a date range

Use the ```-A```/```--after-days``` and the ```-B```/```--before-days``` flags
to define a date range to delete tweets. Twitforget will delete all the tweets
before ```--before-days``` and after ```--after-days```.

### Keep specific tweets

You might particularly like certain tweets and would prefer to keep them. No
problem! Tell twitforget to keep those tweets using the ```-k``` flag and providing
the tweet id for the good tweet. You can provide the flag multiple times if you
have more than one good tweet you'd like to keep.

That can get a bit tedious when you have a lot of good tweets you want to keep,
so you can also add the tweet ids to your config file with the ```keeptweets```
parameter and twitforget will always know to keep those tweets.

Something like this:
```
keeptweets = 825526299879829504
    846940673173540864
    846941755597574145
    869115756755034112
    875394335298142208
```
(These are real tweets of mine that I've decided to keep, so you can go check.)

If you make a new, good tweet and decide to keep it, just add it to the list.

## Deleting Likes

You can also delete your likes with the ```likesforget.py``` command.

It's based on ```twitforget.py``` so it functions pretty much the same way, with
the same flags and approach to things, just adapted to work with likes.

### Getting started

Import your likes archive in the same way you did for your tweets:
```
./likesforget.py -i <zipfile> --nofetch --nodelete <your_twitter_handle>
```

### Delete range

Twitter doesn't provide much detail for likes, so the script doesn't currently
populate the cache with creation times.

### Keep specific likes

Just as with keeping tweets, you might want to keep certain likes. Use the
```-k``` flag, or add the id of the tweets you've liked to your config file with
the ```keeplikes``` parameter and those likes will be kept.

## Limitations

### 3200 tweet history limit

Twitforget has a limitation imposed by Twitter: Twitter only lets you see your
last 3200 tweets.

If you've sent more tweets than that since the last time twitforget added tweets
to its database, you can't delete tweets older than the 3200 limit unless you
get a new archive from twitter and augment your current tweetcache.

twitforget won't overwrite existing information in the tweetcache, so you can
safely load in a Twitter archive file as many times as you like. It'll only add
missing tweets.

#### Verified Users

If you're a verified user, it appears you can see more of your own history
than a non-verified user, so twitforget can delete more of your history.

### API limits

Twitforget deliberately runs a little bit slower than what the Twitter API
limits allow (at time of writing) so the first run of deleting might take a
little while, depending on how many tweets you have to delete.

twitforget is design to run in the background on a schedule once you've done the
initial load and purge of setup.

## The tweetcache

twitforget stores a very basic summary of your tweet history in a SQLlite
database stored by default in your home directory in `~/.tweetcache.db`.

It only stores the tweet id, handle, tweet creation datetime, the tweet text as returned by Twitter, and whether or not twitforget thinks it's deleted the tweet.

If you want to store a full archive of your tweets, keep a copy of what Twitter
sends you when you first set up this tool, and then periodically request an updated
archive before you delete tweets with twitforget.

One day we might start storing the full JSON payload returned from Twitter when
we fetch tweet info, but keeping tweets isn't really what twitforget is about.