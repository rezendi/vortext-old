# vortext

Web service which parses all the links users have ever posted to Twitter, organizes them in a timeline, expands shortened URLs,
and makes the results searchable by hashtag, @-mention, author (for retweets), keyword, or site. Circa 1000 lines of Python
2.7 and 100 of Javascript (with jQuery and Underline.js) running atop Google App Engine. Makes heavy use of App Engine's NDB
and task services.

Up and running at [www.vortext.co](http://www.vortext.co/).
Here's an [example](http://www.vortext.co/rezendi/search/bitcoin) of the parsed results.

Because Twitter's API limits access to a users' most recent 3200 tweets, it also accepts file uploads of your
[Twitter Archive](https://support.twitter.com/articles/20170160-downloading-your-twitter-archive)
and parses its contents as well. (Which is actually mildly tricky, it turns out, because old tweets haven't been updated
to match Twitter's new API format.)

Makes use of gaesessions, tweepy, and ttp (for old tweets.)

This is by no means production-quality code -- you'll notice, in particular, a dearth of tests -- but it seems to work
reasonably well and be reasonably useful. Hopefully useful as an example use of Twitter's API and handling old-style
tweets next to modern ones, as well.

Currently suffers from App Engine's limit of 1MB for any particular (non-blob) datastore entity; it'd be pretty easy to move
to SQL (either for entities above that size or all entities) but I haven't bothered yet. Also, it's got about three-quarters
of a customized web site for mobile users utilizing jQuery Mobile, but that's not (yet) ready for prime time.

MIT license. Pull requests welcome.
