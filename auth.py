import json, logging, os, random, string, urllib, webapp2
from google.appengine.api import memcache, users
from google.appengine.ext import ndb
from google.appengine.ext.webapp import template
from gaesessions import get_current_session
import tweepy
import model

class RegisterTwitter(webapp2.RequestHandler):
    def get(self):
        auth = tweepy.OAuthHandler(model.TWITTER_CONSUMER_KEY,
                                   model.TWITTER_CONSUMER_SECRET,
                                   secure=True)

        try:
            auth.secure = True
            redirect_url = auth.get_authorization_url(signin_with_twitter=True)
            session = get_current_session()
            session["rt_key_"] = auth.request_token.key
            session["rt_secret_"] = auth.request_token.secret
            self.redirect(redirect_url)
        except tweepy.TweepError, ex:
            logging.warn("Tweep Error %s" % ex)
            self.response.out.write('Error! Failed to get request token.')

class TwitterCallback(webapp2.RequestHandler):
    def get(self):
        verifier = self.request.get('oauth_verifier')
        auth = tweepy.OAuthHandler(model.TWITTER_CONSUMER_KEY, model.TWITTER_CONSUMER_SECRET, secure=True)

        session = get_current_session()
        request_key = session.get("rt_key_")
        request_secret = session.get("rt_secret_")
        if request_key is None or request_secret is None:
            self.response.out.write('Error! Failed to retain account/handle or request key/secret.')
            return

        auth.set_request_token(request_key, request_secret)
        try:
            auth.get_access_token(verifier)
        except tweepy.TweepError, ex:
            logging.warn("Tweep Error %s" % ex)
            self.response.out.write('Error! Failed to get access token.')
            return

        auth.set_access_token(auth.access_token.key, auth.access_token.secret)
        twitter = tweepy.API(auth)
        twitter_handle = twitter.auth.get_username()

        account = model.Account.query(model.Account.twitter_handle == twitter_handle).get()
        if account is None:
            account = model.Account()
            account.twitter_handle = twitter_handle
            account.status = model.STATUS_CREATED

        account.twitter_key = auth.access_token.key
        account.twitter_secret = auth.access_token.secret
        account.last_login = model.now()
        account.put()
        
        session["account"]=account.key.urlsafe()
        tempkey = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(20))
        memcache.set(tempkey, account.key.urlsafe(), 60)
        self.redirect("http://www.MYSITE.com/me?v=%s" % tempkey)

class LogOut(webapp2.RequestHandler):
    def get(self):
        session = get_current_session()
        session.terminate()
        if users.get_current_user():
            self.redirect(users.create_logout_url("/"))
        else:
            self.redirect("/")

app = webapp2.WSGIApplication([
    ('/auth/twitter', RegisterTwitter),
    ('/auth/twitter/callback', TwitterCallback),
    ('/auth/logout', LogOut),
    ],
    debug=True)

