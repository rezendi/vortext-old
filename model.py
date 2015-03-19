import datetime, logging
from google.appengine.api import memcache
from google.appengine.ext import db, ndb
import tweepy

TWITTER_CONSUMER_KEY    = "YOUR_CONSUMER_KEY"
TWITTER_CONSUMER_SECRET = "YOUR_CONSUMER_SECRET"

STATUS_CREATED          = 1
STATUS_FETCH_INITIATED  = 10
STATUS_FETCH_COMPLETE   = 100
STATUS_PARSE_URLS       = 200
STATUS_EXPANDING_URLS   = 300
STATUS_URLS_EXPANDED    = 400
STATUS_COMPLETE         = 500
STATUS_FILE_UPLOADED    = 1000
STATUS_FILE_UNPACKED    = 2000

MONTHS={1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun', 7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'}

class Account(ndb.Model):
  time_created = ndb.DateTimeProperty(auto_now_add=True)
  time_edited = ndb.DateTimeProperty(auto_now=True)
  last_login = ndb.DateTimeProperty()
  status = ndb.IntegerProperty()
  name = ndb.StringProperty()
  email = ndb.StringProperty()
  privacy = ndb.IntegerProperty()
  twitter_handle = ndb.StringProperty()
  twitter_key = ndb.StringProperty()
  twitter_secret = ndb.StringProperty()
  twitter_max = ndb.IntegerProperty()
  urls = ndb.JsonProperty(compressed=True)
  keywords = ndb.JsonProperty(compressed=True)
  timeline = ndb.JsonProperty(compressed=True)
  
  def is_private(self):
    return False if self.privacy is None else self.privacy>0
  
  #it's more than possible that this field is excessively overloaded
  def update_status(self, new_status):
    if self.status is None:
      self.status = new_status
    elif new_status in [STATUS_FETCH_INITIATED, STATUS_FETCH_COMPLETE, STATUS_EXPANDING_URLS, STATUS_COMPLETE]:
      self.status = (self.status/1000)*1000 + new_status
    elif new_status/100==0:
      self.status = (self.status/100)*100 + new_status
    elif new_status/1000==0:
      self.status = (self.status/1000)*1000 + new_status + (self.status % 100)
    elif new_status/1000>0:
      self.status = new_status + (self.status % 1000)
    
    if self.key is None:
      self.put()
    memcache.set("%s_status" % self.key.urlsafe(), self.status, 10800)
  
  def newest_raw_data(self):
    q = RawData.query(ndb.AND(RawData.account_key == self.key,RawData.source=="twitter"))
    return q.order(-RawData.time_created).get()

  def has_uploaded_data(self):
    uploaded = RawData.query(ndb.AND(RawData.account_key == self.key,RawData.source=="twitter_upload"))
    uploaded = uploaded.order(-RawData.time_created).get()
    return uploaded is not None

  def raw_uploaded_data(self):
    uploaded = RawData.query(ndb.AND(RawData.account_key == self.key,RawData.source=="twitter_upload"))
    uploaded = uploaded.order(-RawData.time_created).get()
    if uploaded is None:
      uploaded = RawData(account_key = self.key, source = "twitter_upload", data = [])
      uploaded.put()
    return uploaded

  def newest_upload(self):
    return UploadedFile.query(UploadedFile.account_key == self.key).order(-UploadedFile.time_created).get()
  
  def is_private(self):
    return self.privacy==1
  
  def show_name(self):
    return self.name if self.name is not None else ""

  def show_email(self):
    return self.email if self.email is not None else ""


class RawData(ndb.Model):
  time_created = ndb.DateTimeProperty(auto_now_add=True)
  time_edited = ndb.DateTimeProperty(auto_now=True)
  account_key = ndb.KeyProperty(Account)
  source = ndb.StringProperty()
  data = ndb.JsonProperty(compressed=True)


class UploadedFile(ndb.Model):
  time_created = ndb.DateTimeProperty(auto_now_add=True)
  time_edited = ndb.DateTimeProperty(auto_now=True)
  account_key = ndb.KeyProperty(Account)
  blob_key = ndb.BlobKeyProperty()
  source = ndb.StringProperty()


## Access methods

def account_for(keystring):
  if keystring is None or keystring=='':
    return None
  key = ndb.Key(urlsafe = keystring)
  account = key.get()
  if account is not None:
    if account.timeline is None: account.timeline = []
    if account.keywords is None: account.keywords = []
    if account.urls is None: account.urls = {}
  return account

def twitter_for(account):
  if account is not None:
    return twitter_with(account.twitter_key, account.twitter_secret)
  return None

def twitter_with(key, secret):
    auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, secure=True)
    auth.set_access_token(key, secret)
    twitter = tweepy.API(auth, parser=tweepy.parsers.RawParser())
    return twitter

def url_cache(rawkey):
  cache = RawData.query(RawData.account_key == None).order(-RawData.time_created).get()
  if cache is not None:
    try:
      cache_size = len(cache._to_pb().Encode())
      logging.info("cache size %s" % cache_size)
      if cache_size > 960000:
        logging.info("creating new cache")
        cache = None
    except Exception, ex:
      logging.warn("error checking cache size: %s" % ex)
  if cache is None:
    cache = RawData(account_key = None, source = "twitter", data = {})
    cache.put()
  return cache


## Convenience methods

class DictObj(object):
    def __getattr__(self, attr):
        return self.__dict__.get(attr)

    def __getitem__(self, item):
        return self.__dict__.get(item)

    def __repr__(self):
        return '%s' % self.__dict__

def unicodize(s):
  if s is None:
    return ""
  elif not isinstance(s, str):
    return s
  elif not isinstance(s, unicode):
    return unicode(s,"utf-8",errors="ignore")
  return s

def now():
  return datetime.datetime.now()

def status_string(status):
  if status is None: return "None"
  if status == STATUS_CREATED: return "Created"
  if status == STATUS_FILE_UNPACKED + STATUS_COMPLETE: return "All Tweets Parsed"
  if status % 100 == STATUS_FETCH_INITIATED: return "Fetch initiated, please wait..."
  if status % 100 > STATUS_FETCH_INITIATED: return "Fetched %s tweets, please wait..." % ((status % 100-STATUS_FETCH_INITIATED)*200)
  if status % 1000 == STATUS_FETCH_COMPLETE: return "Fetch Complete, please wait..."
  if status % 1000 == STATUS_PARSE_URLS: return "Parsing URLs, please wait..."
  if status % 1000 == STATUS_EXPANDING_URLS: return "Expanding URLs"
  if status % 1000 == STATUS_URLS_EXPANDED: return "URLS Expanded"
  if status/1000 == STATUS_FILE_UPLOADED/1000: return "File Uploaded"
  if status/1000 == STATUS_FILE_UNPACKED/1000: return "File Unpacked"
  if status % 1000 == STATUS_COMPLETE: return "Parse Complete"
  return "Unknown"
  
