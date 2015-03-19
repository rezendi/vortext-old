import datetime, json, logging, string, re, urlparse, webapp2, zipfile
from google.appengine.api import mail, memcache, taskqueue, urlfetch
from google.appengine.ext import blobstore, ndb
import ttp, tweepy
import model

MAX_REPS = 16

STOP_WORDS = "rt,mt,a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your".split(",")

class TwitterFetcher(webapp2.RequestHandler):
  def post(self):
    key = self.request.get("key")
    counter = 1 if self.request.get("counter")=='' else int(self.request.get("counter"))
    max_id = None if self.request.get("max_id")=='' else int(self.request.get("max_id"))
    send_email = self.request.get("send_email")
    logging.info("Fetch iteration %s max_id %s" % (counter, max_id))

    raw = ndb.Key(urlsafe = key).get()
    account = raw.account_key.get()
    twitter = model.twitter_for(account)
    raw_json = twitter.user_timeline(screen_name = account.twitter_handle,
                                     count = 200,
                                     max_id = max_id)

    statuses = parse_raw_twitter(raw_json)

    if len(statuses)==0:
      logging.warn("No more tweets, bailing out")
      memcache.delete("fetch_%s" % account.key.urlsafe())
      taskqueue.add(queue_name='default',
                    url='/tasks/parse',
                    params={'key' : key, 'clean_urls' : 'true', 'send_email' : send_email})
      return

    new_max = statuses[-1]['id']-1 if max_id is None or statuses[-1]['id'] < max_id else max_id
    logging.info("New max %s" % new_max)

    account.update_status(model.STATUS_FETCH_INITIATED + counter);
    raw.data = raw.data + statuses
    raw.put()

    if counter == MAX_REPS or new_max == max_id:
      logging.info("Fetch complete at iteration %s", counter)
      memcache.delete("fetch_%s" % account.key.urlsafe())
      account.update_status(model.STATUS_FETCH_COMPLETE);
      account.put()
      taskqueue.add(queue_name='default',
                    url='/tasks/parse',
                    params={'key' : key, 'clean_urls' : 'true', 'send_email' : send_email})

    elif counter < MAX_REPS:
      logging.info("Iteration %s complete", counter)
      taskqueue.add(queue_name='default',
                    url='/tasks/fetch',
                    params={'key' : key,
                            'counter' : counter+1,
                            'max_id' : new_max,
                            'send_email' : send_email,
                            })

class TwitterParser(webapp2.RequestHandler):
  def post(self):
    raw_key = self.request.get("key")
    send_email = self.request.get("send_email")
    raw = ndb.Key(urlsafe = raw_key).get()
    account = model.account_for(raw.account_key.urlsafe())
    statuses = raw.data
    logging.info("parsing %s statuses" % len(statuses))

    if self.request.get("clean_all")=="true":
      account.urls = {}
      account.keywords = []
      account.timeline = []

    account.update_status(model.STATUS_PARSE_URLS);

    new_timeline = self.build_timeline (statuses)
    account.timeline = self.merge_timeline(account.timeline, new_timeline)

    new_urls = reverse_dict(self.flattened_dict_for(statuses, 'urls'))
    account.urls = self.merge_dicts(account.urls, new_urls)

    authors = self.authors_for(statuses, account.twitter_handle)
    keywords = self.flattened_dict_for(statuses, 'keywords')
    hashtags = self.flattened_dict_for(statuses, 'hashtags')
    mentions = self.flattened_dict_for(statuses, 'mentions')
    
    sites = {}
    urls = reverse_dict(account.urls)
    for key in urls:
      site = self.url_to_site(key)
      sites[site] = sites[site] + urls[key] if site in sites else urls[key]
    #paths = map(self.path_to_words, urls)
    
    collected = [['author',authors], ['hashtag',hashtags], ['keyword',keywords], ['mention',mentions], ['site', sites]]
    existing = account.keywords

    if existing is not None and len(existing)>0:
      for collection in collected:
        if collection[0]=='site': next
        previous = filter(lambda x:x[0]==collection[0], existing)
        if len(previous)>0:
          previous = previous[0][1]
          collection[1] = self.merge_dicts(previous, collection[1])

    account.keywords = collected

    urlphase = self.request.get("clean_all")=="true" or self.request.get("clean_urls")=="true"
    account.update_status(model.STATUS_EXPANDING_URLS if urlphase else model.STATUS_COMPLETE);
    account.put()

    if urlphase:
      logging.info("Urlphase complete")
      taskqueue.add(queue_name='expander',
                    url='/tasks/expand',
                    params={'key' : raw_key, 'send_email' : send_email})

    else:
      memcache.delete("parse_%s" % account.key.urlsafe())
      logging.info("Parsing complete")
      if "True"==self.request.get('send_email'):
        self.send_email_to(account)

  def build_timeline(self, statuses):
    timeline = []
    vals = {}
    for status in statuses:
      if "time" in status:
        tf = "%Y-%m-%d %H:%M:%S +0000" if status['time'].startswith('20') else "%a %b %d %H:%M:%S +0000 %Y"
        time = datetime.datetime.strptime(status['time'], tf)
        year = time.year
        if not year in vals:
          vals[year] = {}
        month = json.dumps([time.month, time.strftime("%B")])
        id_str = str(status["id"]).encode("utf-8")
        vals[year][month] = vals[year][month]+[id_str] if month in vals[year] else [id_str]

    for year in sorted(vals.keys()):
      top = {'number' : year, 'months' : [] }
      timeline.append(top)
      for month_key in sorted(vals[year].keys(), key=lambda x:json.loads(x)[0]):
        num_name = json.loads(month_key)
        bottom = {
          'number' : num_name[0],
          'name' : num_name[1],
          'count' : len(vals[year][month_key]),
          'ids' : vals[year][month_key]
        }
        top['months'].append(bottom)

    return timeline

  def merge_timeline(self, tl1, tl2):
    years = map(lambda x:x['number'], tl1)+map(lambda x:x['number'], tl2)
    for year in sorted(years):
      y1s = filter(lambda x:x['number']==year, tl1)
      y2s = filter(lambda x:x['number']==year, tl2)
      if len(y2s)==0:
        pass
      elif len(y1s)==0:
        tl1.append(y2s[0])

      else: #there's  an overlapping year
        (y1, y2) = (y1s[0], y2s[0])
        for month in range(1,12):
          m1s = filter(lambda x:x['number']==month, y1['months'])
          m2s = filter(lambda x:x['number']==month, y2['months'])
          if len(m2s)==0:
            pass
          elif len(m1s)==0:
            y1['months'].append(m2s[0])

          else: # there's an overlapping month
            (m1, m2) = (m1s[0], m2s[0])
            m1['ids'] = list(set(m1['ids']+m2['ids']))
            m1['count'] = len (m1['ids'])
      
        y1['months'].sort(key = lambda x:x['number'])

    tl1.sort(key = lambda x:x['number'])
    return tl1

  def authors_for(self, statuses, handle):
    tuples = [(s['author'] , str(s['id'])) for s in statuses if s['author']!=handle]

    return self.dict_for (tuples)
  
  def flattened_dict_for(self, statuses, key):
    list_of_lists = map(lambda x:(x[key] , str(x['id'])), statuses)
    tuples = [(entry, sublist[1]) for sublist in list_of_lists for entry in sublist[0]]
    return self.dict_for(tuples)
  
  def dict_for(self, tuples):
    vals = {}
    for t in tuples:
      if t is not None:
        (key, tweetid) = (t[0].encode('utf-8'),  t[1])
        vals[key] = vals[key] + [tweetid] if key in vals else [tweetid]
    return vals
  
  def merge_dicts(self, dict1, dict2):
    if dict1 is None: return dict2
    for key in dict2.keys():
      if key in dict1 and dict1[key] != dict2[key]:
        dict1[key] = list(set(dict1[key]+dict2[key]))
      else:
        dict1[key] = dict2[key]
    return dict1

  def url_to_site(self, url):
    s = urlparse.urlparse(url).netloc
    if s.count(".")>1:
      s=s[s.find(".")+1:]
    return s.lower()
  
  def path_to_words(self, urltuple):
    retval = []
    path = urlparse.urlparse(urltuple[0]).path
    components = path.split("/")
    for component in components:
      if len(component)<12:
        continue
      for divider in ["-","_"]:
        if divider in component:
          words = component.split(divider)
          for word in words:
            if len(word)>2 and word.isalpha() and not word.lower() in STOP_WORDS:
              retval += [(word, urltuple[1])]
    return retval

  def send_email_to(self, account):
    if account.email is not None:
      mail.send_mail(sender="Vortext <info@vortext.co>",
                     to=account.email,
                     subject="Your tweets have been parsed",
                     body="All set! Go ahead and check out all your tweeted links at http://www.vortext.co/me")
  

class UrlExpander(webapp2.RequestHandler):
  local_cache = None
  
  def post(self):
    key = self.request.get("key")
    raw = ndb.Key(urlsafe = key).get()
    account = raw.account_key.get()
    logging.info("Expanding %s urls" % len(account.urls))

    urls = reverse_dict(account.urls)
    urls = self.clean_urls(urls)
    account.urls = reverse_dict(urls)
    account.update_status(model.STATUS_URLS_EXPANDED);
    account.put()
    self.cache().put()
    taskqueue.add(queue_name='default',
                  url='/tasks/parse',
                  params={'key' : key, 'clean_urls' : "false", 'send_email' : self.request.get("send_email")})

  def clean_urls(self, urldict):
    counter = 1
    for url in urldict.keys():

      final_url = self.check_cache(url)
      if final_url is not None:
        #logging.info("Cache resolved %s to %s" % (url, final_url))
        urldict[final_url] = urldict[url]
        del urldict[url]
      else:
        expanded_url = url
        if len(url)<30 and url.count("/")<=4 and len(url)-url.rfind("/")>3:
          try:
            expanded_url = self.expand_shortened_url(url)
          except Exception, ex:
            try:
              logging.warn("Unable to expand %s due to %s" % (url, ex))
            except Exception, ex:
              logging.warn("Unable to log unable-to-expand due to %s" % ex)
            self.set_cache(url, url)

        parsed_url = urlparse.urlparse(expanded_url)
        final_url = parsed_url.scheme+"://"+parsed_url.netloc+parsed_url.path
        if len(parsed_url.params)>0:
          final_url += parsed_url.params
        if len(parsed_url.query)>0:
          queries = filter(lambda x:x.find("utm_")!=0, parsed_url.query.split("&"))
          queries = filter(lambda x:x!="ncid=rss", queries)
          queries = filter(lambda x:x!="", queries)
          if len(queries)>0:
            final_url += "?"+"&".join(queries)

        if "://"+url == final_url: final_url = url
        if final_url != url:
          urldict[final_url] = urldict[url]
          del urldict[url]
          self.set_cache(url, final_url)
          counter+=1
          if counter % 100==0:
            logging.info("Performing interim save")
            self.cache().put()
    return urldict

  def check_cache(self, url):
    cache = self.cache().data
    return cache[url] if url in cache else None
  
  def set_cache(self, url, final_url):
    logging.info("Resolved %s to %s" % (url, final_url))
    self.cache().data[url] = final_url
  
  def cache(self):
    if self.local_cache is None:
      self.local_cache = model.url_cache(self.request.get("key"))
    return self.local_cache

  def expand_shortened_url(self, url):
    #paywalls, etc
    if url.find("//nyti.ms/")>0 or url.find("//pastebin.com/")>0 or url.find("imgur.com")>0:
        return url
        
    #logging.info("Expanding %s" % url)
    final_url = url
    (counter, maxtries, location) = (0, 24, url)
    next_url = urlfetch.fetch(url, follow_redirects = False, deadline=10, headers = self.fetch_headers())
    while next_url.status_code>=300 and next_url.status_code<400 and counter < maxtries:
      counter+=1
      if 'location' in next_url.headers:
        location = next_url.headers['location'].encode("utf-8").replace(" ","%20")

        if location is not None and location.startswith("/"):
          parsed_url = urlparse.urlparse(url)
          location = parsed_url.scheme+"://"+parsed_url.netloc+location
          #logging.info("Relative redirect rewritten to: %s" % location)

        next_url = urlfetch.fetch(location, follow_redirects = False, headers = self.fetch_headers(), deadline=10)
  
      final_url = location if location is not None else url
    return final_url

  def fetch_headers(self):
    return {'User-Agent' : 'Vortext URL Expander', 'Referer': 'http://www.twitter.com/'}


class TwitterUnpacker(webapp2.RequestHandler):
  def post(self):
    key = self.request.get("key")
    upload = ndb.Key(urlsafe = key).get()
    account = upload.account_key.get()
    blob_reader = blobstore.BlobReader(upload.blob_key, buffer_size=524288)
    logging.info("Unpacking")

    logging.info("parsing archive files")
    raw = account.raw_uploaded_data()
    raw.data = []
    with zipfile.ZipFile(blob_reader, 'r') as myzip:
      for name in myzip.namelist():
        if name.startswith("data/js/tweets") and name.endswith(".js"):
          raw_json = myzip.read(name)
          raw_json = raw_json[raw_json.find("["):]
          statuses = parse_raw_twitter(raw_json)
          raw.data += statuses
    raw.put()
    
    account.update_status(model.STATUS_FILE_UNPACKED);
    taskqueue.add(queue_name='default',
                  url='/tasks/parse',
                  params={'key' : raw.key.urlsafe(), 'clean_urls' : 'true', 'send_email' : 'True'})
    logging.info("Unpacked from zip to datastore")
    memcache.delete("unpack_%s" % account.key.urlsafe())



app = webapp2.WSGIApplication([
    ('/tasks/fetch', TwitterFetcher),
    ('/tasks/parse', TwitterParser),
    ('/tasks/unpack', TwitterUnpacker),
    ('/tasks/expand', UrlExpander),
    ],
    debug=True)


def parse_raw_twitter(raw):
  tweets = json.loads(raw)
  mapped = map(parse_twitter_status, tweets)
  return [a for a in mapped if a is not None]

def parse_twitter_status(status):
  no_entities = len(status['entities'])==0 or len(status['entities']['urls'])==0
  text = status["text"]
  author = status['user']['screen_name']
  author = status['retweeted_status']['user']['screen_name'] if 'retweeted_status' in status else author

  if no_entities:
    if text.find("http:")<0 and text.find("https:")<0:
      return None
    result = ttp.Parser().parse(text)
    (mentions, urls, hashtags) = (result.users, result.urls, result.tags)
  else:
    urls = map(lambda x:x['expanded_url'], status['entities']['urls'])
    hashtags = map(lambda x:x['text'], status['entities']['hashtags'])
    mentions = map(lambda x:x['screen_name'], status['entities']['user_mentions'])
    mentions = [m for m in mentions if m!=author]
  
  propers = re.findall('[A-Z][a-z]+[\s-][A-Z][a-z.]*', text)
  for proper in propers:
    text=text.replace(proper,"")
  exclude = set(string.punctuation)
  propers = [''.join(ch for ch in p if ch not in exclude) for p in propers]

  words = text.split(" ")
  words = [w if w.isalpha() else w[0:-1]+" " for w in words] #handle words that end in periods, colons, etc.
  words = [w for w in words if w.isalpha() and not w.lower().encode('utf-8') in STOP_WORDS]
  words = propers + words

  vals = {
    'id' : status['id'],
    'time' : status['created_at'],
    'text' : status['text'],
    'urls' : urls,
    'hashtags' : hashtags,
    'mentions' : mentions,
    'author' : author,
    'keywords' : words,
  }
  return vals

def reverse_dict(old):
  new = {}
  if old is None: return new
  for key in old:
    old_vals = old[key]
    for val in old_vals:
      new[val] = new[val] + [key] if val in new else [key]
  return new
  

