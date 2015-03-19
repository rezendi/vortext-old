import json, logging, os, random, time, urllib, webapp2
from google.appengine.api import memcache, taskqueue, users
from google.appengine.ext import blobstore
from google.appengine.ext.webapp import blobstore_handlers, template
import gaesessions
import model

class BaseHandler(webapp2.RequestHandler):
  def get(self):
    path = self.request.path
    components = path.split("/")
    self.page_values = {
      'mobile' : self.is_mobile(),
    }
    result = self.handle(components)
    if isinstance(result, str):
      self.message(result)
    elif result==True:
      self.write_response()
    elif not hasattr(self,"messaged"):
      self.message("Unable to parse request: %s" % path)

  def write_response(self): #TODO: finish mobile templates
    template_file = 'html/%s' % self.template if self.is_mobile() else 'html/%s' % self.template
    path = os.path.join(os.path.dirname(__file__), template_file)
    to_write = template.render(path, self.page_values)
    self.response.out.write(to_write)

  def message(self, message):
    if hasattr(self,"messaged"):
      return False
    path = os.path.join(os.path.dirname(__file__), 'mobile/message.html' if self.is_mobile() else 'html/message.html')
    self.response.out.write(template.render(path, {'message' : message}))
    self.messaged = True
    return False
  
  def is_admin(self):
    return users.is_current_user_admin()

  def current_account(self):
    session = gaesessions.get_current_session();
    return session.get("account");
  
  def set_account(self, key):
    session = gaesessions.get_current_session();
    session["account"]=key
  
  def is_mobile(self):
    if self.request.get("mobile")=="true":
      return True
    elif self.request.get("mobile")=="false":
      return False
    agent = self.request.headers["User-Agent"]
    return agent is not None and (agent.find("iPhone")>0 or agent.find("Android")>0 or agent.find("iPod")>0 or agent.find("Windows Phone")>0)


class StatusHandler(BaseHandler):
  def get(self):
    self.response.headers['Content-Type'] = 'application/json'
    session = gaesessions.get_current_session();
    key = session.get("account");
    if key is None:
      self.response.out.write('{"success":false,"error":"No session"}')
      return
    status = memcache.get("%s_status" % key)
    if status is None:
      account = model.account_for(key)
      status = account.status

    terminal = status==model.STATUS_CREATED or status % 1000 == model.STATUS_COMPLETE
    results = {'success':True, 'terminal':terminal, 'status': model.status_string(status)}
    self.response.out.write(json.dumps(results))


class ProfileHandler(BaseHandler):
  def handle(self, components):
    account = model.account_for(self.current_account())
    if self.is_admin() and len(self.request.get("handle"))>0:
      account = model.Account.query(model.Account.twitter_handle == self.request.get("handle")).get()

    if account is None:
      self.redirect("/")

    template_values = {
      'account' : account,
      'first_time' : self.request.get("new")=="true",
      'handle' : self.request.get("handle"),
      'admin' : self.is_admin(),
    }
    self.page_values.update(template_values)

    self.template = 'profile.html'
    return True

  def post(self):
    account = model.account_for(self.current_account())
    if self.is_admin() and len(self.request.get("handle"))>0:
      account = model.Account.query(model.Account.twitter_handle == self.request.get("handle")).get()

    logging.info("Saving changes")
    if account is not None:
      account.privacy = 1 if len(self.request.get("privacy"))>0 else 0
      account.name = self.request.get("name")
      account.email = self.request.get("email")
      if self.is_admin():
        account.status = int(self.request.get("status"))
      account.put()
    self.redirect("/")


class UtilHandler(BaseHandler):
  def get(self):
    return "done"


class RefetchHandler(BaseHandler):
  def handle(self, components):
    account = model.account_for(self.current_account())
    if account is None:
      self.redirect("/")

    time.sleep(random.random()) #prevent double fetches, which happen locally
    existing = memcache.get("fetch_%s" % account.key.urlsafe())
    if existing is not None:
      self.redirect("/")

    memcache.set("fetch_%s" % account.key.urlsafe(), model.now(), 36000)
    first_time = account.status==model.STATUS_CREATED
    account.update_status(model.STATUS_FETCH_INITIATED)
    raw = model.RawData(account_key = account.key, source="twitter", data = [])
    raw.data = []
    raw.put()
    taskqueue.add(queue_name='default',
                  url='/tasks/fetch',
                  params={'key' : raw.key.urlsafe(), 'send_email' : str(first_time)})
    self.redirect("/profile?new=true") if first_time else self.redirect("me")


class ReparseHandler(BaseHandler):
  def handle(self, components):
    if len(components)>2 and self.is_admin():
      logging.info("Trying to reparse %s" % components[2])
      account = model.Account.query(model.Account.twitter_handle == components[2]).get()
    else:
      account = model.account_for(self.current_account())

    if account is None:
      self.redirect("/")

    logging.info("Reparsing %s" % account.twitter_handle)
    key_str = account.key.urlsafe()
    existing = memcache.get("parse_%s" % key_str)
    if existing is not None:
      self.redirect("/")

    memcache.set("parse_%s" % key_str, model.now(), 36000)
    account.update_status(model.STATUS_PARSE_URLS);
    raw = account.newest_raw_data()
    taskqueue.add(queue_name='default',
                  url='/tasks/parse',
                  params={'key' : raw.key.urlsafe(),
                          'clean_urls' : self.request.get("clean_urls"),
                          'clean_all' : self.request.get("clean_all"),
                          }
                  )
    self.redirect("/me")


class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  def get(self):
    session = gaesessions.get_current_session();
    account = model.account_for(session.get("account"))
    if account is None:
      self.redirect("/")

    upload_url = blobstore.create_upload_url('/upload')
    page_values = {
      'handle' : account.twitter_handle,
      'upload_url' : upload_url,
    }
    path = os.path.join(os.path.dirname(__file__), "html/upload.html")
    to_write = template.render(path, page_values)
    self.response.out.write(to_write)

  def post(self):
    session = gaesessions.get_current_session();
    account = model.account_for(session.get("account"))
    if account is None:
      self.redirect("/")

    upload_files = self.get_uploads('file')  # 'file' is file upload field in the form
    blob_info = upload_files[0]
    upload = model.UploadedFile(account_key = account.key, source="twitter", blob_key = blob_info.key())
    upload.put()
    taskqueue.add(queue_name='unpacker',
                  url='/tasks/unpack',
                  params={'key' : upload.key.urlsafe()}
                  )
    self.response.out.write("success")


class ReunpackHandler(BaseHandler):
  def handle(self, components):
    account = model.account_for(self.current_account())
    if account is None:
      self.redirect("/")

    key_str = account.key.urlsafe()
    existing = memcache.get("unpack_%s" % key_str)
    if existing is not None:
      return "Already unpacking, started at %s" % existing

    memcache.set("unpack_%s" % key_str, model.now(), 36000)
    upload = account.newest_upload()
    account.update_status(model.STATUS_FILE_UPLOADED);
    taskqueue.add(queue_name='unpacker',
                  url='/tasks/unpack',
                  params={'key' : upload.key.urlsafe()}
                  )
    self.redirect("/me")


class HomeHandler(BaseHandler):
  def handle(self, components):
    account = model.account_for(self.current_account())
    if account is not None:
      self.redirect("/me")

    self.template = 'home.html'
    return True


class MainHandler(BaseHandler):
  def handle(self, components):
    (handle, year, has_year, month, has_month, category, term) = ('me', None, False, None, False, None, None)
    args = self.parse_components(components)

    if len(components)>1:
      handle = components[1]
    if len(components)>2 and components[2].isdigit():
      year = int(components[2])
      has_year = True
    if has_year and len(components)>3 and components[3].isdigit():
      month = int(components[3])
      has_month = True
    if len(components)>3 and not has_year:
      category = components[2]
      term = components[3]
    if len(components)>4 and has_year and not has_month:
      category = components[3]
      term = components[4]
    if len(components)>5 and has_year and has_month:
      category = components[4]
      term = components[5]
      year = int(components[2])

    if handle == 'me':
      if len(self.request.get("v"))>0:
        key = memcache.get(self.request.get("v"))
        self.set_account(key)
        memcache.delete(self.request.get("v"))
        self.redirect("/refetch")

      account = model.account_for(self.current_account())
      if account is None:
        logging.info("redirect for handle=me, components %s" % components)
        self.redirect("/")

    else:
      account = model.Account.query(model.Account.twitter_handle == handle).get()
      if account is not None and account.key.urlsafe()!=self.current_account() and account.is_private():
        return "That user's Vortext is private."
    
    if account is None:
      return "That user has not yet activated their Vortext."

    status = memcache.get("%s_status" % account.key.urlsafe())
    status = account.status if status is None else status
    terminal_status = status is None or status==model.STATUS_CREATED or status % 1000 == model.STATUS_COMPLETE
    template_values = {
      'user_status' : model.status_string(status),
      'fetching' : status is None or status < model.STATUS_FETCH_COMPLETE,
      'terminal_status' : 'true' if terminal_status else 'false',
      'tweets_per_scroll' : 10,
      'autodata' : '[]',
      'status_json' : '[]',
      'args' : args,
    }
    self.page_values.update(template_values)


    if account.timeline is not None and account.keywords is not None:
      timeline = account.timeline
      keywords = account.keywords
      if len(timeline)==0:
        statuses = []
      else:
        if term is not None:
          term = urllib.unquote(term)
          collections = keywords if category=="all" or category=="search" else filter(lambda x:x[0]==category, keywords)
          term_tweets = []
          for collection in collections:
            vals = collection[1]
            #handle multiple entries with different case
            if category=="search":
              terms = filter(lambda x:x.lower().startswith(term.lower()), vals.keys())
            else:
              terms = filter(lambda x:x.lower()==term.lower(), vals.keys())
            category_tweets = [vals[t] for t in terms]
            category_tweets = reduce(list.__add__, category_tweets, [])
            term_tweets += category_tweets
  
        if year is not None:
          months = filter(lambda x:x['number']==year, timeline)[0]['months']
          if month is not None:
            month_ids = filter(lambda x:x['number']==month, months)[0]
            time_tweets = month_ids['ids']
          else:
            month_ids = [m['ids'] for m in months]
            time_tweets = reduce(list.__add__, month_ids, [])
  
        if year is None and term is None:
          month_ids = [m['ids'] for m in timeline[0]['months']]
          statuses = reduce(list.__add__, month_ids, [])
        elif term is None and year is not None:
          statuses = time_tweets
        elif year is None and term is not None:
          statuses = term_tweets
        else:
          statuses = list(set(term_tweets).intersection(set(time_tweets)))
      

      if args.mytweets or args.retweets:
        authors = filter(lambda x:x[0]=="author", keywords)[0][1]
        authored = [tweetid for author in authors.values() for tweetid in author]
        if args.mytweets:
          statuses = list(set(statuses).difference(set(authored)))
        else:
          statuses = list(set(statuses).intersection(set(authored)))

      statuses.sort(key = lambda x:long(x))
      if args.reverse:
        statuses.reverse()

      keywords = self.collect(keywords)
  
      template_values = {
        'self' : self.current_account()==account.key.urlsafe(),
        'handle' : account.twitter_handle,
        'timeline' : timeline,
        'autodata' : json.dumps(keywords),
        'default_year' : timeline[0]['number'] if len(timeline)>0 else 2015,
        'selected_year' : year,
        'selected_month' : month,
        'month_name' : '' if month is None else model.MONTHS[month],
        'category' : category,
        'term' : term,
        'statuses' : statuses,
        'status_json' : json.dumps(statuses),
        'show_args' : args.reverse or args.cards or args.conversations or args.mytweets or args.retweets,
      }
      self.page_values.update(template_values)

    self.template = 'main.html'
    return True

  def collect(self, keywords):
    dict_of_lists = {}
    for keyword_list in keywords:
      dict_of_lists[keyword_list[0]] = sorted(keyword_list[1].keys())
    return dict_of_lists
  
  def parse_components(self, components):
    args = model.DictObj()
    args.mytweets = "mytweets" in components
    if args.mytweets: components.remove("mytweets")

    args.retweets = "retweets" in components
    if args.retweets: components.remove("retweets")

    args.forward = "forward" in components or not "reverse" in components
    if "forward" in components: components.remove("forward")

    args.reverse = "reverse" in components
    if args.reverse:
      components.remove("reverse")
      args.forward = not args.reverse

    args.cards = "cards" in components
    if args.cards: components.remove("cards")

    args.conversations = "conversations" in components
    if args.conversations: components.remove("conversations")

    return args


app = webapp2.WSGIApplication([
    ('/', HomeHandler),
    ('/status', StatusHandler),
    ('/profile', ProfileHandler),
    ('/admin/profile', ProfileHandler),
    ('/upload', UploadHandler),
    ('/refetch.*', RefetchHandler),
    ('/reparse.*', ReparseHandler),
    ('/reunpack.*', ReunpackHandler),
    ('/admin/util', UtilHandler),
    ('/.*', MainHandler),
], debug=True)

