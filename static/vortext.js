var status_delay = 2000;

var refreshStatus = function() {
  $.getJSON("status", function(data) {
    var now_expanding = false;
    if (data['success']) {
      terminal_status = data['terminal'];
      now_expanding = data['status'].indexOf("Expanding")==0 && $("#status_text").text().indexOf("Expanding")!=0;
      now_fetching = data['status'].indexOf("Fetched")==0 && $("#status_text").text().indexOf("Fetched")!=0;
      $("#status_text").text(data['status']);
    }
    if (is_self && (terminal_status || now_expanding || now_fetching)) {
      window.location = "/me"
    }
    else {
      status_delay = status_delay + 1000;
      setTimeout(refreshStatus, status_delay);
    }
  }).fail(function(xhr) {
    console.log("error "+JSON.stringify(xhr));
  });
}

var url_append = function(term, opposite) {
  var newloc = ""+window.location;
  newloc = newloc.replace("/"+term, "");
  newloc = newloc.replace("/"+opposite,"");
  newloc += "/"+term
  window.location=newloc;
}

var my_re = function() {
  if ($("#my_tweets").is(":checked") && !$("#re_tweets").is(":checked")) {
    url_append("mytweets","retweets");
  }
  else if ($("#re_tweets").is(":checked") && !$("#my_tweets").is(":checked")) {
    url_append("retweets","mytweets");
  }
  else {
    var newloc = ""+window.location;
    newloc = newloc.replace("/mytweets", "");
    newloc = newloc.replace("/retweets","");
    window.location=newloc;
  }
}

var filter_autocomplete = function(searchTerm) {
  var matchingItems = [];
  if (searchTerm.length<2) {
    return matchingItems;
  }
  var lower = searchTerm.toLowerCase();
  _.each(["author","keyword","hashtag","mention","site"], function(category) {
    _.each(autodata[category], function(term) {
      if (term.toLowerCase().indexOf(searchTerm) >= 0 || category.indexOf(searchTerm) >= 0) {
        var item = {};
        item['label'] = '' + category + ' | ' + term;
        item['value'] = term;
        matchingItems.push(item);
      }
    });
  });

  if (matchingItems.length === 0)
    return ['No Matches'];
  return matchingItems;
}

var toggle_options = function() {
  $("#options_container").toggle("slide", { direction: 'up' }, 250);
}
