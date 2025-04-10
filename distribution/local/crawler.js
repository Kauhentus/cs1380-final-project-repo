// distribution/local/crawler.js
const fs = require('fs');
const path = require('path');
const parse = require('node-html-parser').parse;

const cb = (e, v) => {
  if (e) {
    console.error(e);
  } else {
    console.log(v);
  }
};

let metrics = null;
let metricsInterval = null;
let stopWordsSet = null;

function initialize(callback) {
  callback = callback || cb;
  
  const crawlerDir = path.join('crawler-files');
  const metricsDir = path.join(crawlerDir, 'metrics');
  
  if (!fs.existsSync(crawlerDir)) fs.mkdirSync(crawlerDir, { recursive: true });
  if (!fs.existsSync(metricsDir)) fs.mkdirSync(metricsDir, { recursive: true });
  
  const metrics_file_path = path.join(metricsDir, `metrics-${global.nodeConfig.port}.json`);
  
  metrics = {
    crawling: {
      pagesProcessed: 0,
      totalCrawlTime: 0,
      bytesDownloaded: 0,
      termsExtracted: 0,
      avgProcessingTime: 0,
      targetsHit: 0
    },
    memory: {
      peaks: [],
      averages: []
    },
    startTime: Date.now()
  };
  
  stopWordsSet = getStopWordsSet();
  
  metricsInterval = setInterval(() => {
    const memUsage = process.memoryUsage();
    metrics.memory.peaks.push({
      timestamp: Date.now(),
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024)
    });
    
    fs.writeFileSync(metrics_file_path, JSON.stringify(metrics, null, 2));
  }, 60000); 
  
  const links_to_crawl_map = new Map();
  const crawled_links_map = new Map();

  global.distribution.local.mem.put(links_to_crawl_map, 'links_to_crawl_map', (e, v) => {
    global.distribution.local.mem.put(crawled_links_map, 'crawled_links_map', (e, v) => {
      global.distribution.local.store.get('links_to_crawl', (e1, v1) => {
        global.distribution.local.store.get('crawled_links', (e2, v2) => {
          if(!e1 && !e2 && v1 && v2) {
            const saved_links_to_crawl = v1.split('\n').filter(s => s.length > 0);
            const saved_crawled_links = v2.split('\n').filter(s => s.length > 0);
            saved_links_to_crawl.map(link => links_to_crawl_map.set(link, true));
            saved_crawled_links.map(link => crawled_links_map.set(link, true));
          }

          callback(null, {
            status: 'success',
            message: 'Crawler service initialized',
            links_to_crawl: links_to_crawl_map.size,
            crawled_links: crawled_links_map.size
          });
        });
      });
    });
  });
}

function add_link_to_crawl(link, callback) {
  callback = callback || cb;
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      if(links_to_crawl_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_in_queue' });
      if(crawled_links_map.has(link)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      links_to_crawl_map.set(link, true);
      callback(null, { status: 'success', message: 'Link added to crawl queue', link: link });
    });
  });
}

function getStopWordsSet() {
  return new Set([
        // Common English stop words (keep your original list)
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves",
        "you", "your", "yours", "yourself", "yourselves",
        "he", "him", "his", "himself", "she", "her", "hers", "herself",
        "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
        "what", "which", "who", "whom", "this", "that", "these", "those",
        "am", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "having", "do", "does", "did", "doing",
        "a", "an", "the", "and", "but", "if", "or", "because", "as",
        "until", "while", "of", "at", "by", "for", "with", "about",
        "against", "between", "into", "through", "during", "before",
        "after", "above", "below", "to", "from", "up", "down", "in",
        "out", "on", "off", "over", "under", "again", "further", "then",
        "once", "here", "there", "when", "where", "why", "how", "all",
        "any", "both", "each", "few", "more", "most", "other", "some",
        "such", "no", "nor", "not", "only", "own", "same", "so", "than",
        "too", "very", "s", "t", "can", "will", "just", "don", "should", "now",

        // Wiki structure and formatting elements
        "hlist", "ext", "child", "color", "vector", "skin", "margin", "lock", "background", 
        "font", "ready", "asbox", "first", "type", "navbar", "list", "false", "true", "last", 
        "none", "size", "feature", "limit", "left", "wikimedia", "display", "width", "padding", 
        "right", "top", "text", "value", "theme", "styles", "media", "border", "inherit", 
        "inline", "user", "main", "align", "lower", "group", "search", "important", "biota", 
        "center", "var", "transparent", "https", "img", "timeless", "function", "cookie", 
        "classname", "screen", "table", "articles", "minerva", "limited", "night", "commons",
        "sticky", "side", "even", "total", "inner", "hatnote", "bold", "italic", "wrap", "clear",
        "format", "selflink", "page", "wrap", "order", "visible", "float", "solid", "wide",
        "overflow", "hide", "empty", "relative", "auto", "depth", "full", "collapse", "split",
        "cursor", "hidden",

        // Wiki metadata terms
        "stub", "abbr", "name", "columns", "upload", "page", "svg", "title", "subgroup", 
        "error", "upper", "abovebelow", "tools", "hide", "counter", "enabled", "pinned", 
        "rlq", "sidebar", "listitem", "description", "weight", "repeat", "free",
        "alpha", "roman", "height", "ambox", "box", "move", "skins", "solid", "www",
        "short", "maint", "retrieved", "registration", "column", "appearance", "word", "site",
        "category", "stubshidden", "commons", "boxtext", "taxonbar", "taxonbars", "gallery",
        "note", "article", "one", "two", "three", "edit", "cite", "portal", "help", "special",
        "talk", "user", "module", "template", "tnc", "form", "ids", "see", "sources", "caps",
        "version", "long", "move", "vte", "open", "contents", "small", "png", "jpg", "add",
        "item", "items",

        // Time and date terms
        "utc", "november", "june", "july", "august", "september", "october", "december", 
        "january", "february", "march", "april", "day", "dates", "created", "retrieved",
        "archived",


        // References and citation terms
        "doi", "isbn", "pmid", "citation", "cite", "reference", "references", "info", "sources",
        "source", "statement", "footnote", "note", "notes", "publisher", "published", "press",
        "journal", "vol", "department", "university", "link", "links", "external", "pdf",
        "attribution", "online", "license", "creative", "sharealike", "information", "data",
        "checklist", "ipni", "database", "foundation", "royal", "society", "academic", "research",
        "web", "api", "itis", "gbif", "tropicos", "kew", "eol", "natureserve", "grin", "eppo",
        "microbank", "powo", "gardens", "speciesfungorum", "indexfungorum", "nzor",
        "mycobank", "wayback", "calflora", "rhs", "apni", "urn", "lsid", "field", "guide", "encyclopedia", "col", "production",
        "government", "org",

        // Wiki technical and UI terms
        "document", "categories", "replace", "window", "options", "centralnotice", "donate",
        "interlanguage", "globalcssjs", "desktoparticletarget", "gadget", "centralauth",
        "centralautologin", "eventlogging", "create", "log", "policy", "unstrip", "tree",
        "history", "kern", "greek", "float", "http", "available", "terms",
        "foundation", "account", "actions", "mobile", "mediawiki", "wikibase", "push",
        "changesupload", "projects", "privacy", "entity", "nowrap", "eqiad", "visualeditor",
        "init", "break", "subscription", "dark", "alt", "white", "prefers", "scheme",
        "limitreport", "pref", "may", "uls", "language", "print", "image", "base", "brackets",
        "normal", "decoration", "spacing", "mini", "position", "different", "red", "responsive",
        "plainlist", "view", "toggle", "inside", "icon", "logo", "avoid", "taxobox",
        "pages", "inc", "set", "readeditview", "taxonomy", "organization", "disabled", "client",
        "menu", "line", "code", "bottom", "context", "scribunto", "platform", "export",
        "contribute", "apply", "login", "logged", "model", "statistics", "personal", "bootstrap",
        "mmv", "mode", "start", "quick", "core", "editors", "learn", "general", "author",
        "privacy", "policy", "enabled", "disabled", "using", "make", "makecollapsible",

        // Measurement and quantity terms
        "cm", "mm", "inches", "meters", "metres", "high", "tall", "wide", "diameter", "maximum",
        "minimum", "min", "max", "limit", "length", "centimeters", "frac", "specific", "well",
        "static", "typically", "commonly", "sometimes", "often", "without", "secure",

        // Status and classification terms
        "concern", "secure",
        "iucn", "rlts", "least", "basionyms", "synonyms",
        "information", "delink", "original", "naming", 
        "described", "added", "expanded", "unsourced", "common", "like",

        // MediaWiki system variables and technical programming terms
        "wg", "wgtitle", "wgrevisionid", "wgisredirect", "wgpageviewlanguage", "wgbackendresponsetime", 
        "wghostname", "wgcurrevisionid", "wgarticleid", "wgisarticle", "wgusergroups", "wgcategories", 
        "wgpagecontentlanguage", "wgpagecontentmodel", "wgrelevantpagename", "wgisprobablyeditable", 
        "wgnoticeproject", "wgcitereferencepreviewsactive", "wgpopupsflags", "wgvisualeditor", 
        "wgmfdisplaywikibasedescriptions", "wgwmepagelength", "wgeditsubmitbuttonlabelpublish", 
        "wgulsiscompactlinksenabled", "wgwikibaseitemid", "wgcheckuserclienthintsheadersjsapi", 
        "wggelevelingupenabledforuser", "wgmediaviewerenabledbydefault", "wgwmeschemaeditattemptstepoversample", 
        "wgulsposition", "wgulsislanguageselectorempty", "wgbreakframes", "wgdigittransformtable", 
        "wgrequestid", "wgcanonicalnamespace", "wgaction", "wgusername", "wgrelevantarticleid", 
        "wgrelevantpageisprobablyeditable", "wgrestrictionedit", "wgrestrictionmove", "wgflaggedrevsparams", 
        "wgmediavieweronclick", "wgpageparsereport", "null", "function", "return", "foreach", 
        "documentelement", "target", "tags", "brands", "platformversion", "rlstate", "wikimediamessages", 
        "startup", "jquery", "popup", "increment", "developers", "ppvisitednodes", "templateargumentsize",
        "entityaccesscount", "cachereport", "timestamp", "transientcontent", "wmf", "custom",
        "match", "enwikimwclientpreferences", "regexp", "wgseparatortransformtable", "wgdefaultdateformat",
        "wgmonthnames", "wgcanonicalspecialpagename", "wgnamespacenumber", "wgpagename", "walltime",
        "expensivefunctioncount", "timeusage", "ttl", "pagelanguagecode", "pagevariantfallbacks",
        "watchlist", "tagline", "architecture", "bitness", "fullversionlist", "loading", "codex",
        "icons", "noscript", "rlpagemodules", "geoip", "toolbar", "popups", "targetloader",
        "echo", "wikimediaevents", "navigationtiming", "checkuser", "clienthints", "suggestededitsession",
        "loader", "impl", "tokens", "jump", "eventsrandom", "csrftoken", "navigation", "filepermanent", 
        "pageget", "urldownload", "download", "pdfprintable", "php", "descriptionshort", "trademark", 
        "topic", "schema", "mainentity", "imageobject", "datepublished", "cputime", "postexpandincludesize", 
        "expansiondepth", "timingprofile", "memusage", "sameas", "contributors", "campaigs", "hor", 
        "googpub", "datemodified", "headline", "config", "output", "parser", "navbox", "reflist", "content",
        "html", "infobox", "url", "wfo", "header", "toc", "named", "distribution", "names", "new", "subsp",
        "absolute", "world", "block", "authomatically", "also", "found", "known", "sistersitebox", "multiple",
        "ncbi", "src", "use", "shaped", "irmng", "non", "contact", "decimal", "images", "quicksurveys", "require",
        "doctype", "rlconf", "gehomepagesuggestededitsenabletopics", "wggetopicsmatchmodeenabled", "index", "wikidataarticles",
        "commonswikispecieswikidata", "john", "speciesbox", "ccf", "taxonrow", "contains", "meaning", "several", "slightly",
        "part", "less", "far", "gas", "foc", "study", "great", "five", "year", "made", "den", "row", "wginternalredirecttargeturl",
        "jstor", "wgredirectedfrom", "wggetopicsmatchmodeenabled", "wginternalredirecttargeturl", "wikidatataxonbars", "wikidatause",
        "wikidataarticles", "clientpref", "body", "fdfdfd", "space", "sizing", "pagelanguagedir", "referencetooltips", "switcher",
        "urlshortener", "growthexperiments", "enhancements", "clientpref", "additional", "smaller", "twinaray", "items","matches", "growing",
        "microformatscommons", "fna", "subtle", "subject", "occurs", "usually", "interactive", "many", "action", "nbn", "redirect", "apdb", "sdcat",
        "eds", "biolib", "photo", "gcc", "svenskawinaray", "sub", "ais", "around", "single", "issn", "due", "time", "identifiershakea", "include",
        "codfw", "refers", "jepson", "early", "throughout", "section", "either", "especially", "occasionally", "however", "listed", "expected", "bot", 
        "photos", "ending", "towards", "recognized", "cap", "nom", "elt", "clip", "service", "tro", "aaa", "flex", "formally", "occurs", "fna",

        // Wiki site references
        "wikimedia", "wikipedia", "wiki", "wikidata", "wiktionary", "wikisource", "wikispecies",
        "commons", "commonswikidata", "wikispecieswikidata", "cebuano", "cebuanosvenskati",

        // Language and internationalization
        "english", "latin", "french", "german", "languages", "spanish", "portuguese", "italian",
        "russian", "japanese", "chinese", "korean", "greek", "ltr", "dmy",

        // Additional relevant wiki related terms from the JSON
        "reset", "ddf", "odd", "useformat", "desktop", "oldid", "edited", "agree", "registered",
        "profit", "disclaimers", "conduct", "patroltoken", "watchtoken", "pagecontentscurrent", 
        "linkpage", "informationcite", "shortened", "articleabout", "wikipediacontact", "helplearn", 
        "editcommunity", "portalrecent", "filespecial", "contributionstalk", "articletalk", 
        "hererelated", "microformats", "microformatstaxonomy", "microformatsall", "microformatstaxonbars",
        "pagetype", "quotes", "rgba", "keyword", "wikitext", "interface", 

        // Format identifiers and general descriptors that don't add taxonomic value
        "bold", "italic", "size", "large", "small", "contain", "containing", "related", "similar",
        "used", "uses", "pink", "purple", "green", "blue", "yellow", "orange", "red", "brown",
        "black", "white", "gray", "grey", "smooth", "id", "style", "div", "span"
    ]);
}

function crawl_one(callback) {
  callback = callback || cb;
  const crawlStartTime = Date.now();
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {                        
      if(links_to_crawl_map.size === 0) return callback(null, { status: 'skipped', reason: 'no_links' });
      const [url, _] = links_to_crawl_map.entries().next().value;
      links_to_crawl_map.delete(url);
      if(crawled_links_map.has(url)) return callback(null, { status: 'skipped', reason: 'already_crawled' });

      fetch(`https://en.wikipedia.org${url}`)
        .then((response) => {
          const contentLength = response.headers.get('content-length') || 0;
          metrics.crawling.bytesDownloaded += parseInt(contentLength);
          return response.text();
        })
        .then((html) => {
          const root = parse(html);

          const biota = root.querySelector('table.infobox.biota');
          const biota_rows = biota?.querySelectorAll('tr');
        
          const hierarchy = biota_rows?.map((row) => {
            const td_data = row.querySelectorAll('td');
            if(td_data.length !== 2) return null;
          
            const label = td_data[0].text.trim().toLocaleLowerCase().slice(0, -1);
            const value = td_data[1].text.trim().toLocaleLowerCase();
            return [label, value];
          }).filter(item => item !== null);
        
          const binomial_name = biota?.querySelector('span.binomial')?.text?.trim().toLocaleLowerCase();
          
          const links_on_page = root.querySelectorAll('a').map(link => link.getAttribute('href'))
            .filter(link => link !== null && link !== undefined)
            .filter(link => link.startsWith('/wiki/'))
            .filter(link => !link.includes('.JPG'))
            .filter(link => !link.includes('.jpg'))
            .filter(link => !link.includes('.JPEG'))
            .filter(link => !link.includes('.jpeg'))
            .filter(link => !link.includes('.PNG'))
            .filter(link => !link.includes('.png'))
            .filter(link => !link.includes('#'))
            .filter(link => !link.includes(':'));
  
          const is_plant = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('plantae'));
          const is_fungi = hierarchy?.find(pair => pair[0] === 'kingdom' && pair[1].includes('fungi'));
          const is_sealife = hierarchy?.find(pair => pair[0] === 'phylum' && pair[1].includes('cnidaria'));
          const is_butterfly = hierarchy?.find(pair => pair[0] === 'order' && pair[1].includes('lepidoptera'));
          const is_target_class = is_plant || is_fungi || is_sealife || is_butterfly;
          
          let result = {
            status: 'success',
            url: url,
            is_target_class: !!is_target_class,
            links_found: links_on_page.length
          };
          
          if(is_target_class && binomial_name) {
            const page_text = root.text;
            const alphaOnlyPattern = /^[a-z]+$/;
            
            const all_words = (page_text.match(/\b\w+\b/g) || [])
              .map(word => word.toLocaleLowerCase())
              .filter(word => word.length > 2) // Filter out very short words
              .filter(word => alphaOnlyPattern.test(word)) // Only alphabetic words
              .filter(word => !stopWordsSet.has(word)); // Filter out stop words
            
            const wordCounts = new Map();
            for (const word of all_words) {
              wordCounts.set(word, (wordCounts.get(word) || 0) + 1);
            }
            
            const species_data = {
              hierarchy: hierarchy,
              binomial_name: binomial_name,
              url: url,
              // !! ONLY SENDING wordCounts instead of all words to reduce transfer size
              word_counts: Object.fromEntries(wordCounts)
            };
            
            const uncompressed_data = JSON.stringify(species_data);
            const dataSize = Buffer.byteLength(uncompressed_data, 'utf8');
            metrics.crawling.bytesTransferred += dataSize;
            metrics.crawling.termsExtracted += wordCounts.size;

            metrics.crawling.targetsHit += 1;

            global.distribution.local.groups.get('index', (err, indexGroup) => {
              if (!err && indexGroup) {
                if (global.distribution.index && global.distribution.index.indexer) {
                  global.distribution.index.indexer.index(species_data, (err, indexResult) => {                      
                  });
                  result.indexing = { status: 'called' }
                  processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
                } else {
                  result.indexing = { status: 'skipped', reason: 'indexer_not_available' };
                  processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
                }
              } else {
                result.indexing = { status: 'skipped', reason: 'index_group_not_available' };
                processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
              }
            });
          } else {
            result.indexing = { status: 'skipped', reason: 'not_target_or_no_binomial' };
            processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback);
          }
        })
        .catch((error) => {
          console.error(`ERROS: fetching ${url}:`, error);
          crawled_links_map.set(url, true);
          
          const crawlEndTime = Date.now();
          const crawlDuration = crawlEndTime - crawlStartTime;
          metrics.crawling.pagesProcessed++;
          metrics.crawling.totalCrawlTime += crawlDuration;
          
          callback(null, {
            status: 'error',
            url: url,
            error: error.message,
            duration_ms: crawlDuration
          });
        });
    });
  });
}

function processCrawlResult(url, links_on_page, result, crawlStartTime, is_target_class, callback) {
  global.distribution.local.mem.get('crawled_links_map', (e, crawled_links_map) => {
    crawled_links_map.set(url, true);

    global.distribution.local.groups.get('taxonomy', (e, v) => {
      if (e) {
        console.error('Error getting group info:', e);
        return callback(null, { 
          status: 'error', 
          error: 'Failed to get group info',
          url: url 
        });
      }

      const nodes = Object.values(v);
      const num_nodes = nodes.length;

      const get_nx = (link) => nodes[parseInt(global.distribution.util.id.getID(link).slice(0, 8), 16) % num_nodes];
      const new_links = [...new Set(is_target_class ? links_on_page : [])];

      let processed = 0;
      const total = new_links.length;
      
      if (total === 0) {
        finishCrawl();
        return;
      }

      new_links.forEach(link => {
        const remote = { node: get_nx(link), service: 'crawler', method: 'add_link_to_crawl'};
        global.distribution.local.comm.send([link], remote, (e, v) => {
          processed++;
          if (processed === total) {
            finishCrawl();
          }
        });
      });
      
      function finishCrawl() {
        const crawlEndTime = Date.now();
        const crawlDuration = crawlEndTime - crawlStartTime;
        metrics.crawling.pagesProcessed++;
        metrics.crawling.totalCrawlTime += crawlDuration;
        metrics.crawling.avgProcessingTime = 
          metrics.crawling.totalCrawlTime / metrics.crawling.pagesProcessed;
        
        result.duration_ms = crawlDuration;
        result.new_links_added = total;
        
        callback(null, result);
      }
    });
  });
}

function get_stats(callback) {
  callback = callback || cb;
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {

      const stats = {
        links_to_crawl: links_to_crawl_map.size,
        crawled_links: crawled_links_map.size,
        metrics: metrics
      };

      callback(null, stats);
    });
  });
}

function save_maps_to_disk(callback) {
  callback = callback || cb;
  
  global.distribution.local.mem.get('links_to_crawl_map', (e1, links_to_crawl_map) => {
    global.distribution.local.mem.get('crawled_links_map', (e2, crawled_links_map) => {
      const links_to_crawl_data = Array.from(links_to_crawl_map.keys()).join('\n');
      const crawled_links_data = Array.from(crawled_links_map.keys()).join('\n');

      global.distribution.local.store.put(links_to_crawl_data, 'links_to_crawl', (e, v) => {
        global.distribution.local.store.put(crawled_links_data, 'crawled_links', (e, v) => {
          callback(null, {
            status: 'success',
            links_to_crawl_saved: links_to_crawl_map.size,
            crawled_links_saved: crawled_links_map.size
          });
        });
      });
    });
  });
}

function cleanup(callback) {
  callback = callback || cb;
  
  if (metricsInterval) {
    clearInterval(metricsInterval);
    metricsInterval = null;
  }

  const metrics_file_path = path.join('crawler-files', 'metrics', `metrics-${global.nodeConfig.port}.json`);
  
  if (metrics) {
    metrics.endTime = Date.now();
    metrics.totalRuntime = (metrics.endTime - metrics.startTime) / 1000;
    
    fs.writeFileSync(metrics_file_path, JSON.stringify(metrics, null, 2));
  }

  save_maps_to_disk((err, result) => {
    callback(null, { 
      status: 'success', 
      metrics: metrics,
      saved_data: result
    });
  });
}

module.exports = {
  initialize,
  add_link_to_crawl,
  crawl_one,
  get_stats,
  save_maps_to_disk,
  cleanup
};