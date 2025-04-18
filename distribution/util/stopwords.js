// stop words were collected from 8k pages statistics and manual review
// removed words all had 1k > frequency and deemed not useful with peer review

module.exports = new Set([
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
    "bold", "italic", "size", "large", "small", "contain", "containing", "related", "similar", "used", "uses",  "smooth", "id", "style", "div", "span",

    "kingdom", "origin", "status", "auth", "campaigns", "loginwiki", "nearby", "scientific", "taxon", 
    "binomial", "classification", "genus", "levels", "species", "family", "inaturalist", "class", "domain", 
    "expanding", "stubs", "imageright", "lepindex", "including", "subsection", "fran", "called", "manual",
    "int", "along", "considered", "pmc", "although", "bokm", "needed", "redirected", "taxapad", "dicky", 
    "identifierseupithecia", "much", "statementsarticles", "com", "since", "volume", "later", "another"
]);