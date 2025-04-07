const distribution = require("./config.js");
const id = distribution.util.id;
const fs = require("fs");
const os = require("os");
const path = require("path");

// Set up nodes for distributed processing
const num_nodes = 4;
const nodes = [];
const nids = [];
const testGroup = {};
const indexGroup = {};

// Define separate groups for different data types
const tfidfConfig = { gid: "tfidf" }; // For document data
const indexConfig = { gid: "index" }; // For term data

function isEmptyObject(obj) {
  return obj && typeof obj === "object" && Object.keys(obj).length === 0;
}

for (let i = 0; i < num_nodes; i++) {
  const nodeConfig = { ip: "127.0.0.1", port: 7112 + i };
  nodes.push(nodeConfig);
  nids.push(id.getNID(nodeConfig));

  // Add node to both groups
  const sid = id.getSID(nodeConfig);
  testGroup[sid] = nodeConfig;
  indexGroup[sid] = nodeConfig;
}

// Configuration
const CONFIG = {
  // Processing config
  BATCH_SIZE: 10, // Number of keys each node processes per batch
  MAX_EMPTY_BATCHES: 4, // Stop after this many consecutive empty batches
  PROCESS_CHUNK_SIZE: 10000, // Chunk size for processing terms
  SAVE_REFERENCE_COPY: true, // Whether to save a reference copy of the full index

  // Index structure config
  USE_DISTRIBUTED_STORE: true, // Whether to use the distributed key-value store
  STORE_DURING_PROCESSING: false, // Store terms immediately during processing or after

  // Group configuration
  TERM_GROUP: "index", // Group for storing terms
  DOC_GROUP: "tfidf", // Group for storing documents

  // Key prefixes
  INDEX_PREFIX: "term:", // Prefix for index keys in the distributed store
  DOC_PREFIX: "doc:", // Prefix for document keys in the distributed store

  // Output directories
  RESULTS_DIR: "./tfidf-results", // Directory for results
  BATCHES_DIR: "./tfidf-results/batches",

  // Inverted index config
  MAX_DOCS_PER_TERM: 500, // Maximum number of documents to store per term (reduced from 1000)
  MAX_TERMS_PER_DOC: 50, // Maximum number of top terms to store per document (reduced from 100)

  // Search optimization config
  CREATE_SHARDS: true, // Whether to create alphabet-based shards
  SHARD_SIZE: 2, // Number of letters per shard

  // Recovery config
  CHECKPOINT_INTERVAL: 10, // Save checkpoint every N batches
  RESUME_ENABLED: true, // Enable resuming from a checkpoint

  // Performance monitoring
  PERF_LOG_INTERVAL: 5, // Log performance stats every N batches
  MEMORY_MONITORING: true, // Monitor memory usage

  // Error handling
  MAX_RETRIES: 3, // Maximum number of retries for failed operations
  RETRY_DELAY: 1000, // Delay in ms between retries
};

// Performance tracking
const PERF = {
  startTime: Date.now(),
  batchTimes: [],
  throughput: [],
  memoryUsage: [],
  errors: 0,
  lastCheckpoint: 0,
};

// Helper function to format time
function formatTime(ms) {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(2)}m`;
  return `${(ms / 3600000).toFixed(2)}h`;
}

// Helper function to format bytes
function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(2)}KB`;
  if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(2)}MB`;
  return `${(bytes / 1073741824).toFixed(2)}GB`;
}

// Helper to log performance stats
function logPerformanceStats(batchIndex) {
  if (!CONFIG.PERF_LOG_INTERVAL || batchIndex % CONFIG.PERF_LOG_INTERVAL !== 0)
    return;

  const currentTime = Date.now();
  const elapsedTime = currentTime - PERF.startTime;
  const avgBatchTime = PERF.batchTimes.length
    ? PERF.batchTimes.reduce((a, b) => a + b, 0) / PERF.batchTimes.length
    : 0;

  const lastNBatches = Math.min(
    CONFIG.PERF_LOG_INTERVAL,
    PERF.batchTimes.length
  );
  const recentBatchTimes = PERF.batchTimes.slice(-lastNBatches);
  const recentAvgTime =
    recentBatchTimes.reduce((a, b) => a + b, 0) / recentBatchTimes.length;

  const avgThroughput = PERF.throughput.length
    ? PERF.throughput.reduce((a, b) => a + b, 0) / PERF.throughput.length
    : 0;

  console.log("\n===== PERFORMANCE STATISTICS =====");
  console.log(`Elapsed time: ${formatTime(elapsedTime)}`);
  console.log(`Average batch processing time: ${formatTime(avgBatchTime)}`);
  console.log(
    `Recent average (last ${lastNBatches} batches): ${formatTime(
      recentAvgTime
    )}`
  );
  console.log(`Average throughput: ${avgThroughput.toFixed(2)} items/sec`);

  if (CONFIG.MEMORY_MONITORING) {
    const memoryUsage = process.memoryUsage();
    console.log(
      `Memory - RSS: ${formatBytes(memoryUsage.rss)}, Heap: ${formatBytes(
        memoryUsage.heapUsed
      )}/${formatBytes(memoryUsage.heapTotal)}`
    );
    PERF.memoryUsage.push({
      timestamp: Date.now(),
      rss: memoryUsage.rss,
      heapUsed: memoryUsage.heapUsed,
      heapTotal: memoryUsage.heapTotal,
    });
  }

  // Estimate remaining time if we have processed batches
  if (batchIndex > 0 && PERF.batchTimes.length > 0) {
    const totalBatches = CONFIG.ESTIMATED_TOTAL_BATCHES || 1000; // Fallback if not set
    const remainingBatches = totalBatches - batchIndex;
    const estimatedTimePerBatch = recentAvgTime || avgBatchTime;
    const estimatedTimeRemaining = remainingBatches * estimatedTimePerBatch;

    console.log(
      `Estimated remaining time: ${formatTime(estimatedTimeRemaining)}`
    );
    console.log(
      `Progress: ${((batchIndex / totalBatches) * 100).toFixed(
        2
      )}% (${batchIndex}/${totalBatches} batches)`
    );
  }

  if (PERF.errors > 0) {
    console.log(`Errors encountered: ${PERF.errors}`);
  }

  console.log("=====================================\n");
}

// Save performance data to a file
function savePerformanceData() {
  const perfData = {
    startTime: PERF.startTime,
    endTime: Date.now(),
    totalRuntime: Date.now() - PERF.startTime,
    batchTimes: PERF.batchTimes,
    throughput: PERF.throughput,
    memoryUsage: PERF.memoryUsage,
    errors: PERF.errors,
    config: {
      batchSize: CONFIG.BATCH_SIZE,
      nodes: num_nodes,
      maxDocsPerTerm: CONFIG.MAX_DOCS_PER_TERM,
      maxTermsPerDoc: CONFIG.MAX_TERMS_PER_DOC,
    },
  };

  const perfDir = path.join(CONFIG.RESULTS_DIR, "performance");
  if (!fs.existsSync(perfDir)) {
    fs.mkdirSync(perfDir, { recursive: true });
  }

  const perfFile = path.join(
    perfDir,
    `perf_${new Date().toISOString().replace(/:/g, "-")}.json`
  );
  fs.writeFileSync(perfFile, JSON.stringify(perfData, null, 2));
  console.log(`Performance data saved to ${perfFile}`);
}

// Check for recovery data
function checkRecovery(resultsDir) {
  if (!CONFIG.RESUME_ENABLED) return null;

  const batchesFile = path.join(resultsDir, "successful-batches.json");

  if (fs.existsSync(batchesFile)) {
    try {
      const batches = JSON.parse(fs.readFileSync(batchesFile, "utf8"));
      if (batches && batches.length > 0) {
        const lastBatch = batches[batches.length - 1];
        return {
          lastBatchIndex: lastBatch.batchIndex,
          batchesProcessed: batches.length,
          successfulBatches: batches,
        };
      }
    } catch (err) {
      console.error(`Error reading recovery data: ${err.message}`);
    }
  }

  return null;
}

// Helper function to create a checkpoint
function createCheckpoint(batchIndex, successfulBatches, resultsDir) {
  if (
    !CONFIG.CHECKPOINT_INTERVAL ||
    batchIndex % CONFIG.CHECKPOINT_INTERVAL !== 0
  )
    return;

  const checkpointDir = path.join(resultsDir, "checkpoints");
  if (!fs.existsSync(checkpointDir)) {
    fs.mkdirSync(checkpointDir, { recursive: true });
  }

  const checkpointFile = path.join(
    checkpointDir,
    `checkpoint_${batchIndex}.json`
  );
  const checkpointData = {
    batchIndex,
    timestamp: Date.now(),
    successfulBatches,
    performance: {
      elapsedTime: Date.now() - PERF.startTime,
      batchTimes: PERF.batchTimes,
      throughput: PERF.throughput,
      errors: PERF.errors,
    },
  };

  fs.writeFileSync(checkpointFile, JSON.stringify(checkpointData, null, 2));
  console.log(`Created checkpoint at batch ${batchIndex} (${checkpointFile})`);
  PERF.lastCheckpoint = batchIndex;
}

// Main function to run the TF-IDF calculation
distribution.node.start(async (server) => {
  PERF.startTime = Date.now();
  console.log("SETTING UP OPTIMIZED TF-IDF TEST NODE...");

  // Helper function to spawn a node
  const spawn_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.status.spawn(node, (e, v) => {
        console.log(
          `Spawned node at ${node.ip}:${
            node.port
          } ${distribution.util.id.getNID(node)} with result:`,
          e ? e : v
        );
        resolve(e, v);
      })
    );

  // Helper function to stop a node
  const stop_node = (node) =>
    new Promise((resolve, reject) =>
      distribution.local.comm.send(
        [],
        { service: "status", method: "stop", node: node },
        (e, v) => resolve(e, v)
      )
    );

  // Start the nodes
  for (let i = 0; i < nodes.length; i++) {
    const node = nodes[i];
    try {
      await spawn_node(node);
      console.log(`Node started at ${node.ip}:${node.port}`);
    } catch (e) {
      console.error(`Failed to start node at ${node.ip}:${node.port}`, e);
      finish();
      return;
    }
  }

  // Set up the TFIDF group
  distribution.local.groups.put(tfidfConfig, testGroup, (e, v) => {
    if (e && !isEmptyObject(e)) {
      console.error("Error setting up TFIDF group:", e);
      finish();
      return;
    }

    console.log("TFIDF group set up successfully");

    // Set up the INDEX group
    distribution.local.groups.put(indexConfig, indexGroup, (e, v) => {
      if (e && !isEmptyObject(e)) {
        console.error("Error setting up INDEX group:", e);
        finish();
        return;
      }

      console.log("INDEX group set up successfully");

      // Set up the group in TFIDF service
      console.log(
        "Setting up TFIDF service group with the following configuration:",
        tfidfConfig,
        testGroup
      );
      distribution.tfidf.groups.put(tfidfConfig, testGroup, (e, v) => {
        if (e && !isEmptyObject(e)) {
          console.error("Error setting up TFIDF service group:", e);
          finish();
          return;
        }

        console.log(
          "TFIDF service group set up successfully, starting TF-IDF calculation..."
        );

        // Define the mapper function
        // This processes each document and emits word -> [doc, count] pairs
        const mapper = function (key, value) {
          try {
            // Expanded stop list combining standard English stop words (inspired by NLTK)
            // with additional HTML/wiki-specific tokens that are common in Wikipedia pages.
            const stopWords = new Set([
              // Common English stop words (keep your original list)
              "i",
              "me",
              "my",
              "myself",
              "we",
              "our",
              "ours",
              "ourselves",
              "you",
              "your",
              "yours",
              "yourself",
              "yourselves",
              "he",
              "him",
              "his",
              "himself",
              "she",
              "her",
              "hers",
              "herself",
              "it",
              "its",
              "itself",
              "they",
              "them",
              "their",
              "theirs",
              "themselves",
              "what",
              "which",
              "who",
              "whom",
              "this",
              "that",
              "these",
              "those",
              "am",
              "is",
              "are",
              "was",
              "were",
              "be",
              "been",
              "being",
              "have",
              "has",
              "had",
              "having",
              "do",
              "does",
              "did",
              "doing",
              "a",
              "an",
              "the",
              "and",
              "but",
              "if",
              "or",
              "because",
              "as",
              "until",
              "while",
              "of",
              "at",
              "by",
              "for",
              "with",
              "about",
              "against",
              "between",
              "into",
              "through",
              "during",
              "before",
              "after",
              "above",
              "below",
              "to",
              "from",
              "up",
              "down",
              "in",
              "out",
              "on",
              "off",
              "over",
              "under",
              "again",
              "further",
              "then",
              "once",
              "here",
              "there",
              "when",
              "where",
              "why",
              "how",
              "all",
              "any",
              "both",
              "each",
              "few",
              "more",
              "most",
              "other",
              "some",
              "such",
              "no",
              "nor",
              "not",
              "only",
              "own",
              "same",
              "so",
              "than",
              "too",
              "very",
              "s",
              "t",
              "can",
              "will",
              "just",
              "don",
              "should",
              "now",

              // Wiki structure and formatting elements
              "hlist",
              "ext",
              "child",
              "color",
              "vector",
              "skin",
              "margin",
              "lock",
              "background",
              "font",
              "ready",
              "asbox",
              "first",
              "type",
              "navbar",
              "list",
              "false",
              "true",
              "last",
              "none",
              "size",
              "feature",
              "limit",
              "left",
              "wikimedia",
              "display",
              "width",
              "padding",
              "right",
              "top",
              "text",
              "value",
              "theme",
              "styles",
              "media",
              "border",
              "inherit",
              "inline",
              "user",
              "main",
              "align",
              "lower",
              "group",
              "search",
              "important",
              "biota",
              "center",
              "var",
              "transparent",
              "https",
              "img",
              "timeless",
              "function",
              "cookie",
              "classname",
              "screen",
              "table",
              "articles",
              "minerva",
              "limited",
              "night",
              "commons",
              "sticky",
              "side",
              "even",
              "total",
              "inner",
              "hatnote",
              "bold",
              "italic",
              "wrap",
              "clear",
              "format",
              "selflink",
              "page",
              "wrap",
              "order",
              "visible",
              "float",
              "solid",
              "wide",
              "overflow",
              "hide",
              "empty",
              "relative",
              "auto",
              "depth",
              "full",
              "collapse",
              "split",
              "cursor",
              "hidden",

              // Wiki metadata terms
              "stub",
              "abbr",
              "name",
              "columns",
              "upload",
              "page",
              "svg",
              "title",
              "subgroup",
              "error",
              "upper",
              "abovebelow",
              "tools",
              "hide",
              "counter",
              "enabled",
              "pinned",
              "rlq",
              "sidebar",
              "listitem",
              "description",
              "weight",
              "repeat",
              "free",
              "alpha",
              "roman",
              "height",
              "ambox",
              "box",
              "move",
              "skins",
              "solid",
              "www",
              "short",
              "maint",
              "retrieved",
              "registration",
              "column",
              "appearance",
              "word",
              "site",
              "category",
              "stubshidden",
              "commons",
              "boxtext",
              "taxonbar",
              "taxonbars",
              "gallery",
              "note",
              "article",
              "one",
              "two",
              "three",
              "edit",
              "cite",
              "portal",
              "help",
              "special",
              "talk",
              "user",
              "module",
              "template",
              "tnc",
              "form",
              "ids",
              "see",
              "sources",
              "caps",
              "version",
              "long",
              "move",
              "vte",
              "open",
              "contents",
              "small",
              "png",
              "jpg",
              "add",
              "item",
              "items",

              // Time and date terms
              "utc",
              "november",
              "june",
              "july",
              "august",
              "september",
              "october",
              "december",
              "january",
              "february",
              "march",
              "april",
              "day",
              "dates",
              "created",
              "retrieved",
              "archived",

              // References and citation terms
              "doi",
              "isbn",
              "pmid",
              "citation",
              "cite",
              "reference",
              "references",
              "info",
              "sources",
              "source",
              "statement",
              "footnote",
              "note",
              "notes",
              "publisher",
              "published",
              "press",
              "journal",
              "vol",
              "department",
              "university",
              "link",
              "links",
              "external",
              "pdf",
              "attribution",
              "online",
              "license",
              "creative",
              "sharealike",
              "information",
              "data",
              "checklist",
              "ipni",
              "database",
              "foundation",
              "royal",
              "society",
              "academic",
              "research",
              "web",
              "api",
              "itis",
              "gbif",
              "tropicos",
              "kew",
              "eol",
              "natureserve",
              "grin",
              "eppo",
              "microbank",
              "powo",
              "gardens",
              "speciesfungorum",
              "indexfungorum",
              "nzor",
              "mycobank",
              "wayback",
              "calflora",
              "rhs",
              "apni",
              "urn",
              "lsid",
              "field",
              "guide",
              "encyclopedia",
              "col",
              "production",
              "government",
              "org",

              // Wiki technical and UI terms
              "document",
              "categories",
              "replace",
              "window",
              "options",
              "centralnotice",
              "donate",
              "interlanguage",
              "globalcssjs",
              "desktoparticletarget",
              "gadget",
              "centralauth",
              "centralautologin",
              "eventlogging",
              "create",
              "log",
              "policy",
              "unstrip",
              "tree",
              "history",
              "kern",
              "greek",
              "float",
              "http",
              "available",
              "terms",
              "foundation",
              "account",
              "actions",
              "mobile",
              "mediawiki",
              "wikibase",
              "push",
              "changesupload",
              "projects",
              "privacy",
              "entity",
              "nowrap",
              "eqiad",
              "visualeditor",
              "init",
              "break",
              "subscription",
              "dark",
              "alt",
              "white",
              "prefers",
              "scheme",
              "limitreport",
              "pref",
              "may",
              "uls",
              "language",
              "print",
              "image",
              "base",
              "brackets",
              "normal",
              "decoration",
              "spacing",
              "mini",
              "position",
              "different",
              "red",
              "responsive",
              "plainlist",
              "view",
              "toggle",
              "inside",
              "icon",
              "logo",
              "avoid",
              "taxobox",
              "pages",
              "inc",
              "set",
              "readeditview",
              "taxonomy",
              "organization",
              "disabled",
              "client",
              "menu",
              "line",
              "code",
              "bottom",
              "context",
              "scribunto",
              "platform",
              "export",
              "contribute",
              "apply",
              "login",
              "logged",
              "model",
              "statistics",
              "personal",
              "bootstrap",
              "mmv",
              "mode",
              "start",
              "quick",
              "core",
              "editors",
              "learn",
              "general",
              "author",
              "privacy",
              "policy",
              "enabled",
              "disabled",
              "using",
              "make",
              "makecollapsible",

              // Measurement and quantity terms
              "cm",
              "mm",
              "inches",
              "meters",
              "metres",
              "high",
              "tall",
              "wide",
              "diameter",
              "maximum",
              "minimum",
              "min",
              "max",
              "limit",
              "length",
              "centimeters",
              "frac",
              "specific",
              "well",
              "static",
              "typically",
              "commonly",
              "sometimes",
              "often",
              "without",
              "secure",

              // Status and classification terms
              "concern",
              "secure",
              "iucn",
              "rlts",
              "least",
              "basionyms",
              "synonyms",
              "information",
              "delink",
              "original",
              "naming",
              "described",
              "added",
              "expanded",
              "unsourced",
              "common",
              "like",

              // MediaWiki system variables and technical programming terms
              "wg",
              "wgtitle",
              "wgrevisionid",
              "wgisredirect",
              "wgpageviewlanguage",
              "wgbackendresponsetime",
              "wghostname",
              "wgcurrevisionid",
              "wgarticleid",
              "wgisarticle",
              "wgusergroups",
              "wgcategories",
              "wgpagecontentlanguage",
              "wgpagecontentmodel",
              "wgrelevantpagename",
              "wgisprobablyeditable",
              "wgnoticeproject",
              "wgcitereferencepreviewsactive",
              "wgpopupsflags",
              "wgvisualeditor",
              "wgmfdisplaywikibasedescriptions",
              "wgwmepagelength",
              "wgeditsubmitbuttonlabelpublish",
              "wgulsiscompactlinksenabled",
              "wgwikibaseitemid",
              "wgcheckuserclienthintsheadersjsapi",
              "wggelevelingupenabledforuser",
              "wgmediaviewerenabledbydefault",
              "wgwmeschemaeditattemptstepoversample",
              "wgulsposition",
              "wgulsislanguageselectorempty",
              "wgbreakframes",
              "wgdigittransformtable",
              "wgrequestid",
              "wgcanonicalnamespace",
              "wgaction",
              "wgusername",
              "wgrelevantarticleid",
              "wgrelevantpageisprobablyeditable",
              "wgrestrictionedit",
              "wgrestrictionmove",
              "wgflaggedrevsparams",
              "wgmediavieweronclick",
              "wgpageparsereport",
              "null",
              "function",
              "return",
              "foreach",
              "documentelement",
              "target",
              "tags",
              "brands",
              "platformversion",
              "rlstate",
              "wikimediamessages",
              "startup",
              "jquery",
              "popup",
              "increment",
              "developers",
              "ppvisitednodes",
              "templateargumentsize",
              "entityaccesscount",
              "cachereport",
              "timestamp",
              "transientcontent",
              "wmf",
              "custom",
              "match",
              "enwikimwclientpreferences",
              "regexp",
              "wgseparatortransformtable",
              "wgdefaultdateformat",
              "wgmonthnames",
              "wgcanonicalspecialpagename",
              "wgnamespacenumber",
              "wgpagename",
              "walltime",
              "expensivefunctioncount",
              "timeusage",
              "ttl",
              "pagelanguagecode",
              "pagevariantfallbacks",
              "watchlist",
              "tagline",
              "architecture",
              "bitness",
              "fullversionlist",
              "loading",
              "codex",
              "icons",
              "noscript",
              "rlpagemodules",
              "geoip",
              "toolbar",
              "popups",
              "targetloader",
              "echo",
              "wikimediaevents",
              "navigationtiming",
              "checkuser",
              "clienthints",
              "suggestededitsession",
              "loader",
              "impl",
              "tokens",
              "jump",
              "eventsrandom",
              "csrftoken",
              "navigation",
              "filepermanent",
              "pageget",
              "urldownload",
              "download",
              "pdfprintable",
              "php",
              "descriptionshort",
              "trademark",
              "topic",
              "schema",
              "mainentity",
              "imageobject",
              "datepublished",
              "cputime",
              "postexpandincludesize",
              "expansiondepth",
              "timingprofile",
              "memusage",
              "sameas",
              "contributors",
              "campaigs",
              "hor",
              "googpub",
              "datemodified",
              "headline",
              "config",
              "output",
              "parser",
              "navbox",
              "reflist",
              "content",
              "html",
              "infobox",
              "url",
              "wfo",
              "header",
              "toc",
              "named",
              "distribution",
              "names",
              "new",
              "subsp",
              "absolute",
              "world",
              "block",
              "authomatically",
              "also",
              "found",
              "known",
              "sistersitebox",
              "multiple",
              "ncbi",
              "src",
              "use",
              "shaped",
              "irmng",
              "non",
              "contact",
              "decimal",
              "images",
              "quicksurveys",
              "require",
              "doctype",
              "rlconf",
              "gehomepagesuggestededitsenabletopics",
              "wggetopicsmatchmodeenabled",
              "index",
              "wikidataarticles",
              "commonswikispecieswikidata",
              "john",
              "speciesbox",
              "ccf",
              "taxonrow",
              "contains",
              "meaning",
              "several",
              "slightly",
              "part",
              "less",
              "far",
              "gas",
              "foc",
              "study",
              "great",
              "five",
              "year",
              "made",
              "den",
              "row",
              "wginternalredirecttargeturl",
              "jstor",
              "wgredirectedfrom",
              "wggetopicsmatchmodeenabled",
              "wginternalredirecttargeturl",
              "wikidatataxonbars",
              "wikidatause",
              "wikidataarticles",
              "clientpref",
              "body",
              "fdfdfd",
              "space",
              "sizing",
              "pagelanguagedir",
              "referencetooltips",
              "switcher",
              "urlshortener",
              "growthexperiments",
              "enhancements",
              "clientpref",
              "additional",
              "smaller",
              "twinaray",
              "items",
              "matches",
              "growing",
              "microformatscommons",
              "fna",
              "subtle",
              "subject",
              "occurs",
              "usually",
              "interactive",
              "many",
              "action",
              "nbn",
              "redirect",
              "apdb",
              "sdcat",
              "eds",
              "biolib",
              "photo",
              "gcc",
              "svenskawinaray",
              "sub",
              "ais",
              "around",
              "single",
              "issn",
              "due",
              "time",
              "identifiershakea",
              "include",
              "codfw",
              "refers",
              "jepson",
              "early",
              "throughout",
              "section",
              "either",
              "especially",
              "occasionally",
              "however",
              "listed",
              "expected",
              "bot",
              "photos",
              "ending",
              "towards",
              "recognized",
              "cap",
              "nom",
              "elt",
              "clip",
              "service",
              "tro",
              "aaa",
              "flex",
              "formally",
              "occurs",
              "fna",

              // Wiki site references
              "wikimedia",
              "wikipedia",
              "wiki",
              "wikidata",
              "wiktionary",
              "wikisource",
              "wikispecies",
              "commons",
              "commonswikidata",
              "wikispecieswikidata",
              "cebuano",
              "cebuanosvenskati",

              // Language and internationalization
              "english",
              "latin",
              "french",
              "german",
              "languages",
              "spanish",
              "portuguese",
              "italian",
              "russian",
              "japanese",
              "chinese",
              "korean",
              "greek",
              "ltr",
              "dmy",

              // Additional relevant wiki related terms from the JSON
              "reset",
              "ddf",
              "odd",
              "useformat",
              "desktop",
              "oldid",
              "edited",
              "agree",
              "registered",
              "profit",
              "disclaimers",
              "conduct",
              "patroltoken",
              "watchtoken",
              "pagecontentscurrent",
              "linkpage",
              "informationcite",
              "shortened",
              "articleabout",
              "wikipediacontact",
              "helplearn",
              "editcommunity",
              "portalrecent",
              "filespecial",
              "contributionstalk",
              "articletalk",
              "hererelated",
              "microformats",
              "microformatstaxonomy",
              "microformatsall",
              "microformatstaxonbars",
              "pagetype",
              "quotes",
              "rgba",
              "keyword",
              "wikitext",
              "interface",

              // Format identifiers and general descriptors that don't add taxonomic value
              "bold",
              "italic",
              "size",
              "large",
              "small",
              "contain",
              "containing",
              "related",
              "similar",
              "used",
              "uses",
              "pink",
              "purple",
              "green",
              "blue",
              "yellow",
              "orange",
              "red",
              "brown",
              "black",
              "white",
              "gray",
              "grey",
              "smooth",
              "id",
              "style",
              "div",
              "span",
            ]);

            // Pre-compile the regex pattern for better performance
            const alphaOnlyPattern = /^[a-z]+$/;

            const docData = value;
            const docId = docData.url;
            const words = docData.article_words || [];

            // Use a Map for word counts - slightly more efficient than object literals
            const wordCounts = new Map();
            const totalWords = words.length;

            // Process all words in a single pass
            for (const word of words) {
              // Skip words with 2 or fewer characters
              if (word.length <= 2) continue;

              // Normalize to lowercase for consistency
              const cleanWord = word.toLowerCase();

              // Skip if the word is in the stop list - O(1) lookup with Set
              if (stopWords.has(cleanWord)) continue;

              // Filter out words containing any non-alphabetic characters
              if (!alphaOnlyPattern.test(cleanWord)) continue;

              // Count the word occurrence
              wordCounts.set(cleanWord, (wordCounts.get(cleanWord) || 0) + 1);
            }

            // Emit each word with document ID, count, and total number of words processed
            // Use Array.from for better performance on large maps
            return Array.from(wordCounts, ([word, count]) => {
              // console.log(`Mapper emitting word: ${word}, count: ${count}, docId: ${docId}, totalWords: ${totalWords}`);
              return { [word]: { docId, count, totalWords } };
            });
          } catch (err) {
            console.error(`Error in mapper for ${key}:`, err);
            return [];
          }
        };

        // Define the reducer function
        // This calculates TF-IDF for each word across all documents
        const reducer = function (word, values) {
          try {
            // Total number of documents
            const totalDocs = values.length; // Use the actual number of documents in your dataset, or set it dynamically

            // Calculate term frequency for each document
            const docScores = values.map((value) => {
              const { docId, count, totalWords } = value;
              // TF = (Number of times term t appears in document) / (Total number of terms in document)
              const tf = count / totalWords;
              return { docId, tf, count };
            });

            // Calculate inverse document frequency

            // Calculate TF-IDF for each document
            const tfScores = docScores.map((doc) => {
              return {
                docId: doc.docId,
                tf: doc.tf,
                count: doc.count,
              };
            });

            // Return word with its TF-IDF scores across documents
            return {
              word: word,
              documentFrequency: totalDocs,
              scores: tfScores,
            };
          } catch (err) {
            console.error(`Error in reducer for ${word}:`, err);
            return { word: word, error: err.message };
          }
        };

        // Get total documents count to use in final calculation
        distribution.tfidf.store.get({ key: null }, async (err, allKeys) => {
          if (err && !isEmptyObject(err)) {
            console.error("Error getting document keys:", err);
            finish();
            return;
          }

          // Set total documents count
          const totalDocuments = allKeys.length;
          console.log(`Total documents to process: ${totalDocuments}`);

          if (totalDocuments === 0) {
            console.log("No documents found to process!");
            finish();
            return;
          }

          // Configure batch processing
          const BATCH_SIZE = CONFIG.BATCH_SIZE;
          const ESTIMATED_TOTAL_BATCHES = Math.ceil(
            totalDocuments / (BATCH_SIZE * num_nodes)
          );
          CONFIG.ESTIMATED_TOTAL_BATCHES = ESTIMATED_TOTAL_BATCHES; // Store for progress estimation

          console.log(
            `Processing with ${num_nodes} nodes, each processing ${BATCH_SIZE} keys per batch`
          );
          console.log(
            `Estimated total batches: ${ESTIMATED_TOTAL_BATCHES} (may vary based on key distribution)`
          );

          // Setup directories for results
          const resultsDir = CONFIG.RESULTS_DIR;
          const batchesDir = CONFIG.BATCHES_DIR;

          if (!fs.existsSync(resultsDir)) {
            fs.mkdirSync(resultsDir, { recursive: true });
          }

          if (!fs.existsSync(batchesDir)) {
            fs.mkdirSync(batchesDir, { recursive: true });
          }

          // Check for recovery data
          const recoveryData = checkRecovery(resultsDir);
          let batchIndex = 0;
          let successfulBatches = [];

          if (recoveryData) {
            console.log(`\n===== RECOVERY DATA FOUND =====`);
            console.log(
              `Last successful batch: ${recoveryData.lastBatchIndex}`
            );
            console.log(`Batches processed: ${recoveryData.batchesProcessed}`);

            // Ask if the user wants to resume
            const readline = require("readline").createInterface({
              input: process.stdin,
              output: process.stdout,
            });

            const response = await new Promise((resolve) => {
              readline.question(
                "Do you want to resume from the last batch? (y/n): ",
                (answer) => {
                  readline.close();
                  resolve(answer.toLowerCase());
                }
              );
            });

            if (response === "y" || response === "yes") {
              batchIndex = recoveryData.lastBatchIndex + 1;
              successfulBatches = recoveryData.successfulBatches;
              console.log(`Resuming from batch ${batchIndex}...`);
            } else {
              console.log("Starting from the beginning...");
              // Rename the old successful batches file as backup
              const backupFile = path.join(
                resultsDir,
                `successful-batches_backup_${Date.now()}.json`
              );
              fs.renameSync(
                path.join(resultsDir, "successful-batches.json"),
                backupFile
              );
              console.log(`Backed up previous progress to ${backupFile}`);
            }
          }

          // Process batches sequentially
          let continueProcessing = true;
          let emptyBatchCount = 0;
          const MAX_EMPTY_BATCHES = CONFIG.MAX_EMPTY_BATCHES;

          // Process batches until completion
          while (continueProcessing && emptyBatchCount < MAX_EMPTY_BATCHES) {
            const batchStartTime = Date.now();
            console.log(`\n===== PROCESSING BATCH ${batchIndex + 1} =====`);

            // Configure and run MapReduce for this batch
            const mrConfig = {
              map: mapper,
              reduce: reducer,
              batchSize: BATCH_SIZE,
              batchIndex: batchIndex,
              enable_checkpoints: true,
              batchInfo: {
                current: batchIndex + 1,
                total: ESTIMATED_TOTAL_BATCHES,
              },
              gid: tfidfConfig.gid,
            };

            // Execute MapReduce for this batch
            try {
              console.log(
                `Starting MapReduce execution for batch ${
                  batchIndex + 1
                } with batch size ${BATCH_SIZE}`
              );

              await new Promise((resolve, reject) => {
                let retries = 0;

                const executeBatch = () => {
                  distribution.tfidf.mr.exec(
                    mrConfig,
                    (batchErr, batchResults) => {
                      if (batchErr && !isEmptyObject(batchErr)) {
                        console.error(
                          `Error processing batch ${batchIndex + 1}:`,
                          batchErr
                        );

                        // Retry logic
                        if (retries < CONFIG.MAX_RETRIES) {
                          retries++;
                          console.log(
                            `Retrying batch ${
                              batchIndex + 1
                            } (Attempt ${retries}/${CONFIG.MAX_RETRIES})...`
                          );
                          setTimeout(executeBatch, CONFIG.RETRY_DELAY);
                          return;
                        }

                        PERF.errors++;
                        resolve(); // Continue despite error after max retries
                        return;
                      }

                      if (!Array.isArray(batchResults)) {
                        console.log(
                          `Batch ${
                            batchIndex + 1
                          } returned invalid results (non-array)`
                        );
                        PERF.errors++;
                        resolve();
                        return;
                      }

                      if (batchResults.length === 0) {
                        console.log(
                          `Batch ${batchIndex + 1} returned no results`
                        );
                        emptyBatchCount++;
                        console.log(
                          `Empty batch count: ${emptyBatchCount}/${MAX_EMPTY_BATCHES}`
                        );
                        resolve();
                        return;
                      }

                      console.log(
                        `Batch ${batchIndex + 1} completed with ${
                          batchResults.length
                        } results`
                      );

                      // Reset empty batch counter since we got results
                      emptyBatchCount = 0;

                      // Save the batch results to a file
                      const batchFilePath = `${batchesDir}/batch-${
                        batchIndex + 1
                      }.json`;
                      try {
                        fs.writeFileSync(
                          batchFilePath,
                          JSON.stringify(batchResults, null, 0)
                        );
                        console.log(
                          `Saved batch ${
                            batchIndex + 1
                          } results to ${batchFilePath}`
                        );

                        successfulBatches.push({
                          batchIndex: batchIndex,
                          resultCount: batchResults.length,
                          filePath: batchFilePath,
                        });

                        fs.writeFileSync(
                          `${resultsDir}/successful-batches.json`,
                          JSON.stringify(successfulBatches, null, 2)
                        );

                        // Create checkpoint if needed
                        createCheckpoint(
                          batchIndex,
                          successfulBatches,
                          resultsDir
                        );
                      } catch (writeErr) {
                        console.error(
                          `Error saving batch results to file: ${writeErr.message}`
                        );
                        PERF.errors++;
                      }

                      resolve();
                    }
                  );
                };

                executeBatch();
              });
            } catch (execErr) {
              console.error(
                `Exception during MapReduce execution for batch ${
                  batchIndex + 1
                }:`,
                execErr
              );
              PERF.errors++;
            }

            // Calculate performance metrics
            const batchEndTime = Date.now();
            const batchDuration = batchEndTime - batchStartTime;
            PERF.batchTimes.push(batchDuration);

            if (batchDuration > 0) {
              const itemsProcessed =
                successfulBatches.length > 0
                  ? successfulBatches[successfulBatches.length - 1].resultCount
                  : 0;
              const throughput = itemsProcessed / (batchDuration / 1000);
              PERF.throughput.push(throughput);
            }

            // Log performance statistics
            logPerformanceStats(batchIndex);

            // Force garbage collection if available
            if (global.gc) {
              console.log("Forcing garbage collection between batches...");
              global.gc();
            }

            // Add a small delay between batches to allow system to stabilize
            await new Promise((resolve) => setTimeout(resolve, 2000));

            // Move to next batch
            batchIndex++;

            // Stop if we've had too many consecutive empty batches
            if (emptyBatchCount >= MAX_EMPTY_BATCHES) {
              console.log(
                `Stopping after ${MAX_EMPTY_BATCHES} consecutive empty batches`
              );
              continueProcessing = false;
            }
          }

          console.log(`\n===== ALL BATCHES COMPLETED =====`);
          console.log(
            `Successfully processed ${successfulBatches.length} batches`
          );

          // Save final performance data
          savePerformanceData();

          // Perform final aggregation with separate groups for terms and documents
          await optimizedFinalAggregation(
            resultsDir,
            batchesDir,
            totalDocuments,
            CONFIG.TERM_GROUP,
            CONFIG.DOC_GROUP
          );

          // Finish and shutdown
          finish();
        });
      });
    });
  });

  /**
   * Memory-optimized document index creation function
   * This function processes documents in smaller batches and uses streaming to
   * reduce memory consumption.
   */
  async function createDocumentIndex(resultsDir, termGid, docGid, documentMap) {
    console.log(`Creating document index...`);

    // Read metadata to get document count
    const indexMetadataPath = `${resultsDir}/index-metadata.json`;
    let totalDocuments = 0;
    try {
      if (fs.existsSync(indexMetadataPath)) {
        const metadata = JSON.parse(fs.readFileSync(indexMetadataPath, "utf8"));
        totalDocuments = metadata.totalDocuments || 0;
      }
    } catch (err) {
      console.error(`Error reading index metadata: ${err.message}`);
    }

    // If we can't get the count from metadata, try to determine it another way
    if (totalDocuments === 0) {
      // Try to get a count from the document map keys
      totalDocuments = Array.from(documentMap.keys()).length;
    }

    console.log(`Creating document index for ${totalDocuments} documents...`);

    // Configuration for batched processing
    const BATCH_SIZE = 100; // Process this many documents at a time
    const DOC_PREFIX = CONFIG.DOC_PREFIX || "doc:";

    // Function to put a value in the distributed store
    function putInDistributedStore(gid, key, value) {
      return new Promise((resolve, reject) => {
        // Make sure the key is a proper string
        if (typeof key !== "string") {
          key = String(key);
        }

        distribution[gid].store.put(value, key, (err, result) => {
          if (err && !isEmptyObject(err)) {
            console.error(
              `Error storing ${key} in ${gid} distributed store:`,
              err
            );
            reject(err);
          } else {
            resolve(result);
          }
        });
      });
    }

    // Create document output directory if it doesn't exist
    const docsDir = `${resultsDir}/docs`;
    if (!fs.existsSync(docsDir)) {
      fs.mkdirSync(docsDir, { recursive: true });
    }

    // Get all document IDs
    const docIds = Array.from(documentMap.keys());

    // Process documents in batches to reduce memory usage
    let processedDocs = 0;
    let errorCount = 0;

    for (let i = 0; i < docIds.length; i += BATCH_SIZE) {
      const batchDocIds = docIds.slice(i, i + BATCH_SIZE);
      console.log(
        `Processing documents ${i + 1} to ${Math.min(
          i + BATCH_SIZE,
          docIds.length
        )} of ${docIds.length}...`
      );

      const batchPromises = [];

      for (const docId of batchDocIds) {
        const docData = documentMap.get(docId);

        if (!docData || !docData.terms || !docData.terms.length) {
          continue;
        }

        // Process this document in a memory-efficient way
        try {
          // Sort terms by TF-IDF score (descending)
          docData.terms.sort((a, b) => b.tfidf - a.tfidf);

          // Limit to top N terms per document to save memory
          const topTermsCount = Math.min(
            docData.terms.length,
            CONFIG.MAX_TERMS_PER_DOC
          );
          const topTerms = docData.terms.slice(0, topTermsCount);

          // Calculate document vector norm for similarity calculations
          let normSum = 0;
          for (let i = 0; i < topTerms.length; i++) {
            normSum += Math.pow(topTerms[i].tfidf, 2);
          }
          const docNorm = Math.sqrt(normSum);

          // Create optimized document object
          const docObject = {
            id: docId,
            totalWords: docData.totalWords,
            docNorm: parseFloat(docNorm.toFixed(6)),
            // Store terms in compact format [term, tfidf, count]
            terms: topTerms.map((t) => [
              t.term,
              parseFloat(t.tfidf.toFixed(6)),
              t.count,
            ]),
          };

          // Store the document object
          if (CONFIG.USE_DISTRIBUTED_STORE) {
            // Store in distributed key-value store
            const storeKey = `${DOC_PREFIX}${docId}`;
            batchPromises.push(
              putInDistributedStore(docGid, storeKey, docObject)
                .then(() => {
                  processedDocs++;
                  return null;
                })
                .catch((err) => {
                  console.error(`Failed to store document ${docId}: ${err}`);
                  errorCount++;
                  return null;
                })
            );
          } else {
            // Store to local file system
            try {
              const docPath = `${docsDir}/${encodeURIComponent(docId)}.json`;
              fs.writeFileSync(docPath, JSON.stringify(docObject));
              processedDocs++;
            } catch (fsErr) {
              console.error(
                `Error writing document ${docId} to file: ${fsErr}`
              );
              errorCount++;
            }
          }

          // Free memory - remove this document from the map
          documentMap.delete(docId);
        } catch (err) {
          console.error(`Error processing document ${docId}: ${err}`);
          errorCount++;
        }
      }

      // Wait for all store operations to complete for this batch
      if (batchPromises.length > 0) {
        await Promise.allSettled(batchPromises);
      }

      // Force garbage collection to reclaim memory
      if (global.gc) {
        console.log("Running garbage collection...");
        global.gc();
      } else {
        // If explicit GC is not available, try to force it through memory pressure
        const pressure = [];
        pressure.length = 1000000;
        pressure.length = 0;
      }

      // Log progress
      console.log(
        `Processed ${processedDocs} documents with ${errorCount} errors...`
      );
    }

    console.log(
      `Document index creation complete. Processed ${processedDocs} documents with ${errorCount} errors.`
    );
  }

  /**
   * Memory-optimized term storage function
   * This function processes terms in smaller batches with better error handling
   */
  async function storeTermsInDistributedStore(
    termsFinalIndex,
    termGid,
    resultsDir
  ) {
    const termCount = Object.keys(termsFinalIndex).length;
    console.log(`Storing ${termCount} terms in distributed store...`);

    // Configuration
    const BATCH_SIZE = 50; // Store this many terms at a time
    const INDEX_PREFIX = CONFIG.INDEX_PREFIX || "term:";

    // Function to put a value in the distributed store with better error handling
    function putInDistributedStore(gid, key, value) {
      return new Promise((resolve, reject) => {
        // Make sure the key is a proper string
        if (typeof key !== "string") {
          key = String(key);
        }

        let retries = 0;
        const maxRetries = CONFIG.MAX_RETRIES || 3;

        const storeWithRetry = () => {
          distribution[gid].store.put(value, key, (err, result) => {
            if (err && !isEmptyObject(err)) {
              if (retries < maxRetries) {
                retries++;
                console.log(
                  `Retrying store operation for ${key} (${retries}/${maxRetries})...`
                );
                setTimeout(storeWithRetry, CONFIG.RETRY_DELAY || 1000);
                return;
              }
              console.error(
                `Error storing ${key} after ${maxRetries} retries: ${JSON.stringify(
                  err
                )}`
              );
              reject(err);
            } else {
              resolve(result);
            }
          });
        };

        storeWithRetry();
      });
    }

    // Create terms output directory for backup
    const termsDir = `${resultsDir}/terms`;
    if (!fs.existsSync(termsDir)) {
      fs.mkdirSync(termsDir, { recursive: true });
    }

    // Get all terms
    const allTerms = Object.keys(termsFinalIndex);

    // Track progress
    let completedTerms = 0;
    let errors = 0;

    // Process terms in batches
    for (let i = 0; i < allTerms.length; i += BATCH_SIZE) {
      const batchStartTime = Date.now();
      const termBatch = allTerms.slice(i, i + BATCH_SIZE);
      console.log(
        `Processing terms ${i + 1} to ${Math.min(
          i + BATCH_SIZE,
          allTerms.length
        )} of ${allTerms.length}...`
      );

      // Use Promise.allSettled to continue even if some promises reject
      const promises = termBatch.map((term) => {
        const storeKey = `${INDEX_PREFIX}${term}`;
        const termObject = termsFinalIndex[term];

        // Also save a backup to file system
        try {
          // Use the first two characters of the term as a directory prefix
          // to avoid too many files in one directory
          const prefix = term.substring(0, 2).toLowerCase();
          const prefixDir = `${termsDir}/${prefix}`;

          if (!fs.existsSync(prefixDir)) {
            fs.mkdirSync(prefixDir, { recursive: true });
          }

          const termPath = `${prefixDir}/${encodeURIComponent(term)}.json`;
          fs.writeFileSync(termPath, JSON.stringify(termObject));
        } catch (fileErr) {
          console.error(`Error saving backup of term ${term}: ${fileErr}`);
        }

        return putInDistributedStore(termGid, storeKey, termObject)
          .then(() => true)
          .catch((err) => {
            errors++;
            return false;
          });
      });

      // Wait for all promises to settle
      const results = await Promise.allSettled(promises);
      const successCount = results.filter(
        (r) => r.status === "fulfilled" && r.value === true
      ).length;

      // Update progress
      completedTerms += successCount;

      // Free memory by deleting processed terms
      for (const term of termBatch) {
        delete termsFinalIndex[term];
      }

      // Calculate throughput
      const batchEndTime = Date.now();
      const batchDuration = batchEndTime - batchStartTime;
      const throughput = BATCH_SIZE / (batchDuration / 1000);

      // Log progress with throughput information
      console.log(
        `Stored ${completedTerms} of ${allTerms.length} terms with ${errors} errors...`
      );
      console.log(`Batch throughput: ${throughput.toFixed(2)} terms/sec`);

      // Force garbage collection
      if (global.gc) {
        console.log("Running garbage collection...");
        global.gc();
      } else {
        // If explicit GC is not available
        const pressure = [];
        pressure.length = 1000000;
        pressure.length = 0;
      }
    }

    console.log(`Completed storing all terms in distributed store.`);
    return { completedTerms, errors };
  }

  /**
   * Memory-optimized final aggregation function
   * This function processes terms and documents in batches to avoid memory issues
   */
  async function optimizedFinalAggregation(
    resultsDir,
    batchesDir,
    totalDocuments,
    termGid,
    docGid
  ) {
    const aggregationStartTime = Date.now();
    console.log(`\n===== STARTING OPTIMIZED INDEX CREATION =====`);
    console.log(
      `Using separate groups for terms (${termGid}) and documents (${docGid})`
    );

    // Get list of batch files
    let batchFiles = [];
    try {
      if (fs.existsSync(`${resultsDir}/successful-batches.json`)) {
        const successfulBatches = JSON.parse(
          fs.readFileSync(`${resultsDir}/successful-batches.json`, "utf8")
        );
        console.log(
          `Found ${successfulBatches.length} successful batches from metadata`
        );
        batchFiles = successfulBatches
          .map((batch) => batch.filePath)
          .filter((path) => fs.existsSync(path));
      } else {
        batchFiles = fs
          .readdirSync(batchesDir)
          .filter((file) => file.startsWith("batch-") && file.endsWith(".json"))
          .map((file) => `${batchesDir}/${file}`);
        console.log(`Found ${batchFiles.length} batch files in directory`);
      }
    } catch (err) {
      console.error(`Error reading batch files: ${err.message}`);
      return;
    }

    if (batchFiles.length === 0) {
      console.log("No batch files found for aggregation");
      return;
    }

    // Create terms structure with a more memory-efficient approach
    const termsMap = new Map();
    const documentMap = new Map();
    let allDocuments = new Set();
    let totalProcessedResults = 0;

    // Create stats directory for batch processing metrics
    const statsDir = path.join(resultsDir, "stats");
    if (!fs.existsSync(statsDir)) {
      fs.mkdirSync(statsDir, { recursive: true });
    }

    // FIRST PASS: Process all batch files in chunks
    console.log("First pass: Building term and document maps...");

    // Process files in batches to control memory usage
    const FILE_BATCH_SIZE = 10;
    let fileProcessingStats = [];

    for (
      let fileIndex = 0;
      fileIndex < batchFiles.length;
      fileIndex += FILE_BATCH_SIZE
    ) {
      const fileBatchStartTime = Date.now();
      const fileBatch = batchFiles.slice(
        fileIndex,
        fileIndex + FILE_BATCH_SIZE
      );
      console.log(
        `Processing files ${fileIndex + 1} to ${Math.min(
          fileIndex + FILE_BATCH_SIZE,
          batchFiles.length
        )} of ${batchFiles.length}...`
      );

      let batchProcessedResults = 0;

      for (const batchFile of fileBatch) {
        console.log(`Processing batch file: ${batchFile}`);

        try {
          // Read the batch file
          const startReadTime = Date.now();
          const batchData = JSON.parse(fs.readFileSync(batchFile, "utf8"));
          const readTime = Date.now() - startReadTime;

          console.log(
            `Loaded ${
              batchData.length
            } results from ${batchFile} in ${formatTime(readTime)}`
          );

          // Process results in chunks to avoid memory pressure
          const CHUNK_SIZE = 1000;

          for (let j = 0; j < batchData.length; j += CHUNK_SIZE) {
            const chunkStartTime = Date.now();
            const chunk = batchData.slice(j, j + CHUNK_SIZE);

            // Process each result in the chunk
            for (const result of chunk) {
              if (!result || !result.word) continue;

              const word = result.word;

              if (!termsMap.has(word)) {
                termsMap.set(word, {
                  word,
                  documentFrequency: 0,
                  scores: [],
                  uniqueDocIds: new Set(),
                });
              }

              const entry = termsMap.get(word);

              // Add document scores
              if (Array.isArray(result.scores)) {
                result.scores.forEach((score) => {
                  if (!score || !score.docId) return;

                  // Add this document ID to our global set of all documents
                  allDocuments.add(score.docId);

                  // Add this document ID to the set of unique docs for this term
                  entry.uniqueDocIds.add(score.docId);

                  // Add score to our collection (efficiently)
                  entry.scores.push({
                    docId: score.docId,
                    tf: score.tf || 0,
                    count: score.count || 1,
                    totalWords: score.totalWords || 0,
                  });

                  // Initialize or update document map
                  if (!documentMap.has(score.docId)) {
                    documentMap.set(score.docId, {
                      terms: [],
                      totalWords: score.totalWords || 0,
                    });
                  }
                });
              }
            }

            batchProcessedResults += chunk.length;
            totalProcessedResults += chunk.length;

            const chunkProcessTime = Date.now() - chunkStartTime;

            // Only log every few chunks to reduce log noise
            if ((j / CHUNK_SIZE) % 10 === 0) {
              console.log(
                `Processed ${batchProcessedResults} results from this batch (${formatTime(
                  chunkProcessTime
                )} for last ${Math.min(CHUNK_SIZE, chunk.length)} results)...`
              );
            }
          }
        } catch (err) {
          console.error(
            `Error processing batch file ${batchFile}: ${err.message}`
          );
        }
      }

      const fileBatchEndTime = Date.now();
      const fileBatchDuration = fileBatchEndTime - fileBatchStartTime;

      // Record stats for this file batch
      fileProcessingStats.push({
        batchIndex: fileIndex / FILE_BATCH_SIZE,
        filesProcessed: fileBatch.length,
        resultsProcessed: batchProcessedResults,
        duration: fileBatchDuration,
        throughput: batchProcessedResults / (fileBatchDuration / 1000),
      });

      // Force garbage collection between file batches
      if (global.gc) {
        console.log("Forcing garbage collection between file batches...");
        global.gc();
      }

      // Save intermediate progress
      const progressFile = path.join(statsDir, "aggregation_progress.json");
      fs.writeFileSync(
        progressFile,
        JSON.stringify(
          {
            currentFileIndex: fileIndex + fileBatch.length,
            totalFiles: batchFiles.length,
            processedResults: totalProcessedResults,
            uniqueDocuments: allDocuments.size,
            uniqueTerms: termsMap.size,
            elapsedTime: Date.now() - aggregationStartTime,
          },
          null,
          2
        )
      );

      // Log progress
      console.log(
        `Processed ${totalProcessedResults} results from ${
          fileIndex + fileBatch.length
        } of ${batchFiles.length} files...`
      );
      console.log(
        `File batch throughput: ${(
          batchProcessedResults /
          (fileBatchDuration / 1000)
        ).toFixed(2)} results/sec`
      );
    }

    // Save file processing stats
    fs.writeFileSync(
      path.join(statsDir, "file_processing_stats.json"),
      JSON.stringify(fileProcessingStats, null, 2)
    );

    // Get the actual total documents count
    const actualTotalDocuments = allDocuments.size;
    console.log(
      `Detected ${actualTotalDocuments} unique documents in the corpus`
    );
    console.log(
      `Processed ${totalProcessedResults} total results from ${batchFiles.length} files`
    );
    console.log(`Found ${termsMap.size} unique terms`);

    // Save metadata for future reference
    const indexMetadata = {
      totalTerms: termsMap.size,
      totalDocuments: actualTotalDocuments,
      indexCreationDate: new Date().toISOString(),
      maxDocsPerTerm: CONFIG.MAX_DOCS_PER_TERM,
      maxTermsPerDoc: CONFIG.MAX_TERMS_PER_DOC,
      usingDistributedStore: CONFIG.USE_DISTRIBUTED_STORE,
      termGroup: termGid,
      docGroup: docGid,
      indexPrefix: CONFIG.INDEX_PREFIX,
      docPrefix: CONFIG.DOC_PREFIX,
      processingStats: {
        totalRuntime: Date.now() - aggregationStartTime,
        resultsPerSecond:
          totalProcessedResults / ((Date.now() - aggregationStartTime) / 1000),
      },
    };

    fs.writeFileSync(
      `${resultsDir}/index-metadata.json`,
      JSON.stringify(indexMetadata, null, 2)
    );

    // SECOND PASS: Calculate final metrics with a memory-efficient approach
    console.log(
      `Calculating final metrics using ${actualTotalDocuments} unique documents...`
    );

    // Create shard keys if requested
    let shards = {};
    let termShardMapping = {};

    if (CONFIG.CREATE_SHARDS) {
      // Create alphabet-based shards
      const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
      for (let i = 0; i < alphabet.length; i += CONFIG.SHARD_SIZE) {
        const shardKey = alphabet.slice(i, i + CONFIG.SHARD_SIZE);
        shards[shardKey] = {
          termCount: 0,
          terms: [],
        };
      }

      // Add a catch-all shard for terms that don't match any prefix
      shards["other"] = {
        termCount: 0,
        terms: [],
      };

      console.log(
        `Created ${Object.keys(shards).length} shards for term organization`
      );
    }

    // Function to get shard key for a term
    function getShardKey(term) {
      const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
      const prefix = term.substring(0, CONFIG.SHARD_SIZE).toLowerCase();

      for (const shardKey in shards) {
        if (shardKey === "other") continue;
        if (prefix.startsWith(shardKey)) {
          return shardKey;
        }
      }

      return "other";
    }

    // Create terms index in chunks
    const termsFinalIndex = {};
    const globalTermsIndex = [];

    // Process terms in chunks to manage memory
    const termKeys = Array.from(termsMap.keys()).sort();
    const PROCESS_CHUNK_SIZE = 10000; // Reduced for better memory management
    let termProcessingStats = [];

    for (let i = 0; i < termKeys.length; i += PROCESS_CHUNK_SIZE) {
      const chunkStartTime = Date.now();
      const chunkKeys = termKeys.slice(i, i + PROCESS_CHUNK_SIZE);
      console.log(
        `Processing terms ${i + 1} to ${i + chunkKeys.length} of ${
          termKeys.length
        }...`
      );

      let termsProcessed = 0;

      // Process each term in the chunk
      for (const word of chunkKeys) {
        const entry = termsMap.get(word);

        // Skip terms with no documents
        if (!entry.uniqueDocIds.size) continue;

        // Calculate document frequency from unique doc IDs
        const documentFrequency = entry.uniqueDocIds.size;

        // Calculate IDF using the actual total document count
        const idf = Math.log(
          actualTotalDocuments / Math.max(documentFrequency, 1)
        );

        // Create efficient posting list structure
        let docScores = [];

        // Process all scores for this word
        entry.scores.forEach((score) => {
          const docId = score.docId;
          if (!docId) return;

          // Calculate TF-IDF
          let tf = score.tf;
          if (
            typeof tf !== "number" &&
            typeof score.count === "number" &&
            typeof score.totalWords === "number" &&
            score.totalWords > 0
          ) {
            tf = score.count / score.totalWords;
          } else if (typeof tf !== "number" || isNaN(tf)) {
            tf = 0.001; // Default value for safety
          }

          const tfidf = tf * idf;

          // Add to doc scores array for sorting
          docScores.push({
            docId,
            score: tfidf,
            count: score.count || 1,
          });

          // Update document map with term info
          if (documentMap.has(docId)) {
            const docData = documentMap.get(docId);
            docData.terms.push({
              term: word,
              tfidf,
              count: score.count || 1,
            });
          }
        });

        // Sort document scores by descending TF-IDF
        docScores.sort((a, b) => b.score - a.score);

        // Limit to top N documents to conserve space
        if (docScores.length > CONFIG.MAX_DOCS_PER_TERM) {
          docScores = docScores.slice(0, CONFIG.MAX_DOCS_PER_TERM);
        }

        // Create final term object for storage
        const termObject = {
          term: word,
          df: documentFrequency,
          idf,
          // Use compact posting list format [docId, score, count]
          postings: docScores.map((p) => [
            p.docId,
            parseFloat(p.score.toFixed(6)),
            p.count,
          ]),
        };

        // Calculate term importance (sum of all TF-IDF scores)
        termObject.importance = docScores.reduce(
          (sum, doc) => sum + doc.score,
          0
        );

        // Add to global terms index if it's significant
        if (termObject.importance > 0.05 || globalTermsIndex.length < 10000) {
          globalTermsIndex.push({
            term: word,
            df: documentFrequency,
            idf,
            importance: termObject.importance,
          });
        }

        // Add to shard if using sharding
        if (CONFIG.CREATE_SHARDS) {
          const shardKey = getShardKey(word);
          shards[shardKey].termCount++;

          // Only store a sample of terms in memory to save space
          if (shards[shardKey].terms.length < 100) {
            shards[shardKey].terms.push(word);
          }

          termShardMapping[word] = shardKey;
        }

        // Add to terms index for batch storage
        termsFinalIndex[word] = termObject;
        termsProcessed++;

        // If we've accumulated a lot of terms, store them to free memory
        if (Object.keys(termsFinalIndex).length >= 10000) {
          // Threshold for batch storage
          console.log(
            `Storing accumulated ${
              Object.keys(termsFinalIndex).length
            } terms to free memory...`
          );
          await storeTermsInDistributedStore(
            termsFinalIndex,
            termGid,
            resultsDir
          );
          // termsFinalIndex is now empty after processing
        }

        // Free memory - remove this term from the map
        termsMap.delete(word);
      }

      const chunkEndTime = Date.now();
      const chunkDuration = chunkEndTime - chunkStartTime;

      // Record stats for this term chunk
      termProcessingStats.push({
        chunkIndex: i / PROCESS_CHUNK_SIZE,
        termsProcessed,
        duration: chunkDuration,
        throughput: termsProcessed / (chunkDuration / 1000),
      });

      console.log(
        `Processed ${Math.min(i + PROCESS_CHUNK_SIZE, termKeys.length)} of ${
          termKeys.length
        } terms in ${formatTime(chunkDuration)}...`
      );
      console.log(
        `Term processing throughput: ${(
          termsProcessed /
          (chunkDuration / 1000)
        ).toFixed(2)} terms/sec`
      );

      // Force garbage collection if available
      if (global.gc) {
        console.log("Forcing garbage collection between term chunks...");
        global.gc();
      }

      // Save progress
      const progressFile = path.join(statsDir, "term_processing_progress.json");
      fs.writeFileSync(
        progressFile,
        JSON.stringify(
          {
            currentTermIndex: i + chunkKeys.length,
            totalTerms: termKeys.length,
            elapsedTime: Date.now() - aggregationStartTime,
            remainingTerms: termKeys.length - (i + chunkKeys.length),
          },
          null,
          2
        )
      );
    }

    // Save term processing stats
    fs.writeFileSync(
      path.join(statsDir, "term_processing_stats.json"),
      JSON.stringify(termProcessingStats, null, 2)
    );

    // Sort global terms index by importance
    globalTermsIndex.sort((a, b) => b.importance - a.importance);

    // Save global terms index
    fs.writeFileSync(
      `${resultsDir}/global-terms-index.json`,
      JSON.stringify(globalTermsIndex.slice(0, 10000), null, 2)
    );

    // If using shards, save shard information
    if (CONFIG.CREATE_SHARDS) {
      // Save shard metadata (excluding full term lists to save space)
      const shardMetadata = {};
      for (const shardKey in shards) {
        shardMetadata[shardKey] = {
          termCount: shards[shardKey].termCount,
          sampleTerms: shards[shardKey].terms.slice(0, 10), // Just save a few sample terms
        };
      }

      fs.writeFileSync(
        `${resultsDir}/shard-metadata.json`,
        JSON.stringify(shardMetadata, null, 2)
      );

      // Save a sample of the term-to-shard mapping (not the entire mapping to save space)
      const sampleMapping = {};
      let count = 0;
      for (const term in termShardMapping) {
        if (count++ > 1000) break;
        sampleMapping[term] = termShardMapping[term];
      }

      fs.writeFileSync(
        `${resultsDir}/term-shard-mapping.json`,
        JSON.stringify(sampleMapping, null, 2)
      );
    }

    // Store any remaining terms that haven't been stored yet
    if (Object.keys(termsFinalIndex).length > 0) {
      console.log(
        `Storing remaining ${Object.keys(termsFinalIndex).length} terms...`
      );
      await storeTermsInDistributedStore(termsFinalIndex, termGid, resultsDir);
    }

    // Create document index
    const docIndexStartTime = Date.now();
    await createDocumentIndex(resultsDir, termGid, docGid, documentMap);
    const docIndexTime = Date.now() - docIndexStartTime;

    // Final stats
    const aggregationEndTime = Date.now();
    const totalAggregationTime = aggregationEndTime - aggregationStartTime;

    console.log(`\n===== OPTIMIZED INDEX CREATION COMPLETE =====`);
    console.log(
      `Created index for ${actualTotalDocuments} documents and ${termKeys.length} terms`
    );
    console.log(`Total processing time: ${formatTime(totalAggregationTime)}`);
    console.log(`Document indexing time: ${formatTime(docIndexTime)}`);

    // Save final stats
    fs.writeFileSync(
      path.join(statsDir, "aggregation_summary.json"),
      JSON.stringify(
        {
          totalRuntime: totalAggregationTime,
          documentsIndexed: actualTotalDocuments,
          termsIndexed: termKeys.length,
          totalResultsProcessed: totalProcessedResults,
          documentIndexingTime: docIndexTime,
          averageTermThroughput:
            termKeys.length / ((totalAggregationTime - docIndexTime) / 1000),
          averageDocThroughput: actualTotalDocuments / (docIndexTime / 1000),
        },
        null,
        2
      )
    );

    if (CONFIG.USE_DISTRIBUTED_STORE) {
      console.log(`Index is available in the distributed store with:`);
      console.log(
        `  - Terms: ${termGid} group with prefix ${CONFIG.INDEX_PREFIX}`
      );
      console.log(
        `  - Documents: ${docGid} group with prefix ${CONFIG.DOC_PREFIX}`
      );
    }
  }

  // Cleanup function
  const finish = async () => {
    console.log("SHUTTING DOWN...");
    for (const node of nodes) {
      await stop_node(node);
    }
    server.close();
  };
});
