# Distributed Taxonomic Search Engine

A high-performance, distributed search engine built for Wikipedia biology content with specialized taxonomy-aware indexing and querying capabilities. This project implements a distributed system that crawls, indexes, and queries biological data using TF-IDF ranking and hierarchical taxonomic classification.

## Features

### Distributed Architecture
- **Scalable Multi-Node System**: Configurable node count supporting up to 128 distributed nodes working in parallel
- **Service Groups**: Separate groups for crawling, indexing, range indexing, and querying
- **Fault Tolerance**: Automatic recovery mechanisms and state persistence
- **Horizontal Scalability**: Can run locally or across multiple servers with consistent hashing

### Intelligent Web Crawler
- **Distributed BFS Crawling**: Implements breadth-first search across distributed nodes, where each link is assigned to a specific node via consistent hashing. When a page is crawled, discovered links are distributed to their designated nodes, enabling parallel BFS traversal across the cluster.
- **Rate-Limited Requests**: Built-in throttling (3 requests/second) to respect Wikipedia's servers
- **Retry Logic**: Exponential backoff with configurable timeouts (up to 15s)
- **Smart Link Discovery**: Automatically discovers and queues related biological pages
- **Persistent State**: Saves crawled data to disk for recovery
- **Performance Metrics**: Real-time tracking of crawl throughput and errors

### Advanced Indexing

#### TF-IDF Indexer  
- **Term Frequency-Inverse Document Frequency**: Standard search ranking algorithm
- **Prefix-Based Sharding**: Distributes index across nodes using smart prefixes
- **Stopword Filtering**: Removes common words to improve relevance
- **Batch Processing**: Efficient batch updates to distributed index
- **Taxonomy Boosting**: Enhances scores for terms appearing in taxonomic classifications

#### Taxonomic Range Indexer
- **Hierarchical Structure**: Builds tree-based taxonomy from Kingdom to Species
- **Range Queries**: Efficiently retrieves all organisms within a taxonomic group
- **Parent-Child Relationships**: Maintains complete phylogenetic hierarchy
- **Species Detection**: Identifies and marks species-level classifications
- **Tree Visualization**: Beautiful ASCII tree rendering with color-coded taxa

### Powerful Querying System
- **Multi-Term Search**: Supports complex queries with multiple keywords
- **Relevance Ranking**: Combines TF-IDF scores with taxonomy-aware boosting
- **Two Query Types**:
  - **`query_one`**: Standard keyword search with ranked results
  - **`query_range`**: Hierarchical taxonomy exploration
- **Fast Response Times**: Distributed querying across multiple nodes
- **Rich Metadata**: Returns taxonomy classification, binomial names, descriptions

### Interactive REPL Interface
- **Beautiful Terminal UI**: Color-coded results with box-drawing characters
- **Multiple Commands**:
  - Search queries with optional `-d` flag for detailed results
  - `crawl /wiki/PAGE` - Add Wikipedia pages to crawler queue
  - `tree TAXONOMY` - Explore taxonomic trees (e.g., `tree plantae`)
  - `stats` - Display system performance metrics
  - `save` - Force save system state to disk
- **Visual Feedback**: Animated spinners and progress indicators
- **Query Growth Tracking**: Monitors search patterns over time

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Main Coordinator Node                     │
└─────────────────────────────────────────────────────────────┘
         │
         ├─────────────────────────────────────────────────────┐
         │                                                     │
    ┌────▼─────┐    ┌───────────┐    ┌──────────────┐   ┌────▼─────┐
    │ Crawler  │───▶│  Indexer  │───▶│Range Indexer │──▶│ Querier  │
    │  Group   │    │   Group   │    │    Group     │   │  Group   │
    │(N nodes) │    │ (N nodes) │    │  (N nodes)   │   │(N nodes) │
    └──────────┘    └───────────┘    └──────────────┘   └──────────┘
       (Distributed BFS with consistent hashing)
         │               │                   │                 │
         ▼               ▼                   ▼                 ▼
    ┌────────────────────────────────────────────────────────────┐
    │           Distributed Key-Value Store (Persistent)         │
    └────────────────────────────────────────────────────────────┘
```

### Component Breakdown

- **Crawler**: Fetches Wikipedia pages, extracts biological metadata (taxonomy, binomial names, descriptions)
- **Indexer**: Builds inverted TF-IDF index with prefix-based distribution
- **Range Indexer**: Constructs hierarchical taxonomy tree across distributed nodes
- **Querier**: Processes search queries, merges results, and ranks by relevance
- **Store**: Persistent distributed key-value store for crawled documents

## Getting Started

### Prerequisites

- Node.js (v14 or higher)
- npm or yarn

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/cs1380-final-project-repo.git
   cd cs1380-final-project-repo
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Start the search engine REPL**
   ```bash
   node search-repl.js
   ```

   The system will automatically (with default configuration of 8 nodes):
   - Spawn 8 local nodes
   - Initialize all service groups
   - Add seed links for Plants, Sealife, and Butterflies
   - Start crawling and indexing in the background

### Quick Start Commands

Once the REPL is running, try these commands:

```bash
# Search for organisms
citrus                 # Search for citrus-related organisms
leafy sour -d          # Search with detailed results
aquatic water seed     # Multi-term search

# Explore taxonomy trees
tree plantae           # View plant taxonomy
tree cnidaria          # View cnidarian taxonomy
tree lepidoptera       # View butterfly taxonomy

# System management
stats                  # View performance metrics
crawl /wiki/Rosids     # Add a specific page to crawl
save                   # Save current state to disk
help                   # Show all available commands
```

## Performance Metrics

The system tracks comprehensive metrics:

- **Crawler**: Pages processed, crawl throughput (pages/sec), queue depth, errors
- **Indexer**: Documents indexed, index throughput, terms processed, batch operations
- **Range Indexer**: Taxonomic relationships indexed, tree depth, species count
- **Querier**: Query response times, average results, cache hit rates

View real-time metrics with the `stats` command in the REPL.

## Configuration

### Node Configuration (`main.js`)

```javascript
const spawn_nodes_locally = true;  // Set to false for distributed deployment
const num_nodes = 8;                // Number of nodes (configurable up to 128)
```

### Remote Deployment

For deploying across multiple servers, update the `nodes` array:

```javascript
const nodes = [
    { ip: '3.87.36.179', port: 8000 },
    { ip: '54.205.32.141', port: 8000 },
    // ... add more nodes
];
```

### Crawler Settings

Adjust rate limiting in `distribution/local/crawler.js`:

```javascript
const fetchLimited = new RateLimiter(3, 1000); // 3 requests per second
```

## Use Cases

1. **Biological Research**: Quickly find organisms by common characteristics
2. **Taxonomy Exploration**: Navigate phylogenetic trees interactively
3. **Educational Tool**: Learn about organism classification and relationships
4. **Data Mining**: Extract structured biological data from Wikipedia
5. **Search Algorithm Testing**: Experiment with distributed search techniques

## Project Structure

```
cs1380-final-project-repo/
├── distribution/
│   ├── local/
│   │   ├── crawler.js          # Web crawler with rate limiting
│   │   ├── indexer.js          # TF-IDF indexer
│   │   ├── indexer_ranged.js   # Taxonomic range indexer
│   │   ├── querier.js          # Query processor
│   │   ├── store.js            # Distributed KV store
│   │   ├── groups.js           # Group management
│   │   └── ...
│   └── util/
│       └── stopwords.js        # Stopword list
├── main.js                     # Production orchestration
├── search-repl.js              # Interactive REPL interface
├── config.js                   # Configuration loader
├── distribution.js             # Distribution framework entry point
├── visualize-metrics.js        # Performance visualization
├── visualize-query-growth.js   # Query growth analysis
└── package.json
```

## Testing

Run the main orchestration script for automated testing:

```bash
node main.js
```

This will:
- Start crawling and indexing
- Execute sample queries periodically
- Log comprehensive metrics to `log.txt`
- Generate performance visualizations

## Advanced Features

### Recovery Mode
The system periodically pauses core services to take snapshots and calculate metrics without interrupting ongoing operations. Recovery times are tracked and logged.

### Custom Taxonomy Boosting
Results are boosted when:
- Query terms appear in taxonomic classification (Kingdom, Phylum, Class, etc.)
- Query terms match binomial scientific names
- Multiple taxonomy levels match

### Distributed Consistent Hashing
Uses naive hash-based distribution to ensure consistent key-to-node mapping across the distributed store.

## License

This project was developed as part of CS1380 (Distributed Computer Systems) at Brown University.

## Acknowledgments

- Built using the `@brown-ds/distribution` framework
- Wikipedia API for biological content
- Brown University CS1380 course staff

## Contact

For questions or contributions, please open an issue on GitHub.

---

**Happy searching!**