# 📊 Optionality

High-performance stock and options chain data loader using Delta Lake.

## ✨ Features

- **Parallel file processing** ⚡ - Configurable concurrent processing of flat files with live progress bars
- **Streaming CSV loader** - Memory-efficient processing with Polars lazy evaluation
- **Delta Lake storage** - ACID transactions, time travel, and efficient columnar storage
- **Automatic ticker parsing** - Extracts underlying symbol, expiration, strike, option type from options tickers
- **Polygon API integration** - Download flatfiles and sync splits via official API
- **Stock splits support** - Automatic split-adjusted prices with validation against Polygon
- **Split validation** - Spot-check adjusted prices to ensure data accuracy
- **Incremental updates** - Daily workflow downloads only new data and applies splits
- **Optimized schema** - Separate Delta tables for stocks, options, splits, and ticker metadata
- **Easy database reset** - Clean slate for development and testing

## 📦 Installation

```bash
uv sync
```

## 🔧 Configuration

Create a `.env` file with your configuration:

```env
# Polygon API 🔑
POLYGON_API_KEY=your_polygon_api_key_here

# Polygon S3 Flatfiles Access (for source data) 📦
POLYGON_FLATFILES_ACCESS_KEY=your_polygon_flatfiles_access_key_here
POLYGON_FLATFILES_SECRET_KEY=your_polygon_flatfiles_secret_key_here

# Delta Lake Storage Configuration ☁️
# S3 is the default! Change to "local" only for development/testing
STORAGE_BACKEND=s3  # "s3" or "local"
STORAGE_PATH=s3://major-optionality/delta  # S3 URI or local path (e.g., ./data)

# AWS Configuration (for Delta Lake S3 storage) 🔐
# NOTE: Leave these empty to use IAM role / OIDC (recommended for GitHub Actions)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=  # Optional - leave empty to use IAM role
AWS_SECRET_ACCESS_KEY=  # Optional - leave empty to use IAM role

# Performance (optional - tune for your system)
MAX_CONCURRENT_DOWNLOADS=2          # Simultaneous downloads from Polygon (default: 2)
MAX_CONCURRENT_FILE_PROCESSING=2    # Simultaneous CSV file processing (default: 2)
```

### 🎯 Storage Backend Options

**S3 Storage (Default)** ☁️
- Delta tables stored in AWS S3
- Perfect for GitHub Actions and cloud deployments
- Automatically uses IAM roles / OIDC for authentication
- Cost-effective with lazy reading (only fetches needed data)
- Example: `STORAGE_PATH=s3://major-optionality/delta`

**Local Storage** 💾
- Delta tables stored locally on disk
- Good for development and testing
- Faster for small datasets
- Example: `STORAGE_PATH=./data`

### 🔐 AWS Authentication

The project supports multiple AWS authentication methods:

1. **IAM Role / OIDC (Recommended for GitHub Actions)**
   - Leave `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` empty
   - GitHub Actions uses OIDC to assume the IAM role
   - No long-lived credentials needed!

2. **Environment Variables (Local Development)**
   - Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`
   - Or use AWS CLI profiles

3. **Default Credential Chain**
   - Uses `~/.aws/credentials` or environment variables
   - Falls back to instance metadata (EC2/ECS)

## 🚀 Usage

### Core Commands

There are four main commands for managing your data:

#### 1️⃣ **update** - Run incremental load 🌟

Runs the complete daily update workflow:
- ✅ Checks if Polygon flatfiles are available
- ✅ Syncs yesterday's stock splits from Polygon API
- ✅ Downloads and loads new stock data
- ✅ Downloads and loads new options data
- ✅ Validates split adjustments (5 random spot checks)
- ✅ Shows database statistics

```bash
# Using uv directly
uv run optionality update

# Or using Make
make update
```

**Automation tip**: 🤖 Use GitHub Actions for automatic updates!

The project includes a GitHub Actions workflow that:
- ✅ **Runs hourly** to check for new data availability
- ✅ Automatically detects when Polygon files arrive (typically between 8PM ET day-of and 11AM ET next day)
- ✅ Skips gracefully when files aren't ready yet (no failed runs!)
- ✅ Authenticates with AWS using OIDC (no long-lived credentials!)
- ✅ Updates Delta tables directly in S3 when files are available
- ✅ Runs verification spot checks
- ✅ Shows statistics after completion

**To use GitHub Actions:**
1. Set up repository secrets:
   - `POLYGON_API_KEY`
   - `POLYGON_FLATFILES_ACCESS_KEY`
   - `POLYGON_FLATFILES_SECRET_KEY`
2. Configure AWS IAM role for OIDC (already set up: `arn:aws:iam::911986281031:role/github-actions-optionality`)
3. The workflow runs automatically every hour (or manually trigger from the Actions tab)
4. Files are detected automatically - no manual intervention needed! 🎉

**For local automation**, use a cron job:
```bash
# Add to your crontab (runs at 6 PM daily after market close):
0 18 * * * cd /path/to/optionality && uv run optionality update
```

#### 2️⃣ **check-files** - Check data availability 🔍

Quickly checks if yesterday's flatfiles have arrived from Polygon yet:
- Checks both stocks and options files in Polygon S3 bucket
- Uses NYC timezone to determine "yesterday"
- Shows current NYC time and file availability status
- Fast check (completes in ~1 second)

```bash
# Using uv directly
uv run optionality check-files

# Or using Make
make check-files
```

**Exit codes**:
- `0` = Both files available (ready to update) ✅
- `2` = Files not yet available (waiting for Polygon) ⏳
- `1` = Error occurred ❌

**When to use this**: Perfect for automation! The GitHub Actions workflow uses this command hourly to detect when new data arrives, so you don't waste time running full updates before files are ready.

#### 3️⃣ **verify** - Run spot checks 🔍

Validates stock split adjustments by comparing your data against Polygon's adjusted prices:
- Checks random sample of tickers with splits
- Tests historical dates AND critical dates around split execution
- Uses 0.1% tolerance for price comparisons
- Exits with error code if any validation fails

```bash
# Using uv directly
uv run optionality verify

# Or using Make
make verify
```

**What gets validated**: ✅
- Historical prices before splits (at least 30 days before)
- **Critical dates around split execution** (day before, execution day, day after)
- Comparison against Polygon's adjusted prices with 0.1% tolerance

#### 4️⃣ **clean** - Delete all data 🗑️

⚠️ **Warning**: This will delete ALL Delta tables! Use with caution.

```bash
# Using uv directly
uv run optionality clean

# Or using Make
make clean
```

## 🗄️ Delta Lake Schema

All data is stored as Delta Lake tables with ACID transactions and time travel support.

### `tickers` Table

- Columns: ticker, name, market, locale, primary_exchange, type, active, currency_name, currency_symbol, cik, composite_figi, share_class_figi, last_updated_utc, delisted_utc
- Enriched metadata from Polygon API
- Partitioned by: `ticker`

### `stocks` Table

- Columns: ticker, volume, open, close, high, low, window_start, transactions
- Contains **raw (unadjusted)** OHLCV prices
- Partitioned by: `ticker`
- Split adjustments are applied at query time using the `splits` table

### `splits` Table

- Columns: ticker, execution_date, split_from, split_to, split_factor
- Corporate action data for stock splits (forward and reverse)
- Example: AAPL 4:1 split on 2020-08-31 → split_from=1, split_to=4, split_factor=4
- Partitioned by: `ticker`

### `options` Table

- Columns: underlying_symbol, expiration_date, option_type, strike_price, volume, open, close, high, low, window_start, transactions, original_ticker
- Options chain OHLCV data
- Partitioned by: `underlying_symbol`

### 📐 Understanding Split Adjustments

Stock splits change the number of shares and price per share, but not the total market value. To compare historical prices accurately, we need to adjust old prices for splits that happened later.

#### 🎯 What is a Stock Split?

**Example**: Apple (AAPL) had a 4:1 split on August 31, 2020
- **Before split**: 1 share at $400 = $400 value
- **After split**: 4 shares at $100 = $400 value (same total value)
- **Adjusted historical price**: Old $400 price becomes $100 in split-adjusted terms

#### 🔍 How We Calculate Adjusted Prices

We use **forward adjustment**, the industry-standard method used by financial data providers:

1. **Raw prices** are stored in the `stocks` table exactly as they were quoted on that date
2. **Adjusted prices** are calculated at query time by dividing historical prices by the cumulative split factor
3. For splits that occur ON OR AFTER a date, we apply the adjustment to that date
   - This is critical: the price on split execution day is still the pre-split price!
   - Example: If AAPL splits 4:1 on 2020-08-31, prices from 2020-08-31 backward are divided by 4

#### 📊 Split Adjustment Formula

```
adjusted_price = raw_price / cumulative_split_factor
```

Where `cumulative_split_factor` is the product of all split factors from splits that occurred on or after each date.

**Example Timeline**:
- **2020-08-28**: Raw close $500 → Adjusted: $500 / 4 = $125 (split happens after this date)
- **2020-08-31**: Raw close $500 → Adjusted: $500 / 4 = $125 (split execution day - still pre-split price!)
- **2020-09-01**: Raw close $125 → Adjusted: $125 / 1 = $125 (split already happened, no adjustment)

#### ✅ Validation Against Polygon

We validate our adjusted prices against Polygon's adjusted prices, checking:
- Random historical dates before splits
- **Critical dates around split execution** (day before, execution day, day after)
- This ensures our adjustment logic matches market standards

#### 📋 When to Use Raw vs Adjusted Prices

- **Use adjusted prices** for:
  - Technical analysis and charting 📈
  - Calculating returns and performance metrics 📊
  - Any historical price comparisons 🔍
  - Options pricing models (use underlying's adjusted price)

- **Use raw prices** only when:
  - You need the exact historical price as it was quoted 📰
  - Reconstructing historical market conditions
  - Academic research requiring unadjusted data

## 📝 Example Queries

Delta Lake tables can be queried using DuckDB or Polars - works with both S3 and local storage!

### 🚀 Using Polars (Recommended for S3!)

Polars uses lazy evaluation and predicate pushdown for efficient S3 queries:

```python
import polars as pl
from optionality.storage.delta_manager import delta

# 🔍 Lazy scans are CRITICAL for S3 performance!
# Only fetches data you actually need

# Get Apple stock data (lazy scan with filter pushdown)
aapl = (
    delta.scan_stocks_adjusted(ticker='AAPL')  # 🎯 Partition pruning!
    .filter(pl.col('window_start') >= '2024-01-01')
    .collect()  # Only now does it fetch from S3
)

# Get recent splits
recent_splits = (
    delta.scan_splits()
    .filter(pl.col('execution_date') >= '2024-01-01')
    .collect()
)

# Find active NASDAQ stocks
nasdaq = (
    delta.scan_tickers(active_only=True)
    .filter(pl.col('primary_exchange') == 'NASDAQ')
    .collect()
)

# Efficient aggregation with lazy evaluation
top_options = (
    delta.scan_options(underlying_symbol='AAPL')  # 🎯 Partition pruning!
    .group_by('expiration_date')
    .agg(pl.col('volume').sum().alias('total_volume'))
    .sort('total_volume', descending=True)
    .limit(10)
    .collect()  # Minimal data transfer from S3!
)
```

### 💡 S3 Query Tips

**DO** ✅
- Use lazy scans (`delta.scan_*()`) instead of eager reads
- Filter by partition keys (`ticker`, `underlying_symbol`) for partition pruning
- Apply filters before `.collect()` to minimize S3 data transfer
- Use the delta manager's built-in scan methods

**DON'T** ❌
- Use `pl.read_delta()` for S3 (loads everything into memory!)
- Collect entire tables and then filter (wastes S3 egress)
- Skip partition filters when querying specific tickers

### Using DuckDB

DuckDB can also query Delta tables (both local and S3):

```python
import duckdb
from optionality.config import get_settings

settings = get_settings()
storage_path = settings.storage_path

# Connect to Delta tables
con = duckdb.connect()

# Get all splits for Apple
con.execute(f"""
    SELECT * FROM delta_scan('{storage_path}/splits')
    WHERE ticker = 'AAPL'
    ORDER BY execution_date
""").df()

# Top symbols by options volume
con.execute(f"""
    SELECT underlying_symbol, COUNT(*) as contract_count
    FROM delta_scan('{storage_path}/options')
    GROUP BY underlying_symbol
    ORDER BY contract_count DESC
    LIMIT 10
""").df()
```

## ⚡ Performance

- **S3-optimized lazy reading** 🚀: Polars lazy evaluation + predicate pushdown = minimal S3 data transfer
- **Parallel processing**: Configurable concurrent file downloads and processing (default: 2 workers each)
- **Lazy loading**: Processes data in 10K row batches using Polars
- **Columnar storage**: Delta Lake's Parquet format for efficient compression and queries
- **Memory efficient**: Handles large CSV files with minimal memory usage
- **Incremental updates**: Only downloads and processes new data each day
- **Partitioning**: Tables partitioned by ticker/underlying_symbol for fast queries (partition pruning!)
- **Live progress tracking**: Rich progress bars show real-time processing status for all files

### 💰 S3 Cost Optimization

The default S3 configuration minimizes costs:
- ✅ **Lazy reads**: Only fetch data you need (not entire tables)
- ✅ **Partition pruning**: Filter by ticker to read only relevant partitions
- ✅ **Predicate pushdown**: Filters applied at S3 level, not after download
- ✅ **Incremental updates**: Only process new dates
- ✅ **Same region**: GitHub Actions runners in us-east-1 (no cross-region transfer fees)

## 🧪 Testing

Run tests:

```bash
uv run pytest
```

Run with coverage:

```bash
uv run pytest --cov=optionality
```

## 📂 Data Format

Expected file structure:

```
flatfiles/
├── stocks/
│   └── YYYY/MM/YYYY-MM-DD.csv.gz
└── options/
    └── YYYY/MM/YYYY-MM-DD.csv.gz
```

CSV format (both stocks and options):

```
ticker,volume,open,close,high,low,window_start,transactions
```

Options ticker format: `O:{SYMBOL}{YYMMDD}{C/P}{STRIKE*1000}`

- Example: `O:AMD231215C00150000` → AMD, 2023-12-15, Call, $150.00
