# automated-excel-pipeline
An intelligent, high-performance Excel data processing system that automates end-to-end data preparation workflows. The system detects new Excel files, normalizes inconsistent column headers using fuzzy logic and database mapping, filters out irrelevant columns and rows, and merges all valid files into a unified output—organized by session.

Key Features
File Monitoring: Automatically detects and processes Excel files placed in a watch folder.
Smart Header Normalization: Renames column headers using a dictionary and fuzzy matching to ensure consistency across datasets.
Data Cleaning: Removes unnecessary columns and filters out rows based on dynamic database-defined rules.
Session-Based Queueing: Uses a queue data structure to manage session-specific file tracking for accurate batch processing.
Merging Engine: Efficiently consolidates cleaned data files into a single output .xlsx or .csv.
Technical Stack
Languages: Python
Libraries: pandas, watchdog, openpyxl, rapidfuzz, sqlite3
Data Structures: Queue (session management), Dictionary (header mapping)

Why This Project?
Managing inconsistent Excel files is a common bottleneck in data pipelines. This tool eliminates manual intervention, supports data ingestion, and ensures clean, normalized output for downstream tasks like analytics, reporting, or machine learning.

Real-World Impact
Handled more than 40 Excel files (~200MB) per batch
Reduced manual preprocessing time by 90%
Used in freelance client delivery and personal data workflows

Usage
Simply drag and drop .xlsx files in the monitored folder.
The system auto-detects and processes them one-by-one.
Cleaned,standardized and filtered files are merged into a single output file.
