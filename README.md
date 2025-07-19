# automated-excel-pipeline
An automated Excel pipeline that normalizes headers, removes unnecessary columns, filters rows, and merges session-specific files using a queue-based system.

Automatic File Detection: Monitors a designated folder using watchdog to detect incoming Excel files.
Header Normalization: Renames inconsistent column headers using a pre-defined database and fuzzy matching techniques.
Column Filtering: Retains only required columns defined in a configurable list.
Row Filtering: Applies custom filters (e.g., ignore values, conditions) to preserve only relevant data rows.
Session-Based Merging: Combines only files added in the current session into a single output file.
Output Export: Saves cleaned and merged data as a .xlsx
