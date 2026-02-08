## Data Exploration & Analysis

### 1. Dataset Overview
The dataset contains current job postings available on the City of New York’s official jobs site.

- **Total Rows**: 2,946
- **Total Columns**: 28
- **File Format**: CSV

### 2. Column Analysis
The dataset is a mix of numerical, categorical, and unstructured text data.

| Column Name         | Data Type   | Description                                              | Key Observations & Notes                                                                                             |
|---------------------|-------------|----------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Job ID              | Numerical   | Unique identifier for the job.                           | Primary key, can be leveraged for deduplication.                                                                     |
| Agency              | Categorical | The hiring agency (e.g., DEPT OF BUSINESS SERV).         | 52 unique agencies are hiring.                                                                                       |
| Posting Type        | Categorical | Type of posting (Internal or External).                  | Most jobs are posted twice (once for each type). Need to club data for better insights & create an indicator column. |
| # Of Positions      | Numerical   | Number of openings.                                      | N/A                                                                                                                  |
| Job Category        | Categorical | Occupational category (e.g., Engineering, Architecture). | Contains 130 unique categories.                                                                                      |
| Salary Range From / To  | Numerical   | The lower bound (minimum) of the salary.                 | Ranges from $0 to $234,402 (across salary frequencies).                                                              |                                                         |
| Salary Frequency    | Categorical | Frequency of pay (Annual, Hourly, Daily).                | Salaries are not on the same scale, normalization is required.                                                       |
| Civil Service Title | Categorical | Official civil service title.                            | High cardinality text field.                                                                                         |
| Business Title      | Categorical | Specific business title for the role.                    | High cardinality text field.                                                                                         |
| Preferred Skills    | Text        | Unstructured text describing skills.                     | Requires text processing to extract individual skills.                                                               |
| Posting Date        | DateTime    | Date the job was posted.                                 | Used to identify freshest data. Currently string; needs conversion to DateType.                                      |
| Post Until          | DateTime    | Date the posting expires.                                | Currently string, needs conversion to DateTime.                                                                      |
| Posting Updated     | DateTime    | Date the posting was last updated.                       | Currently string, needs conversion to DateTime.                                                                      |
| Process Date        | DateTime    | Date the data was processed.                             | Currently string, needs conversion to DateTime.                                                                      |


### 3. Data Quality & Missing Values

| Column Name         | Missing Count | % Missing   | Analysis                                                      |
|---------------------|---------------|-------------|---------------------------------------------------------------|
| Recruitment Contact | 2,946         | 100%        | Safe to drop                                                  |
| Post Until          | 2,075         | 70%         | Can indicate that the jobs are still open until the candidate is hired                                                              |
| Hours/Shift         | 2,062         | 70%         | Ignore for general analysis.                                  |
| Work Location 1     | 1,588         | 54%         | Use Work Location as it does not have any missing values      |
| Preferred Skills    | 393           | 13%         | Clean the skills columns & remove stop words                  |
| Job Category        | 2             | Less than 1%|                                                               |


### 4. Findings based on data

#### 4.1. Data Duplication (Internal vs. External)
Upon inspecting Job ID, we found that a single ID often appears twice: once with Posting Type = Internal and once as External.

- **Impact**: Aggregating data directly on the raw file will result in double-counting job openings and skewing salary averages.

- **Resolution**: We must perform a deduplication step, grouping by Job ID and merging the Posting Type into a single indicator (e.g., Internal/External).

#### 4.2 Salary Variance

The Salary Range From varies from 0 to 218,587 ( averaging $50/hr with $50,000/yr) , As the raw salary columns are mix of Hourly, Daily and Annual rates. 

- **Impact**: A direct comparision (e.g., Hourly Rate $30 vs Annual Salary $50,000) would result in wrong calculations which will wrongly indicate KPIs like Highest Salary ,Lowest Salary ,etc

- **Resolution**:  We should annulize the salary or in other words normalize the salary range by introducing salary rates on annual level.For Normalization we would consider 2080 hours/year for Hourly, 260 days/year for Daily

#### 4.3 Unstructured Data
Columns like Minimum Qual Requirements and Preferred Skills contain dense, unstructured text.

- **Resolution**: We will use string manipulation (splitting & filtering stop words) to extract meaningful keywords for the skills related KPIs.


### 5. Column Removal 
Some of the columns can be dropped from the dataframe as they would not be relevant for our analysis ( This assumptions are made based on the requirements of the challenge)

| Column Name            | Reason for Dropping                                                                                                                |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| Recruitment Contact    | 100% Null. It contains no data.                                                                                                    |
| Post Until             | High Null Rate (70%) , we can use Posting Date or Posting Updated. This column is redundant and mostly empty.          |
| Hours/Shift            | High Null Rate (70%), there is no direct relation to Requested KPIs.                                              |
| Work Location 1        | High Null Rate (54%). High null rates , we can leverage Work Location column information.                                                                              |
| To Apply               | Irrelevant Text , Contains instructions like "Click here to apply," which provides no analytical value for salary or category KPIs. |
| Residency Requirement  | Irrelevant Text, Residency Legal & Eligibility text not needed for the requested analysis.                                                     |
| Additional Information | Irrelevant Text, As these are administrative notes that do not contribute to the skills or salary related analysis.                              |
| Process Date           | This is an internal system timestamp. Updating Posting Date can be leveraged instead for analysis             |


# Unit Test Cases Overview (init_test_cases_notebook.ipynb)

The `unit_test_cases_notebook.ipynb` contains comprehensive unit tests for all data processing functions used in the assessment notebook. These tests ensure data transformation functions work correctly and produce expected outputs.

### Test Infrastructure

- **Testing Framework**: PySpark DataFrames with local Spark Session
- **Logging**: Configured logging to track test execution and results
- **Total Tests**: 11 comprehensive unit test cases
- **Test Status**: All tests passed successfully

### Unit Test Cases

#### Test 1: remove_special_characters
- **Purpose**: Validates removal of special characters from text fields
- **Input Scenarios**: 
  - Text with special characters: `hello@world!` → `helloworld`
  - Mixed alphanumeric with symbols: `test#data$` → `testdata`
  - Test maintains hyphens and commas: `abc-123,456` → `abc-123,456`
- **Assertions**: Verifies special characters are removed while preserving alphanumeric characters and specific symbols

#### Test 2: convert_to_numeric
- **Sub-test 2.A**: Integer conversion
  - Converts string currency values to integers: `$100` → `100`
  - Extracts numeric values from mixed strings: `$300abc` → `300`
- **Sub-test 2.B**: Double conversion  
  - Converts decimal currency values: `$100.50` → `100.50`
  - Handles mixed decimal strings: `$300.99abc` → `300.99`
- **Purpose**: Ensures proper numeric type conversion for salary and amount fields

#### Test 3: convert_to_datetime
- **Purpose**: Validates string-to-timestamp conversion for date fields
- **Input Scenarios**: ISO 8601 formatted datetime strings (e.g., `2020-01-15T10:30:45.000`)
- **Assertions**: Verifies the resulting column has timestamp data type

#### Test 4: convert_to_tilecase
- **Purpose**: Converts text to title case (proper capitalization)
- **Input Scenarios**:
  - `hello world` → `Hello World`
  - `PYSPARK CODE` → `Pyspark Code`
  - Handles leading/trailing spaces: `  python programming  ` → `Python Programming`
- **Assertions**: Verifies proper title case formatting with correct capitalization

#### Test 5: remove_duplicates
- **Purpose**: Removes duplicate records based on dedup grain, keeping most recent based on order grain
- **Input Scenarios**: 
  - Multiple records with same ID but different dates
  - Deduplication on `id` column, ordering by `date` in descending order
- **Assertions**: 
  - Verifies row count reduced to 2 (from 4 duplicates)
  - Confirms latest dates are retained: ID 1 → `2020-01-02`, ID 2 → `2020-01-03`

#### Test 6: col_rename_with_mapping
- **Purpose**: Renames DataFrame columns based on a JSON mapping file
- **Input Scenarios**: 
  - Mapping file with old-to-new column name pairs
  - `old_col1` → `new_col1`, `old_col2` → `new_col2`
- **Assertions**: 
  - Verifies new column names exist in output
  - Confirms old column names are removed

#### Test 7: drop_columns
- **Purpose**: Removes specified columns from DataFrame
- **Input Scenarios**: DataFrame with 4 columns (id, name, title, salary)
- **Columns to drop**: `title`, `salary`
- **Assertions**: 
  - Confirms dropped columns removed (`title`, `salary`)
  - Verifies remaining columns preserved (`id`, `name`)
  - Validates final column count equals 2

#### Test 8: annualize_salary
- **Purpose**: Normalizes salary ranges from different frequencies to annual basis
- **Input Scenarios**:
  - Annual salary: `1000` remains `1000` (unchanged)
  - Hourly salary: `10` → `20800` (multiplied by 2080 hours/year)
  - Daily salary: `100` → `26000` (multiplied by 260 days/year)
- **Assertions**: Verifies correct annualization factors applied based on salary frequency

#### Test 9: create_qualification_indicator
- **Purpose**: Creates binary indicator for degree requirements in qualification text
- **Input Scenarios**:
  - Contains degree: `Bachelor's degree required` → `is_degree_req = 1`
  - No degree: `High school diploma` → `is_degree_req = 0`
  - Alternative degree: `Master's degree preferred` → `is_degree_req = 1`
- **Assertions**: Verifies correct detection of degree-related keywords in qualification requirements

#### Test 10: display
- **Purpose**: Validates basic DataFrame structure and row/column counts
- **Input Scenarios**: DataFrame with columns (id, name, value)
- **Assertions**: 
  - Confirms row count: 3
  - Confirms column count: 3

#### Test 11: export_to_csv
- **Purpose**: Exports DataFrame to CSV file with proper formatting
- **Input Scenarios**: 
  - DataFrame with 3 rows and 2 columns (id, name)
  - Export to temporary directory
- **Assertions**: 
  - Verifies output file exists at expected path
  - Confirms CSV structure: header row + 3 data rows = 4 total lines
  - Validates header contains expected column names (`id`, `name`)

### Test Execution Summary

| Test ID | Function | Status |
|---------|----------|--------|
| Test 1 | remove_special_characters | ✓ PASSED |
| Test 2A | convert_to_numeric (int) | ✓ PASSED |
| Test 2B | convert_to_numeric (double) | ✓ PASSED |
| Test 3 | convert_to_datetime | ✓ PASSED |
| Test 4 | convert_to_tilecase | ✓ PASSED |
| Test 5 | remove_duplicates | ✓ PASSED |
| Test 6 | col_rename_with_mapping | ✓ PASSED |
| Test 7 | drop_columns | ✓ PASSED |
| Test 8 | annualize_salary | ✓ PASSED |
| Test 9 | create_qualification_indicator | ✓ PASSED |
| Test 10 | display | ✓ PASSED |
| Test 11 | export_to_csv | ✓ PASSED |

**Total Tests**: 12 | **Passed**: 12 | **Failed**: 0 | **Success Rate**: 100%


# User Functions (user_functions.py)

Reusable PySpark utility functions for data processing:

| Function | Purpose | Key Parameters |
|----------|---------|-----------------|
| `display(df)` | Display DataFrame in tabular format | df |
| `remove_special_characters(df, col)` | Remove special chars using regex | df, column_name |
| `convert_to_numeric(df, col, to_double)` | Extract and convert numbers | df, column_name, to_double flag |
| `convert_to_datetime(df, col)` | Convert ISO 8601 strings to timestamp | df, column_name |
| `convert_to_tilecase(df, col)` | Convert text to title case | df, column_name |
| `col_rename_with_mapping(df, path)` | Rename columns from JSON mapping | df, mapping_file_path |
| `drop_columns(df, cols_list)` | Remove specified columns | df, columns_to_drop list |
| `remove_duplicates(df, dedup, order, desc)` | Deduplicate using window functions | df, dedup_grain, order_grain, is_desc |
| `annualize_salary(df)` | Normalize salary to annual basis | df (creates 4 new columns) |
| `create_qualification_indicator(df)` | Add degree requirement binary flag | df (creates is_degree_req column) |
| `export_to_csv(df, path, name)` | Export DataFrame to CSV | df, output_path, file_name |


# Assessment Notebook (assesment_notebook.ipynb)

Main data processing pipeline :

**Pipeline Stages:**
1. **Setup**: Initialize Spark, import libraries, configure logging
2. **Load**: Read CSV (2,946 rows × 28 columns)
3. **EDA**: Analyze column types, nulls, statistics using Pandas
4. **Preprocess**: Rename columns, drop irrelevant columns, clean text, convert types, deduplicate rows
5. **Feature Engineering**: Annualize salaries, add degree requirement indicator
6. **Analysis**: Run 7 analytical queries

**Key Queries:**
- Top 10 job categories by posting count
- Salary distribution by job category
- Correlation between degree requirement and salary (positive correlation found)
- Highest paid position per agency
- Average salary trends (last 2 years)
- Top 20 highest paid skills (with frequency filter > 10)

**Output:** `/dataset/cleaned/nyc-jobs-cleaned.csv` (1,640 rows × 27 columns)
