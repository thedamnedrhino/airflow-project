# Tolldata processing

## Pipeline Steps:

### 1. Download tolldata. Output: `tolldata.tgz`.
-- via airflow `BashOperator`.

### 2. Extract tolldata files. Output: `tolldata_unzipped`.
-- via airflow `BashOperator`.

### 3. Preprocess `vehicle-types.csv`. Output: `csv.csv`. 
-- pandas and Python `@task` decorator.

### 4. Preprocess `tolldata-data.tsv` -> Output: `tsv.csv`. 
-- pandas and Python `@task` decorator.

### 5. Preprocess `payment-data.txt`. Output: `fwf.csv`. 
-- pandas and Python `@task` decorator.

### 6. Merge the results. Output: `merged.csv`. 
-- pandas and Python `@task` decorator.

### 7. Transform the result. Output: `result.csv`.
-- pandas and Python `@task` decorator.

### 8. Test the results.
-- pandas, Python `@task` decorator and `assert` statements`.
