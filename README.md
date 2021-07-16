# About Polly Library
Polly Libraries give access to the various capabilities on Polly like querying, filtering and accessing the data on Polly OmixAtlas. It allows access to data in OmixAtlas over any computational platform (like DataBricks, SageMaker, Polly, etc.) of your choice. These functionalities can be accessed through functions in python and bash which can be used over a Terminal.

# About Polly Python
Polly Python library provides convenient access to the above-mentioned functionalities through function in Python language.

# Installation
## Install Polly Python using pip
```
pip install polly-python
```

# Getting started
## Import from libraries
The following libraries need to be imported over the development environment to access the data.

```
from polly.omixatlas import OmixAtlas
import pandas as pd
from json import dumps
```

# Authentication
Authentication of the account is required to be able to access the capabilities of the Polly Python library
## Copying the token for authentication

1. Go to Polly
2. Click the User Options icon from the left-most panel
3. Click on Authentication on the panel that appears
4. Click on Copy to copy the authentication token

# Using the token
The following code is required to add the authentication function in the Polly Python library

```
AUTH_TOKEN = "[authentication_token_copied]"
library_client = OmixAtlas(AUTH_TOKEN)
```
# OmixAtlas
## Calling a function
Use the response from the authentication token to call any function. E.g.

```
output = library_client.[function()]
```

The output of the functions is in JSON and/or data frame formats. You can print/download this output.

## Functions in Polly Python
### 1. Get details of all OmixAtlases
The following function details all the OmixAtlases accessible by you.

```
get_all_omixatlas() 
```
The output of this function would be JSON containing
```
{'data': 
[
  {'repo_name': 'name', 
    'repo_id': 'id', 
    'indexes': 
    { 
      'gct_metadata': 'abc', 
      'h5ad_metadata': 'abc', 
      'csv': 'abc', 
      'files': 'abc', 
      'json': 'abc', 
      'ipynb': 'abc', 
      'gct_data': 'abc', 
      'h5ad_data': 'abc'
    }, 
    'dataset_count': 123, 
    'disease_count': 123, 
    'diseases': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'organism_count': 123, 
    'organisms': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'sources': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'datatypes': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'sample_count': 123
    }, 
    {...},
    {...}
  ]
}
```
### 2. Get the summary of any OmixAtlas
The following function details a particular OmixAtlas. The key/repo id of this OmixAtlas can be identified by calling the get_all_omixatlas() function.

```
omixatlas_summary(”[repo_id OR repo_name]”)
```
The output of this function would be JSON containing

```
{'data': 
  {
    'repo_name': 'name', 
    'repo_id': 'id', 
    'indexes': 
    { 
      'gct_metadata': 'abc', 
      'h5ad_metadata': 'abc', 
      'csv': 'abc', 
      'files': 'abc', 
      'json': 'abc', 
      'ipynb': 'abc', 
      'gct_data': 'abc', 
      'h5ad_data': 'abc'
    }, 
    'dataset_count': 123, 
    'disease_count': 123, 
    'diseases': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'organism_count': 123, 
    'organisms': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'sources': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'datatypes': ['abc', 'bcd', 'cde', 'def', 'efg', 'fgh', 'ghi', 'hij', 'ijk'], 
    'sample_count': 123
  }
}
```
### 3. Querying the data and the metadata
To access, filter, and search through the metadata schema, the function mentioned below can be used:

```
 query_metadata(“[query_written_in_SQL]”) 
```
Refer to the Queries section to understand how you could write a query in SQL. The columns returned would depend on the query that was written. The output of the function is a dataframe or a JSON depending on the operations used in the query.

### 4. Downloading any dataset
To download any dataset, the following function can be used to get the signed URL of the dataset.

```
 download_data(”[repo_name OR repo_id]”, “[dataset_id]”)
```
The `[repo_name OR repo_id]` of this OmixAtlas can be identified by calling the `get_all_omixatlas()` function. The `[dataset_id]` can be obtained by querying the metadata at the dataset level using `query_metadata(“[query written in SQL]”)`.

The output of this function is a signed URL. The data can be downloaded by clicking on this URL. The output data is in .gct format except for single cell data.
