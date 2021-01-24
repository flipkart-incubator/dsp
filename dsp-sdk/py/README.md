# Python DSPSDK
`dspsdk` is a lightweight development kit library to easily share and access datasets, evaluation metrics or any other python object between workflows steps, across local and remote env for Decision Science Platform (DSP).

`dspsdk` has `blob` which provide many interesting features:
- Built-in integration with DSP for data versioning with respect to workflow runs
- local access of data/metrics generated in remote DSP env for easy debugging
- support for ad-hoc override of dataset from local env which can be used in DSP runs

# Installation

# Usage

`dspsdk` is very simple to use, the main methods are:
- `blob.Blob(request_id)` to instantiate a new blob python object binded with request_id
- `list_all()` to list all variables saved
- `dump('variable_name', python_object)` to save python_object as variable_name
- `load('variable_name')` to load back a previously saved python_object

Here is a quick example:
```python
from dspsdk import blob

# instantiate blob
my_blob = blob.Blob("my_unique_request_id")

a = "hello"
b = {"my_key": 1.2} 

# save a and b as string_var and dict_var respectively
my_blob.dump("string_var", a)
my_blob.dump("dict_var", b)

# print all saved variables
print(my_blob.list_all())

# load back saved variables
loaded_string = my_blob.load("string_var")
loaded_dict = my_blob.load("dict_var")
```

A dsp worklflow example with partition handling can be found here:
https://github.fkinternal.com/Flipkart/dsp-sandbox-trial/tree/master/blob/py

# Developer Guide
For publishing package to flipkart artifactory using artcli

```bash
pip install setuptools wheel
python setup.py sdist bdist_wheel

~/artcli create --store pypi --packagename dspsdk --version 1.0.0 --package_path dist/dspsdk-1.0.0-py3-none-any.whl
```
