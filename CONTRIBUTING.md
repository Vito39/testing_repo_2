## Steps to create a release branch
* In the `polly/__init__.py` update the `__version__` variable to the version that is going to get released next.
* The above should be the first commit for the `release branch` which will be named according to the branch naming `release_vx.x.x`.

## Steps to raise a Pull request
* First format all the files using black command i.e. `black {source_file_or_directory}...`.
* Check if the build `unit_testing.yml` has all the checks passed.
* Check for `merge-conflicts`, if there then rebase.
