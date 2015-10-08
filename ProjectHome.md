A script for converting csv files to postgresql tables using python-psycopg2.
It may be used to convert a single file, or every .csv file in a directory.

Automatically creates a table with the corresponding columns, assuming that the csv file has a one-row header. The data types are automatically selected among text, double precision, or bigint.

Assumes the database connection is managed by the user's settings, for example with a .pgpass file on linux.

The null value may be automatically detected among the double quotes (""), the empty string or NA.

usage: python csv2pgsql.py [-h] [--host HOST] [-u USERNAME] [-db DATABASE] [-n NULL] [--fixednull] [-v] [--autodrop] src

positional arguments:

> src                   Directory containing the csv files

optional arguments:

> -h, --help            show this help message and exit

> --host HOST           Host of the database

> -u USERNAME, --username USERNAME Name of the user who connects to the database

> -db DATABASE, --database DATABASE Name of the database

> -n NULL, --null NULL  Null String in the CSV file

> --fixednull           Do not check for the null string

> -v, --verbosity       Increase output verbosity

> --autodrop            Drop table if it already exists


The error messages are not correct when the -v (or higher) flag is not set. Exception catching and debugging in general could use some refinements.