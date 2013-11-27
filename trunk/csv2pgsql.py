""" Written by Cedric Boittin, 13/11/2013
    A script for converting csv files to postgresql tables using python-psycopg2. It may be used to convert a single file, or every .csv file in a directory.
    Automatically creates a table with the corresponding columns, assuming that the csv file has a one-row header. The data types are automatically selected among text, double precision, or bigint.
    Assumes the database connection is managed by the user's settings, for example with a .pgpass file on linux.
    The null value may be automatically detected among the double quotes (""), the empty string or NA. 

usage: python csv2pgsql.py [-h] [--host HOST] [-u USERNAME] [-db DATABASE] [-n NULL] [--fixednull] [-v] [--autodrop] src

positional arguments: 
    src Directory containing the csv files 

optional arguments: 
    -h, --help show this help message and exit
    --host HOST Host of the database
    -u USERNAME, --username USERNAME Name of the user who connects to the database 
    -db DATABASE, --database DATABASE Name of the database
    -d DELIMITER, --delimiter DELIMITER Delimiter in the csv file
    -n NULL, --null NULL Null String in the CSV file
    --fixednull Do not check for the null string
    -v, --verbosity Increase output verbosity
    --autodrop Drop table if it already exists
"""

import csv
import psycopg2 as pg
import os.path
import argparse
import re
from subprocess import call

parser = argparse.ArgumentParser()
parser.add_argument('src', help="Directory containing the csv files")
parser.add_argument("--host", default="localhost", help='Host of the database')
parser.add_argument('-u', "--username", default="YOUR_USERNAME_HERE", 
                help='Name of the user who connects to the database')
parser.add_argument('-db', "--database", default="YOUR_DB_HERE", help='Name of the database')
parser.add_argument('-d', '--delimiter', default=',', help='Delimiter in the csv file')
parser.add_argument('-n', '--null', default="\"\"", help='Null String in the csv file')
parser.add_argument('--fixednull', action='store_true', help='Do not check for the null string')
# parser.add_argument('--debug', action='store_true', help='Debug information on/off')
parser.add_argument('-v', '--verbosity', action="count", help="Increase output verbosity")
parser.add_argument('--autodrop', action='store_true', help='Drop table if it already exists')
args = parser.parse_args()

src = args.src
dbname = args.database
debug = args.verbosity
userName = args.username
hostName = args.host
nullString = args.null
autoDrop = args.autodrop
fixedNullString = args.fixednull
csvDelimiter = args.delimiter
# Check presence of potential null strings
hasNullString = {"NA":False, "\"\"":False, "":False}

nRowsToParse = 10
systematicReading = True #The False flag remains to be managed
nonnumericRegex = re.compile("[^\d\.]")
startingZeroRegex = re.compile("^0[^\.]")

def _buildRowInsertString(fields):
    """ Construct an adapted format string for quick use in an INSERT INTO sql statement """
    elts = []
    for field in fields:
        if field[1] == "text":
            elts.append('\'%s\'')
            continue
        elts.append('%s')
    return "(%s)" % ','.join(elts)

def _dataType(data):
    """ Return a postgresql data type according to the given csv string """
    # Set field to text if it is quoted, starts with 0, or contains a non-digit,
    # non-point character 
    if data in ("", nullString):
        return "null" # No evidence
    elif data[0] in ('"', '\'') or nonnumericRegex.search(data) is not None or startingZeroRegex.search(data) is not None:
        if data == "NA":
            return "bigint"
        else:
            return "text"
    # Set field to double if it contains a point
    elif '.' in data:
        return "double precision"
    # Else, the field should only contain digits, set it to bigint
    else:
        return "bigint"

def _mostGenericType(type1, type2):
    """ Return the most generic type among (text, double precision, bigint).
    Assume type1 is not "null" """
    if type1 == "text":
        return "text"
    elif type2 == "text":
        return "text"
    elif type2 == "null":
        return type1
    elif type1 == "double precision":
        return "double precision"
    return type2
    
def _checkAndSetNullString(candidate):
    """ If the null string has not been manually set, the script is in charge of finding the right one 
    Return True if the null string has been modified."""
    global nullString
    hasNullString[candidate]=True
    if hasNullString[candidate] == hasNullString["\"\""] == hasNullString[""] == True:
        print "File error : too many potential null Strings"
        import sys; sys.exit()
    if fixedNullString or nullString == candidate or candidate == "\"\"" or nullString == "NA":
        return False
    nullString = candidate #The other four cases
    if debug:
        if nullString == "":
            print  "\n ***** New null string : [empty string] ***** \n"
        else:
            print  "\n ***** New null string : " + nullString + " ***** \n"
    return True

def _parseSomeFields(reader, fieldsCount, dataTypes):
    """ Parse all lines and return a list of datatypes for each column """
    for i in range(nRowsToParse):
        #Read the line to check the data types
        try:
            line = reader.next()
            for i in range(fieldsCount):
                dataTypes[i].append(_dataType(line[i]))
        except Exception as e:
            if debug:
                print "EoF reached before reading " + str(nRowsToParse) + " lines."
                try:
                    e.print_stack_trace()
                except Exception:
                    pass
            break
    return dataTypes

def _parseFields(filePath):
    """Return a list of (field type, field name) tuples. The field types are estimated """
    reader = csv.reader(open(filePath, 'rb'), quoting=csv.QUOTE_NONE, delimiter=csvDelimiter)
    fieldNames = reader.next()
    fieldsCount = len(fieldNames)
    
    #Initialize data structures
    columns = [None]*fieldsCount
    dataTypes = [None]*fieldsCount
    for i in range(fieldsCount):
        dataTypes[i] = []
    
    dataTypes = _parseSomeFields(reader, fieldsCount, dataTypes)
    
    #Parse the datatypes and select the most general one
    for i in range(fieldsCount):
        #Fix empty field names
        if fieldNames[i] in ("", "\"\"", "\'\'"):
            fieldNames[i] = "autoNull"+str(i)
        if "text" in dataTypes[i] or "null" in dataTypes[i]:
            columns[i] = (fieldNames[i], "text")
        elif "double precision" in dataTypes[i]:
            columns[i] = (fieldNames[i], "double precision")
        else:
            columns[i] = (fieldNames[i], "bigint")
    return columns

def _reParseFields(data, fields):
    """ Fix the first incorrect field type according to the given data. Terminate python instance
    if no data type is modified """
    n = len(data)
    fieldsCount = len(fields)
    
    for line in data:
        for j in range(fieldsCount):
            newType = _mostGenericType(fields[j][1], _dataType(line[j]))
            if newType != fields[j][1]:
                if debug: print "Found that " + str(fields[j]) + " should be of type " + newType
                fields[j] = (fields[j][0], newType)
                return (fields, fields[j][0], newType)
#                 fields[j] = (fields[j][0], newType)
#                 return fields
    print "\n***** Error : cannot find a proper data type. Terminating ..."
    import sys; sys.exit()

def _reParseAndAlter(tableName, data, fields):
    """ Parse the fields again with to the given data, and modifies the table accordingly. """
    (fields, column, newType) = _reParseFields(data, fields)
    colsString = []
    command = "ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s;" % (tableName, column, newType)
    if debug: print "\nExecuting postgresql command :\n" + command
    try:
        cursor.execute(command)
        connection.commit()
        if debug: print "... Successful"
    except Exception as e:
        try :
            print e.pgerror
        except Exception:
            try:
                e.print_stack_trace()
            except Exception:
                pass
        import sys; sys.exit()
    return fields

def _parse(filePath):
    """ Parse the file """
    # Verify file extension
    aFile = os.path.basename(filePath)
    source = aFile.split(".")
    fileShortName = source[0]
    if (len(source) != 2 or (source[1] != "csv" and source[1] != "CSV")):
        if debug: print "File " + aFile + " doesn't have the csv extension"
        return None
    
    # Get initial types for the fields
    fields = _parseFields(filePath)
    
    # Create a table with the corresponding rows
    commandString = "CREATE TABLE " + fileShortName + " ("
    commandString += fields[0][0] + " " + fields[0][1]
    for i in range(1, len(fields)):
        field = fields[i]
        commandString += ", " + field[0] + " " + field[1] 
    commandString += ");"
    if debug >= 2: print "\nExecuting postgresql command :\n" + commandString
    try:
        cursor.execute(commandString)
        connection.commit()
        if debug: print "Successfully created table " + fileShortName
    except Exception as e:
        print "\n/!\\ Cannot create table " + fileShortName + ", it probably already exists"
        if autoDrop == True:
            command = "DROP TABLE %s;" % fileShortName
            print "Executing postgresql command : " + command
            try:
                connection.rollback()
                cursor.execute(command)
                connection.commit()
            except Exception as e:
                try:
                    e.print_stack_trace()
                except Exception:
                    pass
                if debug:
                    print "Couldn't drop table. Terminating ..."
                import sys; sys.exit()
            try:
                cursor.execute(commandString)
                connection.commit()
                if debug: print "Successfully created table " + fileShortName
            except Exception as e:
                try:
                    e.print_stack_trace()
                except Exception:
                    pass
                if debug:
                    print "Cannot create table. Terminating ..."
                import sys; sys.exit()
        try:
            e.print_stack_trace()
        except Exception:
            pass
    _doParse(fileShortName, fields, filePath)
    if debug >= 2:
        if nullString == "": print "Finished parsing %s, with [empty string] as null string\n" % aFile
        else: print "Finished parsing %s, with %s as null string\n" % (aFile, nullString)
    
def _doParse(tableName, fields, filePath):
    """ Perform the parsing operations """
    reader = csv.reader(open(filePath, 'rb'), quoting=csv.QUOTE_NONE, delimiter=csvDelimiter)
    reader.next()
    
    rowString = _buildRowInsertString(fields)
    
    # Insert data
    count = 0
    dataBuffer = [None]*10000
    for line in reader:
        dataBuffer[count] = line
        count += 1
        #After 1000 lines, try sending data
        if count > 9999:
            count = 0
            rowString = _sendData(tableName, fields, filePath, dataBuffer, rowString)
            if rowString is None:
                return
    if count > 0:
        _sendData(tableName, fields, filePath, [dataBuffer[i] for i in range(count)], rowString)
    
def _sendData(tableName, fields, filePath, data, rowString):
    """ Send data to the connected database. Manage partially wrong table specification """
    if data[len(data)-1] is None: #Incomplete batch
        newBuffer = []
        for line in data:
            if line is not None:
                newBuffer.append(line)
        data = newBuffer
    success = _send(tableName, data, rowString)
    if success != 1:
        # While the row insertion fails, alter the table so it can store the data.
        # This will never cause an infinite loop because _reParseAndAlter requires that fields
        # are modified during its execution
        while success == -1:
            connection.rollback()
            fields = _reParseAndAlter(tableName, data, fields)
            rowString = _buildRowInsertString(fields)
            success = _send(tableName, data, rowString)
        if success == 0:
            #If the null string has to be changed, restart from the beginning
            _doParse(tableName, fields, filePath)
            return None
    try:
        connection.commit()
    except Exception as e:
        if debug:
            print "/!\ Error while committing changes. Terminating ..."
        try:
            e.print_stack_trace()
        except Exception:
            pass
        import sys; sys.exit()
    return rowString

def _send(tableName, data, rowString):
    """ Send data to the connected database. Return 0 if an error occurred, -1 if the fields have to
    be reparsed, 1 otherwise """
    cmd = []
    for line in data:
        values = []
        for elt in line:
            if elt in ("", "\"\"", "NA"):
                if _checkAndSetNullString(elt):
                    #The null string has been changed, and the parsing must be done accordingly
                    return 0
                if elt == nullString:
                    values.append('null')
                    continue
            values.append(elt.replace("\'", "\'\'").replace("\"", ""))
        try:
            cmd.append(rowString % tuple(values))
        except Exception as e:
            print values
            print rowString.count("%s")
            print len(values)
            try: e.print_stack_trace()
            except Exception: pass
            import sys; sys.exit()
    command = ("INSERT INTO %(table)s VALUES %(values)s;" 
               % {"table":tableName, "values":",\n".join(cmd)})
    if debug >= 3: print "\nExecuting postgresql command :\n" + command + "\n"
    try:
        cursor.execute(command)
    except Exception as e:
        if debug:
            print "\n/!\\ Error while inserting rows"
            try:
                print e.pgerror
            except Exception:
                pass
            finally:
                try:
                    e.print_stack_trace()
                except Exception:
                    pass
        return -1
    return 1

def parseOneFile():
    """ Read one csv file and parse it"""
    _parse(src)

def parseAllFiles():
    """ Read all .csv/.CSV files in src and parse them """
    for aFile in os.listdir(src):
        filePath = os.path.join(src, aFile)
        if os.path.isdir(filePath):
            continue
        hasNullString = {"NA":False, "\"\"":False, "":False}
        global nullString
        nullString = args.null
        if debug >= 2 :
            if nullString == "": print "Back to [empty string] as null string"
            else: print "Back to " + nullString + " as null string"
        _parse(filePath)
    
connection = None
try:
    connection = pg.connect(database=dbname, user=userName, host=hostName)
    cursor = connection.cursor()
except Exception as e:
    print "/!\\ Error : Cannot connect to the database. Terminating ..."
    import sys; sys.exit()

if os.path.isdir(src):
    parseAllFiles()
else:
    parseOneFile()
