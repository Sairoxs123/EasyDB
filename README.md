# EasyDB
Made to easily use sqlite in applications

# Documentation

## Table Creation
```
users = Model("Users")
users.addField(columnName : str, type : str, primary_key=True/False, null=True/False, max_length=int[256 default])
users.create()
```
 This creates a table called Users with the fields added with users.addField.
 columnName takes values:
 #
 str for strings
 date for date
 int for integers
