# BigData-PA1-WikiNameCount
Compares file of names with wiki xml dump file to match instances of names

Implements PeopleMapper and WikiMapper, which emit key, value pairs to 
WikiReducer, which counts instances of that name, and writes it to output
assuming is appears at least once in the wiki file and is also a name in the people file
