import sys
import os.path


copylog = open("1", "r").readlines()
result= open("firm_names_list.txt","w", encoding="utf16")

for i in copylog:
	i = i.rstrip()
	i = f"    '{i}',\n"	
	result.write(i)