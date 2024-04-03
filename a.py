import re

a="asd:b"

b=a.split(":")
c=re.split(r":",a)

print(b,c)
