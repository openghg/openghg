# import re
# import sys

# base_module = sys.argv[1]

# for line in sys.stdin.readlines():
#     m = re.search(r"from ([\w\d\.]+) import ([\w\d\.]+)", line)

#     path = m.groups()[0]
#     cls = m.groups()[1]

#     print("%s = _lazy_import.lazy_class(\"%s%s.%s\")" %
#           (cls, base_module, path, cls))
