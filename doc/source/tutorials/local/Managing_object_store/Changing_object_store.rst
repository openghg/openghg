.. TODO - this needs converting from md to rst
# Changing the object store path

To change the object store path you can do it in one of two ways

## 1. Modifying the configuration file directly

Your local configuration file can be found in the `.config` directory in your home folder. On Linux and MacOS the file can be accessed at `~/.config/openghg/openghg.conf`. We don't currently support Windows.

If you created the file using the `openghg --quickstart` command the file should look like this

```toml
user_id = "6dea284d-888a-4cd6-974e-f8888c4b7be3"

[object_store]
local_store = "/home/gareth/openghg_store"
```

To update the path to the object store all I need to do is change the path after `local_store = `. Say I want to move the store to another folder in my home directory, I can just update the path like so

```toml
user_id = "6dea284d-888a-4cd6-974e-f8888c4b7be3"

[object_store]
local_store = "/home/gareth/atmos_chem/object_store"
```

Now we can use the `check_config` command line tool to make sure the configuration file is valid. This checks to make sure the layout of the config file is correct and that the directory exists. To do this we'll use an `ipython` session.

```python
from openghg.util import check_config

check_config()
Your configuration file is valid.
```

If the folder doesn't exist you'll be presented with an input field asking if you want the folder to be created.

```python!
check_config()
our configuration file is correct.
The folder /home/gareth/atmos_chem/object_store does not exist
Should we create it? (y/n): y
The folder /home/gareth/atmos_chem/object_store has ben created.
```

OpenGHG will now use the new path when searching for and storing data.


## 2. Command line

OpenGHG provides a command line interface accessible from the terminal. To update the path of the object store you can run `openghg --quickstart`. This will read your current configuration file and ask if you wan to update the object store path.

```shell!
OpenGHG configuration
---------------------

User config exists at /home/gareth/.config/openghg/openghg.conf, checking...
Current object store path: /home/gareth/openghg_store
Would you like to update the path? (y/n): y
Enter new path for object store: /home/gareth/atmos_chem/openghg_store
Updated configuration saved.
```

OpenGHG will now use the new path when searching for and storing data.
