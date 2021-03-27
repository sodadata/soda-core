**Problem:** There are known issues on Soda SQL when using pip version 19. <br />
**Solution:** Upgrade `pip` to version 20 or greater using the following command:
```shell
$ pip install --upgrade pip
```
<br />

**Problem:** Upgrading Soda SQL does not seem to work. <br />
**Solution:** Run the following command to skip your local cache when upgrading your Soda SQL version:
```shell
$ pip install --upgrade --no-cache-dir soda-sql
```