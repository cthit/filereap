# filereap

Delete dated files in a directory according to a time period config.
Useful for backup directories that accumulate files over time.

## Example config
```toml
# Specify a folder containing backups
path = "/backups/docker"

# uncomment if the backups are btrfs subvolumes
#btrfs = true

[[periods]]
# For the first day, keep one backup per second (basically, don't delete backups)
# syntax supports suffixes s, m, h, d, w
period_length = "1d"
chunk_size = "1s"

[[periods]]
# For the next week, keep one backup per hour
period_length = "1w"
chunk_size = "1h"

[[periods]]
# For the next 4 weeks, keep one backup per day
period_length = "4w"
chunk_size = "1d"

# can add more [[periods]] as needed
```

## Example usage
```sh
filereap --help
```
