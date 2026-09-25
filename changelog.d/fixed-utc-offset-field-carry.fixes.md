* Fixed a wall-clock shift when reading a `DateTime`/`DateTime64` column whose `Fixed/UTC±HH:MM:SS`
  timezone name has a minute or second field above 59. The server carries such a field into the next
  one, so `Fixed/UTC+05:60:00` means +06:00; the driver rejected the name and read the value as UTC.
