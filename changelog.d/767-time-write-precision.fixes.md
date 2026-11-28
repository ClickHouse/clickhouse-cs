* Fixed HTTP binary inserts and HTTP and TCP parameters for `Time` and `Time64`. The clients now truncate `TimeSpan` and `TimeOnly` toward zero to the target precision (#767).
