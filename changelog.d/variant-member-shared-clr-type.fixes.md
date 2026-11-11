* Fixed `InsertBinaryAsync` writing a value to the wrong `Variant` member, or failing, when several members read
  as the same .NET type: for example `Array(IPv4)` and `Array(IPv6)`, two tuples of 8 or more elements, or maps
  and `Nested` columns of these.
