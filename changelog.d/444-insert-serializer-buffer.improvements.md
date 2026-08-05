* Removed a 4KiB/batch allocation in the binary-insert serializers (for writing the SQL query). Pooled buffers are now used instead.
