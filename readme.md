# RuSQL

A complete database written entirely from scratch in Rust!

## Highlights
- Persistent B+Tree based key-value storage
- Copy-on-write architecture
- Memory-mapped file I/O 
- Free-list garbage collection
- Crash resilience through rollbacks
- Supports tables and secondary indices
- Concurrent transactions through MVCC
- Hand rolled LRU cache for shared reads
- Hand rolled lexer and parser for accepting SQL queries
- Custom serialization and deserializtion
