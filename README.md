# MongoBalancer

## Edited Source (Old Version)
https://gist.github.com/wioniqle-q/4d71ba6847ead70417db1568650d0c9c

MongoBalancer is an advanced load balancing solution for MongoDB read operations in .NET applications. It dynamically adjusts read preferences between primary and secondary nodes based on real-time performance metrics, ensuring optimal read distribution and improved overall system performance.

## Features

- Dynamic load balancing between primary and secondary MongoDB nodes
- Real-time adjustment based on latency and staleness metrics
- Configurable staleness threshold

## How It Works
MongoBalancer continuously monitors the performance of primary and secondary nodes. It adjusts the balance fraction based on:

Round-trip times to primary and secondary nodes
Staleness of secondary nodes
The balance fraction determines the likelihood of reads being directed to secondary nodes. This fraction is dynamically adjusted to optimize read performance while maintaining data consistency within the specified staleness threshold.

## Contributing
Contributions to MongoBalancer are welcome! Please feel free to submit a Pull Request.
