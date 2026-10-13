# 0.1.3

## CID denylist

- Ability to point to a CID or name as a CID deny list

## Migrate to JSON stores to sqlite3

## Improved bootstraping without bootstrap node

Add uptime & number of known nodes to startup announce

If an announce is heard from a new node, advertise self to the new node after a random delay if the following conditions are met:

* New node's number of known nodes is zero
* This node is the closest node in the DHT to the new node that it knows of
* This node's uptime is above one hour or

This change should not break compatibility with previous versions. Announces without this information should still be recognised as normal

# 0.1.4

## Add files without duplicating them with --nocopy flag

## RNFS content as a folder (linux only)