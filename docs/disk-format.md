# Relatable Disk Format:

So the disk is separated into a set of n-byte pages. The first page, or the meta page, has the following layout:

```
offset: description
-------------------

0: version
1: exponent for the database size (e.g. 16 here indicates 2^16 bytes per page)
```

Bytes 2-the end of the page are reserved for further use. Yes this is a lot, but w/e I don't care.

## What do pages look like?

Ok so relatable is really just a big-ass b-tree. To facilitate this, the file is divided up into a set of pages, and it looks something like the following:
