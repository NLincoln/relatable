# Removing blocks?

Ok so I've been thinking about that a bit. Blocks still work. They're still what I'm doing. But I can't shake this feeling that I'm doing something inefficient.

Namely, the current way that I allocate data is going to be like the following:

file -> block::from_disk -> blockdisk.read -> row

The problem is that data is "owned" at two stages in this: by the block, and by the blockdisk. This worries me.

What's nice is that the concept of the blocks is really an impl detail of the blockdisk, and I _bet_ I could get it removed.
