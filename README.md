# The Relatable Database

This project is my current attempt at writing a relational database.

Goals:

- Database should be a single file. I should be able to send a single file to someone (in an email or whatever) and they should be able to read it.
- Full ACID compliance.

# Implementation

The implementation is very bare-bones at the moment. The file that the database writes and reads to is organized into a set of blocks. Each block is equal in size. Blocks have some meta, including: the offset of the next block in the list, and the number of bytes written to the block.

## The block list, aka the blockchain

Blocks are organized into a linked list style structure, with blocks having a link to their next block. The reason I made this abstraction was so that I didn't have to allocate n bytes at the beginning of the disk for all schema information, for example. Imagine what would happen if the schema became too large! We would have to shift around every file in the system. Instead we only need to allocate a new block at the end, and there are abstractions in place that allow us to treat a given linked list of blocks as one long piece of memory.

## How to find information

- Root block starts at offset 0. Always. It contains the offset of the first schema block
- Schema block is a list of tables. Each table has the offset of the start of it's data blocks
- Data blocks / Schema blocks will always contain the location of their next block

## immutability of blocks

**This is not implemented yet**

So this is something that's still swimming around in my head, but I think it'll make ACID compliance a lot easier.

Imagine that _every_ time we modified a block, we instead did the following:

1. Copy the contents of the block to a new block
2. Mutate the data in the new block
3. Set the next block pointer of the previous block of the one we're mutating to point to the new block.

There are some issues here, of course: If we do this, we will need to change every single block up the tree until we come to the root block, which must be changed in-place since it's at offset 0. So what to do? I think that this challenge can be overcome with a good abstraction that only does the minimal amount of change possible, stopping when we get to a point where the write can be Atomic.

At that point, even though we've allocated the new blocks and copied, if the atomic write to set the new next_block fails, we won't have any data inconsistencies. It'll still point to the next block properly. If we have an error when constructing the replacement block, no worries because we weren't mutating data in-place.

What happens to the old blocks though? We will need to have a freelist block that contains all of the blocks that are free within the system. With this system we won't always have to allocate more space when requesting new blocks: we can overwrite old blocks.

Of course, this runs into the whole ACID consideration when implementing the freelist. What if writing to the freelist fails? Do we want the freelist blocks to be immutable?

# I/O concepts

## Creates

This one will be the easiest. Copy the block like we discuss in `immutability of blocks` above, add the new record, update the previous blocks pointer.

## Deletes

I'm doing this one before updates, for reasons that will soon be clear.

So somehow I want to have a tombstone file. What that means is that when we delete a record, it isn't _actually_ deleted from the disk. Instead, we only record that it _was_ deleted. Every time we do a query from that point onwards, we ignore the ones in the tombstone. Finally, if the tombstone becomes too big, we can "vacuum" it, where we recreate the table without the deleted rows.

## Updates

If we're updating a row, I'd rather delete the old records, and insert the modified ones anew. This seems conceptually simple. There are probably some gains to be had.

# License

This project is under the MIT license.
