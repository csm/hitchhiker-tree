Can we change this to have the root of the tree always be an index node?

It's somewhat easier to store roots in dynamodb if they're only ever going
to be index nodes. We can have the data node size be large (megabytes or more)
when they only go into S3, and have 