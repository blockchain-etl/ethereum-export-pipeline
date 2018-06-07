from ethereumetl.utils import split_to_batches

# The below partitioning tries to make each partition of equal size.
# The first million blocks are in a single partition.
# The next 3 million blocks are in 100k partitions.
# The next 1 million blocks are in 10k partitions.
# Note that there is a limit in Data Pipeline on the number of objects, which can be
# increased in the Support Center
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-limits.html
EXPORT_PARTITIONS = [(0, 999999)] + \
                    [(start, end) for start, end in split_to_batches(1000000, 1999999, 100000)] + \
                    [(start, end) for start, end in split_to_batches(2000000, 2999999, 100000)] + \
                    [(start, end) for start, end in split_to_batches(3000000, 3999999, 100000)] + \
                    [(start, end) for start, end in split_to_batches(4000000, 4999999, 10000)]
