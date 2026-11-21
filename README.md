# Hadoop Training Questions

Hadoop streaming word count on Amazon EMR (emr-7.14.0, 1 primary + 1 core node), run on three Project Gutenberg books (`5000-0.txt`, `2600-0.txt`, `1399-0.txt`).

Hebrew version of the exercise with the answers (full Word file text): [Hadoop-training-Hebrew-answers.md](Hadoop-training-Hebrew-answers.md)

## Exercise 1

Counters of the job, first with the default number of reducers and then with `-numReduceTasks 6`:

| Counter                | Run 1 (default) | Run 2 (`-numReduceTasks 6`) |
|------------------------|-----------------|-----------------------------|
| Launched map tasks     | 5               | 5                           |
| Launched reduce tasks  | 1               | 6                           |
| Merged Map outputs     | 5               | 30                          |
| Output (part) files    | 1               | 6                           |

**Q1. How many files are there?**
In the first run there is one data file (`part-00000`). After rerunning with `-numReduceTasks 6` there are 6 data files (`part-00000` ... `part-00005`). In both cases the output directory also contains the empty `_SUCCESS` file, which is only a job-status marker and not a data file.

**Q2. Did the number of mappers change?**
No, it stayed at 5. The number of map tasks is determined by the input (the number of input splits, which depends on the size of the input files and the HDFS block size), not by the number of reducers.

**Q3. Did the number of reducers change?**
Yes. It changed from 1 (the default) to 6, because the job was started with `-numReduceTasks 6`.

**Q4. Did the number of output files change? Why?**
Yes, from 1 to 6. Every reducer writes its own output file (`part-0000X`), so the number of output files equals the number of reducers.

**Q5. What does the value of 'Merged Map outputs' represent, and how is it calculated?**
It is the number of map outputs that were shuffled to the reducers and merged there. Each mapper splits its output into one partition per reducer, and every reducer fetches its own partition from every mapper. Therefore:

`Merged Map outputs = (number of mappers) x (number of reducers)`

Run 1: 5 x 1 = 5. Run 2: 5 x 6 = 30.

## Exercise 2

**Q1. Is your change in the mapper or in the reducer?**
In the **mapper**. The words must be cleaned (trailing commas and dots removed) and converted to lower case *before* the mapper emits the `(word, 1)` pair, so that "The", "the" and "the," become the same key and are summed together by the reducer. The reducer is unchanged.

**Q2. Code**

`mapper.py`:

```python
#!/usr/bin/env python3
"""mapper.py - word count mapper (Exercise 2)

Strips trailing commas and dots and lower-cases every word, so
"The", "the" and "the," are counted as the same word.
"""

import sys

def read_input(file):
    for line in file:
        # split the line into words
        yield line.split()

def main(separator='\t'):
    data = read_input(sys.stdin)
    for words in data:
        for word in words:
            # strip trailing commas and dots, then lowercase
            word = word.rstrip(',.').lower()
            if word:
                print('%s%s%d' % (word, separator, 1))

if __name__ == "__main__":
    main()
```

`reducer.py` (unchanged):

```python
#!/usr/bin/env python3
"""reducer.py - word count reducer (unchanged in Exercise 2)"""

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print("%s%s%d" % (current_word, separator, total_count))
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()
```

Effect of the change: the number of distinct words (`Reduce input groups`) dropped from 77,928 to 54,591, because upper/lower case forms and words ending with a comma or a dot were merged.

## Exercise 3

**Q1. Is your change in the mapper or in the reducer?**
In the **reducer**. Only the reducer sees the final total count of every word, so only it can decide which three counts are the highest. The mapper only sees its own split of the input, so it stays the same as in Exercise 2. The job is run with `-numReduceTasks 1` so that a single reducer sees all the words (with several reducers each one would print its own top 3).

**Q2. Code**

The mapper is the same `mapper.py` as in Exercise 2.

`reducer_top3.py`:

```python
#!/usr/bin/env python3
"""reducer_top3.py - prints the 3 words with the highest counts

Run with -numReduceTasks 1 so that a single reducer sees all the words.
"""

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    counts = []
    for word, group in groupby(data, itemgetter(0)):
        try:
            total = sum(int(count) for _, count in group)
            counts.append((total, word))
        except ValueError:
            pass
    # sort by count in descending order and keep only the first 3
    counts.sort(reverse=True)
    for total, word in counts[:3]:
        print("%s%s%d" % (word, separator, total))

if __name__ == "__main__":
    main()
```

Command used:

```
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar \
  -file ./mapper.py -mapper ./mapper.py \
  -file ./reducer_top3.py -reducer ./reducer_top3.py \
  -input /user/hadoop/gutenberg/* \
  -output /user/hduser/gutenberg-output \
  -numReduceTasks 1
```

All the sorting and filtering is done inside the Python reducer, with no external pipes, `sort` or filters.

**Q3. Output of the MapReduce**

Screenshot (taken after typing `hadoop fs -ls /user/hduser/gutenberg-output`):

![Exercise 3 output](Screenshot%202026-11-21%20183422.png)

Text version of the output:

```
$ > hadoop fs -cat /user/hduser/gutenberg-output/*
the     74369
and     42768
of      34588
```
