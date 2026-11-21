# Hadoop training

## Preparations

1. Login to emr master node:

2. You should see in the console:

```
EEEEEEEEEEEEEEEEEEEE MMMMMMMM           MMMMMMMM RRRRRRRRRRRRRRR
E::::::::::::::::::E M:::::::M         M:::::::M R::::::::::::::R
EE:::::EEEEEEEEE:::E M::::::::M       M::::::::M R:::::RRRRRR:::::R
  E::::E       EEEEE M:::::::::M     M:::::::::M RR::::R      R::::R
  E::::E             M::::::M:::M   M:::M::::::M   R:::R      R::::R
  E:::::EEEEEEEEEE   M:::::M M:::M M:::M M:::::M   R:::RRRRRR:::::R
  E::::::::::::::E   M:::::M  M:::M:::M  M:::::M   R:::::::::::RR
  E:::::EEEEEEEEEE   M:::::M   M:::::M   M:::::M   R:::RRRRRR::::R
  E::::E             M:::::M    M:::M    M:::::M   R:::R      R::::R
  E::::E       EEEEE M:::::M     MMM     M:::::M   R:::R      R::::R
EE:::::EEEEEEEE::::E M:::::M             M:::::M   R:::R      R::::R
E::::::::::::::::::E M:::::M             M:::::M RR::::R      R::::R
EEEEEEEEEEEEEEEEEEEE MMMMMMM             MMMMMMM RRRRRRR      RRRRRR
```

```
[hadoop@ip-172-31-80-249 ~]$
```

Create a directory for your work:

```
 $ > mkdir course
 $ > cd course
```

Create a mapper file named "mapper.py"

Open it using "vim" editor (or "nano" if you prefer)

Note: ‘vim’ is a simple editor that is available in all linux systems.

Interface is all command-line.

The following commands are important for you.

- The key “i” enters into “insert” mode. In this mode everything you type is inserted to the file.

- the key "x" deletes one character at a time. Note that in "insert" mode it will insert "x" to the code.

- The key “Esc” exists from “insert” mode. You must exit from insert mode when you want to save the file.

- The key sequence> 'Esc' + ':' + 'q' + '!' exits from the file without saving. Note that in insert mode it will insert those characters into the file!

- The key sequence> 'Esc' + ':' + 'w' + 'q' Exits from the file after saving. Note that in insert mode it will insert those characters into the file!

Open a new file "mapper.py":

```
 $ > vim mapper.py
```

Press “i” to move to “insert” mode.

Copy the following code sample (Ctrl+c) and paste to the editor (Ctrl+v)

#### Code for mapper.py:

```
#!/usr/bin/env python
"""mapper.py"""

import sys

def read_input(file):
    for line in file:
        # split the line into words
        yield line.split()

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        for word in words:
            print ('%s%s%d' % (word, separator, 1))

if __name__ == "__main__":
    main()
```

Save the file by typing the following sequence of keys: 'Esc' + ':' + 'w' + 'q' and Enter.

Open a new file reducer.py:

```
 $ > vim reducer.py
```

Press “i” to move to “insert” mode.

Copy the following code sample (Ctrl+c) and paste to the editor (Ctrl+v)

```
#!/usr/bin/env python
"""reducer.py"""

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all [current_word, count] items
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print ("%s%s%d" % (current_word, separator, total_count))
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()
```

Save the file by typing the following sequence of keys: 'Esc' + ':' + 'w' + 'q' then Enter.

Make the two files executable in linux by changing the files attributes to +x:

```
 $ > chmod +x mapper.py reducer.py
```

To run the mapper in local mode, copy this line end enter:

```
 $ > echo "foo foo quux labs foo bar quux" | ./mapper.py

foo     1
foo     1
quux    1
labs    1
foo     1
bar     1
quux    1
```

To run both mapper and reducer in local mode, copy this line and enter:

```
 $ > echo "foo foo quux labs foo bar quux" | ./mapper.py | sort -k1,1 | ./reducer.py

bar     1
foo     3
labs    1
quux    2
```

### Running Hadoop on the cluster:

Copy two big files from the internet to run the program on:

```
wget http://www.gutenberg.org/files/5000/5000-0.txt
wget http://www.gutenberg.org/files/2600/2600-0.txt
wget http://www.gutenberg.org/files/1399/1399-0.txt
```

Now you need to copy the files to hdfs. Only there you can run you mapreduce:

First we create a new directory named "gutenberg":

```
 $ > hadoop fs -mkdir /user/hadoop/gutenberg
```

Now we copy the two files to that directory:

```
 $ > hadoop fs -copyFromLocal 5000-0.txt 1399-0.txt 2600-0.txt /user/hadoop/gutenberg
```

Check to see that the files are there:

```
 $ > hadoop fs -ls /user/hadoop/gutenberg
```

Found 3 items

```
-rw-r--r--   1 hadoop hadoop    2068079 2020-11-07 19:59 /user/hadoop/gutenberg/1399-0.txt
-rw-r--r--   1 hadoop hadoop    3359584 2020-11-07 19:59 /user/hadoop/gutenberg/2600-0.txt
-rw-r--r--   1 hadoop hadoop    1428841 2020-11-07 19:59 /user/hadoop/gutenberg/5000-0.txt
```

Now we run the job with python code:

```
 $ > hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./mapper.py -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input /user/hadoop/gutenberg/* -output /user/hduser/gutenberg-output
```

You will see some code running, indicating that the mapper and reducer run.

It shall end with the line:

```
   INFO streaming.StreamJob: Output directory: /user/hduser/gutenberg-output
```

Indicating that the output was written to that directory.

Let us check now that the output was generated:

```
 $ > hadoop fs -ls /user/hduser/gutenberg-output
```

How many partition files were created? (keep this number for later)

<div dir="rtl">

**תשובה:** נוצר קובץ נתונים אחד (part-00000), כי ברירת המחדל של ההרצה הייתה reducer אחד. בנוסף יש בתיקייה את הקובץ הריק _SUCCESS, שהוא רק סימון סטטוס של ה-job ולא קובץ נתונים.

</div>

Note that there is one file called _SUCCESS that is not a data file, but rather a status of the job.

To view the results in the output, type:

```
 $ > hadoop fs -cat /user/hduser/gutenberg-output/* | sort -nk2
```

(Pipe to ‘sort -nk2’ will take the output and sort it by the second columns and treat it as numeric)

```
he      14356
that    14583
in      19012
a       19803
to      31391
of      33845
and     39787
the     68790
```

### Viewing history (optional)

In order to see jobs history, go back to the AWS console > EMR

- Choose your cluster

- Click on the YARN timeline server link under Cluster Management

You will see the history of your MapReduce runs. Note that it may take some time for a run to show in the UI.

- Click on your job’s ID link

- Click on the Attempt Id link

You will see all tasks that were run.

- First is the job

- Then mappers

- Then reducers.

Click on logs and search for “gutenberg” to find the splits and whether this is a mapper or reducer

## Exercises

(Please answer in the answers page)

### Exercise 1:

From the output of the run, under "Job Counters",

Write down the following values:

```
- Launched map tasks
- Launched reduce tasks
- Merged Map outputs
```

<div dir="rtl">

**תשובה:** בהרצה הראשונה (ללא ציון מספר reducers):

</div>

```
Launched map tasks     = 5
Launched reduce tasks  = 1
Merged Map outputs     = 5
```

Now, rerun the same MapReduce job, but this time specify how many reducers to use.

1. you need first to delete the output files from previous run:

```
 $ > hadoop fs -rm -r -f /user/hduser/gutenberg-output/
```

2. Run the same command you just ran, but add another parameter to the end of the command:

```
-numReduceTasks 6
```

Now count again the from the summary:

```
- Launched map tasks
- Launched reduce tasks
- Merged Map outputs
```

<div dir="rtl">

**תשובה:** בהרצה השנייה (עם -numReduceTasks 6):

</div>

```
Launched map tasks     = 5
Launched reduce tasks  = 6
Merged Map outputs     = 30
```

Print again the number of output files in the output directory.

<div dir="rtl">

**תשובה:** בתיקיית הפלט יש עכשיו 6 קבצי נתונים (part-00000 עד part-00005), ובנוסף הקובץ _SUCCESS.

</div>

Please answer

Q1. How many files are there now?

<div dir="rtl">

**תשובה:** יש כעת 6 קבצי נתונים (part-00000 עד part-00005), ובנוסף הקובץ הריק _SUCCESS שהוא רק סימון סטטוס, ולכן בסך הכול 7 פריטים בתיקייה.

</div>

Q2. Did number of mapper change?

<div dir="rtl">

**תשובה:** לא. מספר ה-mappers נשאר 5. מספר ה-mappers נקבע לפי הקלט (מספר ה-input splits, שתלוי בגודל שלושת קבצי הקלט ובגודל בלוק ה-HDFS) ולא לפי מספר ה-reducers.

</div>

Q3. Did number of reducer changed?

<div dir="rtl">

**תשובה:** כן. מספר ה-reducers השתנה מ-1 (ברירת המחדל) ל-6, כי הרצנו את ה-job עם הפרמטר -numReduceTasks 6.

</div>

Q4. Did number of output files change? Why?

<div dir="rtl">

**תשובה:** כן, מ-1 ל-6. כל reducer כותב קובץ פלט משלו (part-0000X), ולכן מספר קבצי הפלט שווה למספר ה-reducers.

</div>

Q5. What does the value of 'Merged Map outputs' represents and how is it calculated?

<div dir="rtl">

**תשובה:** הערך מייצג את מספר פלטי ה-map שהועברו (shuffle) ל-reducers ומוזגו אצלם. כל mapper מחלק את הפלט שלו למחיצה אחת לכל reducer, וכל reducer מושך את המחיצה שלו מכל mapper. לכן: Merged Map outputs = מספר ה-mappers × מספר ה-reducers. בהרצה הראשונה 5 × 1 = 5, ובהרצה השנייה 5 × 6 = 30.

</div>

### Exercise 2:

use the existing program and change it a bit to:

1. strip trailing commas and dots at the end of the words

2. treat upper and lower case words the same and count them case-insensitive

you need first to delete the output files from previous run:

```
 $ > hadoop fs -rm -r -f /user/hduser/gutenberg-output/
```

Before you begin: is the code change in the mapper or the reducer?

<div dir="rtl">

**תשובה:** השינוי נעשה ב-mapper. צריך לנקות את המילה (להסיר פסיקים ונקודות בסוף המילה) ולהמיר אותה לאותיות קטנות לפני שה-mapper פולט את הזוג (מילה, 1), כדי ש-The, the ו-"the," ייספרו כמפתח אחד ויסוכמו יחד ב-reducer. ה-reducer לא השתנה.

</div>

<div dir="rtl">

הקוד של mapper.py אחרי השינוי:

</div>

```
#!/usr/bin/env python3
"""mapper.py"""

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

<div dir="rtl">

בדיקה מקומית:

</div>

```
$ > echo "Foo, foo. FOO bar." | ./mapper.py | sort -k1,1 | ./reducer.py
bar     1
foo     3
```

<div dir="rtl">

הרצה על האשכול (בלי -numReduceTasks):

</div>

```
$ > hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./mapper.py -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input /user/hadoop/gutenberg/* -output /user/hduser/gutenberg-output
```

<div dir="rtl">

השפעת השינוי: מספר המילים השונות (Reduce input groups) ירד מ-77,928 ל-54,591, כי צורות באותיות גדולות/קטנות ומילים שמסתיימות בפסיק או בנקודה אוחדו. 8 המילים הנפוצות ביותר:

</div>

```
$ > hadoop fs -cat /user/hduser/gutenberg-output/* | sort -nk2 | tail -n 8
that    15360
he      17589
in      20339
a       20738
to      31891
of      34588
and     42768
the     74369
```

### Exercise 3:

Get only the first 3 highest scores of the word count.

Before you begin: is the code change in the mapper or the reducer?

<div dir="rtl">

**תשובה:** השינוי נעשה ב-reducer. רק ה-reducer רואה את הספירה הסופית של כל מילה, ולכן רק הוא יכול לקבוע אילו שלוש הספירות הגבוהות ביותר. ה-mapper רואה רק חלק מהקלט. ה-mapper של תרגיל 2 נשאר ללא שינוי. מריצים עם -numReduceTasks 1 כדי ש-reducer יחיד יראה את כל המילים (עם כמה reducers כל אחד היה מדפיס 3 מילים משלו, למשל 9 שורות עבור 3 reducers). כל המיון והסינון נעשים בתוך קוד ה-Python, בלי pipe, sort או מסננים חיצוניים.

</div>

<div dir="rtl">

הקוד של reducer_top3.py:

</div>

```
#!/usr/bin/env python3
"""reducer_top3.py - prints the 3 words with the highest counts"""

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

<div dir="rtl">

בדיקה מקומית:

</div>

```
$ > echo "foo foo quux labs foo bar quux" | ./mapper.py | sort -k1,1 | ./reducer_top3.py
foo     3
quux    2
labs    1
```

<div dir="rtl">

הרצה על האשכול:

</div>

```
$ > hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./mapper.py -mapper ./mapper.py -file ./reducer_top3.py -reducer ./reducer_top3.py -input /user/hadoop/gutenberg/* -output /user/hduser/gutenberg-output -numReduceTasks 1
```

Hint: you may need to sort the results by count in descending order, and then take only the first 3 items.

Warning: because the mapreduce work in distributed mode, and there are several reducers (three by default) then each partition will generate the first three counts.

In that case, when your code looks for the first three, you will get the first 3X3, meaning 9!

```
To avoid this, add the parameter -numReduceTasks 1.
```

Print the output of the MapReduce and put in the answers page

```
$ > hadoop fs -cat /user/hduser/gutenberg-output/*
```

<div dir="rtl">

**תשובה:** הפלט של ה-MapReduce (3 המילים הנפוצות ביותר):

</div>

```
the     74369
and     42768
of      34588
```

Note: you should not use external pipes, sort, or filters. All should be done inside the mapReduce Python code

Important: Don’t forget to terminate the cluster after ending the exercise!!!
