# Hadoop Training - תשובות בעברית

<div dir="rtl">

פתרונות לתרגילי Hadoop (ספירת מילים עם MapReduce על Amazon EMR), בעברית. הגרסה באנגלית נמצאת ב-[README.md](README.md).

</div>

## Preparations

**How many partition files were created? (keep this number for later)**

<div dir="rtl">

**תשובה:** נוצר קובץ נתונים אחד (`part-00000`), כי ברירת המחדל של ההרצה הייתה reducer אחד. בנוסף יש בתיקייה את הקובץ הריק `_SUCCESS`, שהוא רק סימון סטטוס של ה-job ולא קובץ נתונים.

</div>

## Exercise 1

**From the output of the run, under "Job Counters", write down the following values: Launched map tasks, Launched reduce tasks, Merged Map outputs**

<div dir="rtl">

**תשובה:** בהרצה הראשונה (ללא ציון מספר reducers):

</div>

```
Launched map tasks     = 5
Launched reduce tasks  = 1
Merged Map outputs     = 5
```

**Now rerun the same job with `-numReduceTasks 6` and count again from the summary**

<div dir="rtl">

**תשובה:** בהרצה השנייה (עם `-numReduceTasks 6`):

</div>

```
Launched map tasks     = 5
Launched reduce tasks  = 6
Merged Map outputs     = 30
```

**Print again the number of output files in the output directory.**

<div dir="rtl">

**תשובה:** בתיקיית הפלט יש עכשיו 6 קבצי נתונים (`part-00000` עד `part-00005`), ובנוסף הקובץ `_SUCCESS`.

</div>

**Q1. How many files are there now?**

<div dir="rtl">

**תשובה:** יש כעת 6 קבצי נתונים (`part-00000` עד `part-00005`), ובנוסף הקובץ הריק `_SUCCESS` שהוא רק סימון סטטוס, ולכן בסך הכול 7 פריטים בתיקייה.

</div>

**Q2. Did number of mapper change?**

<div dir="rtl">

**תשובה:** לא. מספר ה-mappers נשאר 5. מספר ה-mappers נקבע לפי הקלט (מספר ה-input splits, שתלוי בגודל שלושת קבצי הקלט ובגודל בלוק ה-HDFS) ולא לפי מספר ה-reducers.

</div>

**Q3. Did number of reducer changed?**

<div dir="rtl">

**תשובה:** כן. מספר ה-reducers השתנה מ-1 (ברירת המחדל) ל-6, כי הרצנו את ה-job עם הפרמטר `-numReduceTasks 6`.

</div>

**Q4. Did number of output files change? Why?**

<div dir="rtl">

**תשובה:** כן, מ-1 ל-6. כל reducer כותב קובץ פלט משלו (`part-0000X`), ולכן מספר קבצי הפלט שווה למספר ה-reducers.

</div>

**Q5. What does the value of 'Merged Map outputs' represents and how is it calculated?**

<div dir="rtl">

**תשובה:** הערך מייצג את מספר פלטי ה-map שהועברו (shuffle) ל-reducers ומוזגו אצלם. כל mapper מחלק את הפלט שלו למחיצה אחת לכל reducer, וכל reducer מושך את המחיצה שלו מכל mapper. לכן: `Merged Map outputs` = מספר ה-mappers × מספר ה-reducers. בהרצה הראשונה 5 × 1 = 5, ובהרצה השנייה 5 × 6 = 30.

</div>

## Exercise 2

**Before you begin: is the code change in the mapper or the reducer?**

<div dir="rtl">

**תשובה:** השינוי נעשה ב-mapper. צריך לנקות את המילה (להסיר פסיקים ונקודות בסוף המילה) ולהמיר אותה לאותיות קטנות לפני שה-mapper פולט את הזוג (מילה, 1), כדי ש-`The`, `the` ו-`the,` ייספרו כמפתח אחד ויסוכמו יחד ב-reducer. ה-reducer לא השתנה.

הקוד של `mapper.py` אחרי השינוי:

</div>

```python
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

הרצה על האשכול (בלי `-numReduceTasks`):

</div>

```
$ > hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./mapper.py -mapper ./mapper.py -file ./reducer.py -reducer ./reducer.py -input /user/hadoop/gutenberg/* -output /user/hduser/gutenberg-output
```

<div dir="rtl">

השפעת השינוי: מספר המילים השונות (`Reduce input groups`) ירד מ-77,928 ל-54,591, כי צורות באותיות גדולות/קטנות ומילים שמסתיימות בפסיק או בנקודה אוחדו. 8 המילים הנפוצות ביותר:

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

## Exercise 3

**Before you begin: is the code change in the mapper or the reducer?**

<div dir="rtl">

**תשובה:** השינוי נעשה ב-reducer. רק ה-reducer רואה את הספירה הסופית של כל מילה, ולכן רק הוא יכול לקבוע אילו שלוש הספירות הגבוהות ביותר. ה-mapper רואה רק חלק מהקלט. ה-mapper של תרגיל 2 נשאר ללא שינוי. מריצים עם `-numReduceTasks 1` כדי ש-reducer יחיד יראה את כל המילים (עם כמה reducers כל אחד היה מדפיס 3 מילים משלו, למשל 9 שורות עבור 3 reducers). כל המיון והסינון נעשים בתוך קוד ה-Python, בלי pipe, sort או מסננים חיצוניים.

הקוד של `reducer_top3.py`:

</div>

```python
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

**Print the output of the MapReduce and put in the answers page**

<div dir="rtl">

**תשובה:** הפלט של ה-MapReduce (3 המילים הנפוצות ביותר):

</div>

```
$ > hadoop fs -cat /user/hduser/gutenberg-output/*
the     74369
and     42768
of      34588
```
