# Scheme (R6RS) Dataframe Library

A dataframe record type with procedures to select, drop, and rename columns, and filter, sort, split, bind, append, join, reshape, and aggregate dataframes. 

Related blog posts:  
[A dataframe record type for Scheme](https://www.travishinkelman.com/dataframe-record-type-for-scheme/)  
[Select, drop, and rename dataframe columns in Scheme](https://www.travishinkelman.com/select-drop-rename-dataframe-columns-scheme/)  
[Split, bind, and append dataframes in Scheme](https://www.travishinkelman.com/split-bind-append-dataframes-scheme/)  
[Filter, partition, and sort dataframes in Scheme](https://www.travishinkelman.com/filter-partition-and-sort-dataframes-in-scheme/)  
[Modify and aggregate dataframes in Scheme](https://www.travishinkelman.com/modify-aggregate-dataframes-scheme/)  

## Installation

### Akku

```
$ akku install dataframe
```

For more information on getting started with [Akku](https://akkuscm.org/), see this [blog post](https://www.travishinkelman.com/getting-started-with-akku-package-manager-for-scheme/).

## Import

`(import (dataframe))`

## Table of Contents  

### Dataframe record type  

[`(make-dataframe alist)`](#make-df)  
[`(dataframe-alist)`](#dataframe-alist)  
[`(dataframe-names df)`](#dataframe-names)  
[`(dataframe-dim df)`](#dataframe-dim)  
[`(dataframe-display df [n min-width total-width])`](#df-display)  
[`(dataframe-contains? df name ...)`](#df-contains)  
[`(dataframe-crossing obj1 obj2 ...)`](#df-crossing)  
[`(dataframe-head df n)`](#df-head)  
[`(dataframe-tail df n)`](#df-tail)  
[`(dataframe-equal? df1 df2 ...)`](#df-equal)  
[`(dataframe-write df path overwrite?)`](#df-write)  
[`(dataframe-read path)`](#df-read)  
[`(dataframe->rowtable df)`](#df-rows)  
[`(rowtable->dataframe rt [header try->number try-max])`](#rows-df)  
[`(dataframe-ref df indices [name ...])`](#df-ref)  
[`(dataframe-values df name)`](#df-values)  
[`(dataframe-values-unique df name)`](#df-values-unique)  

### Select, drop, and rename columns  

[`(dataframe-select df name ...)`](#df-select)  
[`(dataframe-drop df name ...)`](#df-drop)  
[`(dataframe-rename df name-pairs ...)`](#df-rename)  
[`(dataframe-rename-all df names)`](#df-rename-all)  

### Filter and sort  

[`(dataframe-unique df)`](#df-unique)  
[`(dataframe-filter df names procedure)`](#df-filter)  
[`(dataframe-filter* df names expr)`](#df-filter*)  
[`(dataframe-filter-at df predicate name ...)`](#df-filter-at)  
[`(dataframe-filter-all df predicate)`](#df-filter-all)  
[`(dataframe-partition df names procedure)`](#df-partition)  
[`(dataframe-partition* df names expr)`](#df-partition*)  
[`(dataframe-sort df predicates names)`](#df-sort)  
[`(dataframe-sort* df (predicate name) ...)`](#df-sort*)  

### Split, bind, and append  

[`(dataframe-split df group-name ...)`](#df-split)  
[`(dataframe-bind df1 df2 ...)`](#df-bind)  
[`(dataframe-bind-all missing-value df1 df2 ...)`](#df-bind-all)  
[`(dataframe-append df1 df2 ...)`](#df-append)

### Join

[`(dataframe-left-join df1 df2 join-names missing-value)`](#df-left-join)

### Reshape

[`(dataframe-stack df names names-to values-to)`](#df-stack)  
[`(dataframe-spread df names-from values-from missing-value)`](#df-spread)  

### Modify and aggregate  

[`(dataframe-modify df new-names names procedure ...)`](#df-modify)  
[`(dataframe-modify* df (new-names names expr) ...)`](#df-modify*)  
[`(dataframe-modify-at df procedure name ...)`](#df-modify-at)  
[`(dataframe-modify-all df procedure)`](#df-modify-all)  
[`(dataframe-aggregate df group-names new-names names procedure ...)`](#df-aggregate)  
[`(dataframe-aggregate* df group-names (new-name names expr) ...)`](#df-aggregate*)  

### Thread first and thread last  

[`(-> expr ...)`](#thread-first)  
[`(->> expr ...)`](#thread-last)  

## Dataframe record type  

#### <a name="make-df"></a> procedure: `(make-dataframe alist)`  
**returns:** a dataframe record type with three fields: alist, names, and dim  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6))))

> df
#[#{dataframe cziqfonusl4ihl0gdwa8clop7-3} ((a 1 2 3) (b 4 5 6)) (a b) (3 . 2)]

> (dataframe? df)
#t

> (dataframe? '((a 1 2 3) (b 4 5 6)))
#f

> (dataframe-alist df)
((a 1 2 3) (b 4 5 6))

> (dataframe-names df)
(a b)

> (dataframe-dim df)
(3 . 2)                  ; (rows . columns)

> (define df (make-dataframe '(("a" 1 2 3) ("b" 4 5 6))))

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-crossing"></a> procedure: `(dataframe-crossing obj1 obj2 ...)`  
**returns:** a dataframe formed from the cartesian products of `obj1`, `obj2`, etc.; objects must be either dataframes or lists with the same structure as a dataframe column, e.g., `'(col-name 1 2 3)` 

```
> (dataframe-display (dataframe-crossing '(col1 a b) '(col2 c d)))

 dim: 4 rows x 2 cols
  col1  col2 
     a     c 
     a     d 
     b     c 
     b     d 

> (dataframe-display 
    (dataframe-crossing '(col1 a b) (make-dataframe '((col2 c d)))))

 dim: 4 rows x 2 cols
  col1  col2 
     a     c 
     a     d 
     b     c 
     b     d 
     
> (dataframe-display 
    (dataframe-crossing (make-dataframe '((col1 a b) (col2 c d))) '(col3 e f)))

 dim: 4 rows x 3 cols
  col1  col2  col3 
     a     c     e 
     a     c     f 
     b     d     e 
     b     d     f 
```

#### <a name="dataframe-alist"></a> procedure: `(dataframe-alist df)`  
**returns:** an association list of the contents of dataframe `df` 

```
> (dataframe-alist (make-dataframe '((a 1 2 3) (b 4 5 6))))
((a 1 2 3) (b 4 5 6))
```

#### <a name="dataframe-names"></a> procedure: `(dataframe-names df)`  
**returns:** a list of symbols representing the names of columns in dataframe `df` 

```
> (dataframe-names (make-dataframe '((a 1) (b 2) (c 3) (d 4))))
(a b c d)
```

#### <a name="dataframe-dim"></a> procedure: `(dataframe-dim df)`  
**returns:** a pair of the number of rows and columns `(rows . columns)` in dataframe `df` 

```
> (dataframe-dim (make-dataframe '((a 1) (b 2) (c 3) (d 4))))
(1 . 4)
> (dataframe-dim (make-dataframe '((a 1 2 3) (b 4 5 6))))
(3 . 2)
```

#### <a name="df-display"></a> procedure: `(dataframe-display df [n min-width total-width])`  
**displays:** the dataframe `df` up to `n` rows and the number of columns that fit in `total-width` based on the actual contents of column or minimum column width `min-width`; `min-width` and `total-width` are measured in number of characters; default values: `n = 10`,  `min-width = 5`, `total-width = 80`  

```
> (define df
    (make-dataframe (list '(Boolean #t #f #t)
                          '(Char #\y #\e #\s)
                          '(String "these" "are" "strings")
                          '(Exact 1/2 1/3 1/4)
                          '(Integer 1 -2 3)
                          '(Expt 1e6 -123456 1.2346e-6)
                          '(Dec4 132.1 -157 10.234)   ; based on size of numbers
                          '(Dec2 1234 5784 -76833.123)
                          (list 'Other (cons 1 2) '(a b c) (make-dataframe '((a 2)))))))
                          
> (dataframe-display df)

 dim: 3 rows x 9 cols
  Boolean  Char   String  Exact  Integer       Expt       Dec4       Dec2        Other 
       #t     y    these    1/2       1.   1.000E+6   132.1000    1234.00       <pair> 
       #f     e      are    1/3      -2.  -1.235E+5  -157.0000    5784.00       <list> 
       #t     s  strings    1/4       3.   1.235E-6    10.2340  -76833.12  <dataframe> 
        
> (define df (make-dataframe (list (cons 'a (iota 15))
                                   (cons 'b (map add1 (iota 15))))))
  
> (dataframe-display df 5)

 dim: 15 rows x 2 cols
     a     b 
    0.    1. 
    1.    2. 
    2.    3. 
    3.    4. 
    4.    5. 
```

#### <a name="df-contains"></a> procedure: `(dataframe-contains? df name ...)`  
**returns:** `#t` if all column `names` are found in dataframe `df`, `#f` otherwise  

```
> (define df (make-dataframe '((a 1) (b 2) (c 3) (d 4))))

> (dataframe-contains? df 'a 'c 'd)
#t

> (dataframe-contains? df 'b 'e)
#f
```

#### <a name="df-head"></a> procedure: `(dataframe-head df n)`  
**returns:** a dataframe with first `n` rows from dataframe `df`  

#### <a name="df-tail"></a> procedure: `(dataframe-tail df n)`  
**returns:** a dataframe with the `n`th tail (zero-based) rows from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))

> (dataframe-display (dataframe-head df 3))

 dim: 3 rows x 3 cols
     a     b     c 
    1.    4.    7. 
    2.    5.    8. 
    3.    6.    9. 

> (dataframe-display (dataframe-tail df 2))

 dim: 3 rows x 3 cols
     a     b      c 
    1.    4.  -999. 
    2.    5.  -999. 
    3.    6.  -999. 
```

#### <a name="df-equal"></a> procedure: `(dataframe-equal? df1 df2 ...)`  
**returns:** `#t` if all dataframes are equal, `#f` otherwise  

```
> (dataframe-equal? (make-dataframe '((a 1 2 3) (b 4 5 6)))
                    (make-dataframe '((b 4 5 6) (a 1 2 3))))
#f

> (dataframe-equal? (make-dataframe '((a 1 2 3) (b 4 5 6)))
                    (make-dataframe '((a 10 2 3) (b 4 5 6))))
#f
```

#### <a name="df-write"></a> procedure: `(dataframe-write df path overwrite?)`  
**writes:** a dataframe `df` as a Scheme object to `path`; if file exists at `path`, operation will fail unless `overwrite?` is #t  

#### <a name="df-read"></a> procedure: `(dataframe-read path)`  
**returns:** a dataframe read from `path`

```
> (define df (make-dataframe '((grp "b" "b" "a" "b" "a")
                               (trt b b b a a)
                               (adult 5 4 2 3 1)
                               (juv 50 40 20 30 10))))

> (dataframe-display df)

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     b     b     5.   50. 
     b     b     4.   40. 
     a     b     2.   20. 
     b     a     3.   30. 
     a     a     1.   10.

> (dataframe-write df "df-example.scm" #t)

> (define df2 (dataframe-read "df-example.scm"))

> (dataframe-display df2)

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     b     b     5.   50. 
     b     b     4.   40. 
     a     b     2.   20. 
     b     a     3.   30. 
     a     a     1.   10. 
```

#### <a name="df-rows"></a> procedure: `(dataframe->rowtable df)`  
**returns:** a rowtable from dataframe `df`

```
;; a dataframe is a column-based data structure; a rowtable is a row-based data structure

> (define df (make-dataframe '((a 100 300) (b 4 6) (c 700 900))))

> (dataframe->rowtable df)
((a b c) (100 4 700) (300 6 900))
```

#### <a name="rows-df"></a> procedure: `(rowtable->dataframe rt [header try->number try-max])`  
**returns:** a dataframe from rowtable `rt`; if `header` is `#t` (default), the first row is used as column names, which must be either symbols or strings; string headers are converted to symbols; if `header` is `#f`, a header row is created; if `try->number` is `#t` (default), then, if the values in a column up to `try-max` (default = 100) are strings that can be converted to valid numbers, the whole column will be converted to numbers

```
;; a rowtable is a row-based data structure; a dataframe is a column-based data structure

> (dataframe-display 
    (rowtable->dataframe '((a b c) (1 4 7) (2 5 8) (3 6 9))))
 dim: 3 rows x 3 cols
     a     b     c 
    1.    4.    7. 
    2.    5.    8. 
    3.    6.    9. 

> (dataframe-display 
    (rowtable->dataframe '((1 4 7) (2 5 8) (3 6 9)) #f))

 dim: 3 rows x 3 cols
    V0    V1    V2 
    1.    4.    7. 
    2.    5.    8. 
    3.    6.    9. 

> (dataframe-display 
    (rowtable->dataframe '(("a" "b" "c") (1 4 7) (2 5 8) (3 6 9))))

 dim: 3 rows x 3 cols
     a     b     c 
    1.    4.    7. 
    2.    5.    8. 
    3.    6.    9.

> (rowtable->dataframe '((a b c) ("1" 4 7) ("2" 5 8) ("NA" 6 9)))

;; dataframe-display doesn't show strings as quoted
#[#{dataframe h4486zjeizt8ip8xdgq9nfy6u-51} 
((a "1" "2" "NA") (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]

> (dataframe-display 
    (rowtable->dataframe '((a b c) ("1" 4 7) ("2" 5 8) ("NA" 6 9)) #t #t 2))

 dim: 3 rows x 3 cols
     a     b     c 
     1    4.    7. 
     2    5.    8. 
    #f    6.    9. 
```

#### <a name="df-ref"></a> procedure: `(dataframe-ref df indices [name ...])`  
**returns:** a dataframe with rows specified by `indices` (zero-based) from dataframe `df`; optionally, can specify column `names` to return; defaults to all columns  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-ref df '(0 2 4)))

 dim: 3 rows x 4 cols
   grp   trt  adult   juv 
     a     a     1.   10. 
     b     a     3.   30. 
     b     b     5.   5

> (dataframe-display (dataframe-ref df '(0 2 4) 'adult 'juv))

 dim: 3 rows x 2 cols
  adult   juv 
     1.   10. 
     3.   30. 
     5.   50. 
```

#### <a name="df-values"></a> procedure: `(dataframe-values df name)`  
**returns:** a list of values for column `name` from dataframe `df`  

#### <a name="df-values-unique"></a> procedure: `(dataframe-values-unique df name)`  
**returns:** a list of unique values for column `name` from dataframe `df`  

```
> (define df (make-dataframe '((a 100 200 300) (b 4 5 6) (c 700 800 900))))

> (dataframe-values df 'b)
(4 5 6)

> ($ df 'b)                   ; $ is shorthand for dataframe-values; inspired by R, e.g., df$b.
(4 5 6)

> (map (lambda (name) ($ df name)) '(c a))
((700 800 900) (100 200 300))

> (define df1 (make-dataframe '((x a a b) (y c d e))))

> (dataframe-values-unique df1 'x)
(a b)

> (dataframe-values-unique df1 'y)
(c d e)
```

## Select, drop, and rename columns  

#### <a name="df-select"></a> procedure: `(dataframe-select df name ...)`  
**returns:** a dataframe of columns with `names` selected from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-display (dataframe-select df 'a))

 dim: 3 rows x 1 cols
     a 
    1. 
    2. 
    3. 

> (dataframe-display (dataframe-select df 'c 'b))

 dim: 3 rows x 2 cols
     c     b 
    7.    4. 
    8.    5. 
    9.    6. 
```

#### <a name="df-drop"></a> procedure: `(dataframe-drop df name ...)`  
**returns:** a dataframe of columns with `names` dropped from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-display (dataframe-drop df 'c 'b))

 dim: 3 rows x 1 cols
     a 
    1. 
    2. 
    3.

> (dataframe-display (dataframe-drop df 'a))

 dim: 3 rows x 2 cols
     b     c 
    4.    7. 
    5.    8. 
    6.    9. 
```

#### <a name="df-rename"></a> procedure: `(dataframe-rename df name-pairs ...)`  
**returns:** a dataframe with column names from dataframe `df` renamed according to `name-pairs`, which takes the form `'(old-name new-name)` 

#### <a name="df-rename-all"></a> procedure: `(dataframe-rename-all df names)`  
**returns:** a dataframe with `names` replacing column names from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-display (dataframe-rename df '(b Bee) '(c Sea)))

 dim: 3 rows x 3 cols
     a   Bee   Sea 
    1.    4.    7. 
    2.    5.    8. 
    3.    6.    9. 

> (dataframe-display (dataframe-rename-all df '(A B C)))

 dim: 3 rows x 3 cols
     A     B     C 
    1.    4.    7. 
    2.    5.    8. 
    3.    6.    9.

> (dataframe-rename-all df '(A B C D))
Exception in (dataframe-rename-all df names): names length must be 3, not 4
```

## Filter and sort  

#### <a name="df-unique"></a> procedure: `(dataframe-unique df)`  
**returns:** a dataframe with only the unique rows of dataframe `df`  

```
> (define df (make-dataframe '((Name "Peter" "Paul" "Mary" "Peter")
                               (Pet "Rabbit" "Cat" "Dog" "Rabbit"))))

> (dataframe-display (dataframe-unique df))

 dim: 3 rows x 2 cols
     Name       Pet 
     Paul       Cat 
     Mary       Dog 
    Peter    Rabbit  

> (define df2 (make-dataframe '((grp a a b b b)
                                (trt a b a b b)
                                (adult 1 2 3 4 5)
                                (juv 10 20 30 40 50))))

> (dataframe-display 
    (dataframe-unique (dataframe-select df2 'grp 'trt)))

 dim: 4 rows x 2 cols
   grp   trt 
     a     a 
     a     b 
     b     a 
     b     b 
```

#### <a name="df-filter"></a> procedure: `(dataframe-filter df names procedure)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to columns `names`

#### <a name="df-filter*"></a> procedure: `(dataframe-filter* df names expr)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `expr` applied to columns `names`

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-filter df '(adult) (lambda (adult) (> adult 3))))

 dim: 2 rows x 4 cols
   grp   trt  adult   juv 
     b     b     4.   40. 
     b     b     5.   50. 

> (dataframe-display (dataframe-filter* df (adult) (> adult 3)))

 dim: 2 rows x 4 cols
   grp   trt  adult   juv 
     b     b     4.   40. 
     b     b     5.   50. 

> (dataframe-display 
    (dataframe-filter df '(grp juv) (lambda (grp juv) (and (symbol=? grp 'b) (< juv 50)))))

 dim: 2 rows x 4 cols
   grp   trt  adult   juv 
     b     a     3.   30. 
     b     b     4.   40. 

> (dataframe-display 
    (dataframe-filter* df (grp juv) (and (symbol=? grp 'b) (< juv 50))))

 dim: 2 rows x 4 cols
   grp   trt  adult   juv 
     b     a     3.   30. 
     b     b     4.   40. 
```

#### <a name="df-filter-at"></a> procedure: `(dataframe-filter-at df procedure name ...)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to columns `names`  

#### <a name="df-filter-all"></a> procedure: `(dataframe-filter-all df procedure)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to all columns  

```
> (define df (make-dataframe '((a 1 "NA" 3)
                               (b "NA" 5 6)
                               (c 7 "NA" 9))))

> (dataframe-display df)

  dim: 3 rows x 3 cols
     a     b     c 
     1    NA     7 
    NA     5    NA 
     3     6     9 
         
> (dataframe-display (dataframe-filter-at df number? 'a 'c))

 dim: 2 rows x 3 cols
     a     b     c 
    1.    NA    7. 
    3.     6    9. 
         
> (dataframe-display (dataframe-filter-all df number?))

 dim: 1 rows x 3 cols
     a     b     c 
    3.    6.    9. 
```

#### <a name="df-partition"></a> procedure: `(dataframe-partition df names procedure)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned based on `procedure` applied to columns `names`  

#### <a name="df-partition*"></a> procedure: `(dataframe-partition* df names expr)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned based on `expr` applied to columns `names`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (define-values (keep drop) 
    (dataframe-partition df '(adult) (lambda (adult) (> adult 3))))
> (define-values (keep drop) 
    (dataframe-partition* df (adult) (> adult 3)))

> (dataframe-display keep)

 dim: 2 rows x 4 cols
   grp   trt  adult   juv 
     b     b     4.   40. 
     b     b     5.   50. 

> (dataframe-display drop)

 dim: 3 rows x 4 cols
   grp   trt  adult   juv 
     a     a     1.   10. 
     a     b     2.   20. 
     b     a     3.   30. 
```

#### <a name="df-sort"></a> procedure: `(dataframe-sort df predicates names)`  
**returns:** a dataframe where the rows of dataframe `df` are sorted according a list of `predicate` procedures acting on a list of column `names` 

#### <a name="df-sort*"></a> procedure: `(dataframe-sort* df (predicate name) ...)`  
**returns:** a dataframe where the rows of dataframe `df` are sorted according to the `predicate name` pairings

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-sort df (list string>?) '(trt)))

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     a     b     2.   20. 
     b     b     4.   40. 
     b     b     5.   50. 
     a     a     1.   10. 
     b     a     3.   30. 

> (dataframe-display (dataframe-sort* df (string>? trt)))

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     a     b     2.   20. 
     b     b     4.   40. 
     b     b     5.   50. 
     a     a     1.   10. 
     b     a     3.   30. 

> (dataframe-display (dataframe-sort df (list string>? >) '(trt adult)))

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     b     b     5.   50. 
     b     b     4.   40. 
     a     b     2.   20. 
     b     a     3.   30. 
     a     a     1.   10. 

> (dataframe-display (dataframe-sort* df (string>? trt) (> adult)))

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     b     b     5.   50. 
     b     b     4.   40. 
     a     b     2.   20. 
     b     a     3.   30. 
     a     a     1.   10. 
```

## Split, bind, and append  

#### <a name="df-split"></a> procedure: `(dataframe-split df group-names ...)`  
**returns:** list of dataframes split into unique groups by `group-names` from dataframe `df`; requires that all values in each grouping column are the same type  
```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-split df 'grp)
(#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "a" "a") (trt "a" "b") (adult 1 2) (juv 10 20)) (grp trt adult juv) (2 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "b" "b" "b") (trt "a" "b" "b") (adult 3 4 5) (juv 30 40 50)) (grp trt adult juv) (3 . 4)])
  
> (dataframe-split df 'grp 'trt)
(#[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "a") (trt "a") (adult 1) (juv 10)) (grp trt adult juv) (1 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "a") (trt "b") (adult 2) (juv 20)) (grp trt adult juv) (1 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "b") (trt "a") (adult 3) (juv 30)) (grp trt adult juv) (1 . 4)]
  #[#{dataframe ovr2k7mu0mp76rg2arsmxbw6m-3} ((grp "b" "b") (trt "b" "b") (adult 4 5) (juv 40 50)) (grp trt adult juv) (2 . 4)])
```

#### <a name="df-bind"></a> procedure: `(dataframe-bind df1 df2 ...)`  
**returns:** a dataframe formed by binding only shared columns of the dataframes `df1 df2 ...`  

#### <a name="df-bind-all"></a> procedure: `(dataframe-bind-all missing-value df1 df2 ...)`  
**returns:** a dataframe formed by binding all columns of the dataframes `df1 df2 ...` where `missing-value` is used to fill values for columns that are not common to all dataframes  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (apply dataframe-bind (dataframe-split df 'grp 'trt)))

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     a     a     1.   10. 
     a     b     2.   20. 
     b     a     3.   30. 
     b     b     4.   40. 
     b     b     5.   50. 

> (define df1 (make-dataframe '((a 1 2 3) (b 10 20 30) (c 100 200 300))))

> (define df2 (make-dataframe '((a 4 5 6) (b 40 50 60))))

> (dataframe-display (dataframe-bind df1 df2))

 dim: 6 rows x 2 cols
     a     b 
    1.   10. 
    2.   20. 
    3.   30. 
    4.   40. 
    5.   50. 
    6.   60. 

> (dataframe-display (dataframe-bind df2 df1))

 dim: 6 rows x 2 cols
     a     b 
    4.   40. 
    5.   50. 
    6.   60. 
    1.   10. 
    2.   20. 
    3.   30. 

> (dataframe-display (dataframe-bind-all -999 df1 df2))

 dim: 6 rows x 3 cols
     a     b      c 
    1.   10.   100. 
    2.   20.   200. 
    3.   30.   300. 
    4.   40.  -999. 
    5.   50.  -999. 
    6.   60.  -999.  

> (dataframe-display (dataframe-bind-all -999 df2 df1))

 dim: 6 rows x 3 cols
     a     b      c 
    4.   40.  -999. 
    5.   50.  -999. 
    6.   60.  -999. 
    1.   10.   100. 
    2.   20.   200. 
    3.   30.   300. 
```

#### <a name="df-append"></a> procedure: `(dataframe-append df1 df2 ...)`  
**returns:** a dataframe formed by appending columns of the dataframes `df1 df2 ...`  

```
> (define df1 (make-dataframe '((a 1 2 3) (b 4 5 6))))

> (define df2 (make-dataframe '((c 7 8 9) (d 10 11 12))))

> (dataframe-display (dataframe-append df1 df2))

 dim: 3 rows x 4 cols
     a     b     c     d 
    1.    4.    7.   10. 
    2.    5.    8.   11. 
    3.    6.    9.   12. 
  
> (dataframe-display (dataframe-append df2 df1))

 dim: 3 rows x 4 cols
     c     d     a     b 
    7.   10.    1.    4. 
    8.   11.    2.    5. 
    9.   12.    3.    6. 
```

## Join

#### <a name="df-left-join"></a> procedure: `(dataframe-left-join df1 df2 join-names missing-value)`  
**returns:** a dataframe formed by joining on the shared columns, `join-names`, of the dataframes `df1` and  `df2` where `df1` is the left dataframe; rows in `df1` not matched by any rows in `df2` are filled with `missing-value`; if a row in `df1` matches multiple rows in `df2`, then rows in `df1` will be repeated for all matching rows in `df2`

```
> (define df1 (make-dataframe '((site "b" "a" "c")
                               (habitat "grassland" "meadow" "woodland"))))

> (define df2 (make-dataframe '((site "c" "b" "c" "b")
                               (day 1 1 2 2)
                               (catch 10 12 20 24))))

> (dataframe-display (dataframe-left-join df1 df2 '(site) -999))

 dim: 5 rows x 4 cols
  site      habitat    day  catch 
     b    grassland     1.    12. 
     b    grassland     2.    24. 
     a       meadow  -999.  -999. 
     c     woodland     1.    10. 
     c     woodland     2.    20.  

> (dataframe-display (dataframe-left-join df2 df1 '(site) -999))

 dim: 4 rows x 4 cols
  site   day  catch      habitat 
     c    1.    10.     woodland 
     c    2.    20.     woodland 
     b    1.    12.    grassland 
     b    2.    24.    grassland 

> (define df3 (make-dataframe '((first "sam" "bob" "sam" "dan")
                               (last  "son" "ert" "jam" "man")
                               (age 10 20 30 40))))

> (define df4 (make-dataframe '((first "sam" "bob" "dan" "bob")
                               (last "son" "ert" "man" "ert")
                               (game 1 1 1 2)
                               (goals 0 1 2 3))))

> (dataframe-display (dataframe-left-join df3 df4 '(first last) -999))

 dim: 5 rows x 5 cols
  first   last   age   game  goals 
    sam    son   10.     1.     0. 
    bob    ert   20.     1.     1. 
    bob    ert   20.     2.     3. 
    sam    jam   30.  -999.  -999. 
    dan    man   40.     1.     2. 

> (dataframe-display (dataframe-left-join df4 df3 '(first last) -999))

 dim: 4 rows x 5 cols
  first   last  game  goals   age 
    sam    son    1.     0.   10. 
    dan    man    1.     2.   40. 
    bob    ert    1.     1.   20. 
    bob    ert    2.     3.   20. 
```

## Reshape

#### <a name="df-stack"></a> procedure: `(dataframe-stack df names names-to values-to)`  
**returns:** a dataframe formed by stacking pieces of a wide-format `df`; `names` is a list of column names to be combined into a single column; `names-to` is the name of the new column formed from the columns selected in `names`; `values-to` is the the name of the new column formed from the values in the columns selected in `names`

```
> (define df (make-dataframe '((day 1 2)
                               (hour 10 11)
                               (a 97 78)
                               (b 84 47)
                               (c 55 54))))

> (dataframe-display (dataframe-stack df '(a b c) 'site 'count))

 dim: 6 rows x 4 cols
   day  hour  site  count 
    1.   10.     a    97. 
    2.   11.     a    78. 
    1.   10.     b    84. 
    2.   11.     b    47. 
    1.   10.     c    55. 
    2.   11.     c    54. 

;; reshaping to long format is useful for aggregating
> (-> '((day 1 1 2 2)
        (hour 10 11 10 11)
        (a 97 78 83 80)
        (b 84 47 73 46)
        (c 55 54 38 58))
      (make-dataframe)
      (dataframe-stack '(a b c) 'site 'count)
      (dataframe-aggregate*
       (hour site)
       (total-count (count) (apply + count)))
      (dataframe-display))

 dim: 6 rows x 3 cols
  hour  site  total-count 
   10.     a         180. 
   11.     a         158. 
   10.     b         157. 
   11.     b          93. 
   10.     c          93. 
   11.     c         112. 
```

#### <a name="df-spread"></a> procedure: `(dataframe-spread df names-from values-from missing-value)`  
**returns:** a dataframe formed by spreading a long format `df` into a wide-format dataframe; `names-from` is the name of the column containing the names of the new columns; `values-from` is the the name of the column containing the values that will be spread across the new columns; `missing-value` is a scalar value used to fill combinations that are not found in the long format `df`

```
> (define df1 (make-dataframe '((day 1 1 2)
                                (grp "A" "B" "B")
                                (val 10 20 30))))

> (dataframe-display (dataframe-spread df1 'grp 'val -999))

 dim: 2 rows x 3 cols
   day      A     B 
    1.    10.   20. 
    2.  -999.   30.

> (define df2 (make-dataframe '((day 1 1 1 1 2 2 2 2)
                                (hour 10 10 11 11 10 10 11 11)
                                (grp a b a b a b a b)
                                (val 83 78 80 105 95 77 96 99))))

> (dataframe-display (dataframe-spread df2 'grp 'val -999))

 dim: 4 rows x 4 cols
   day  hour     a     b 
    1.   10.   83.   78. 
    1.   11.   80.  105. 
    2.   10.   95.   77. 
    2.   11.   96.   99. 
```

## Modify and aggregate  

#### <a name="df-modify"></a> procedure: `(dataframe-modify df new-names names procedure ...)`  
**returns:** a dataframe where the columns `names` of dataframe `df` are modified or added according to the `procedure`  

#### <a name="df-modify*"></a> procedure: `(dataframe-modify* df (new-name names expr) ...)`  
**returns:** a dataframe where the columns `names` of dataframe `df` are modified or added according to the `expr`  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
                               
;; if new name occurs in dataframe, then column is replaced
;; if not, then new column is added

;; if names is empty, 
;;   and expr is a scalar, then the scalar is repeated to match the number of rows in the dataframe
;;   and expr is a list of length equal to number of rows in dataframe, then the list is used as a column

> (dataframe-display
   (dataframe-modify
    df
    '(grp total scalar lst)
    '((grp) (adult juv) () ())
    (lambda (grp) (string-upcase grp))  
    (lambda (adult juv) (+ adult juv))
    (lambda () 42)
    (lambda () '(2 4 6 8 10))))
                                     
 dim: 5 rows x 7 cols
   grp   trt  adult   juv  total  scalar   lst 
     A     a     1.   10.    11.     42.    2. 
     A     b     2.   20.    22.     42.    4. 
     B     a     3.   30.    33.     42.    6. 
     B     b     4.   40.    44.     42.    8. 
     B     b     5.   50.    55.     42.   10. 

> (dataframe-display
   (dataframe-modify*
    df
    (grp (grp) (string-upcase grp))    
    (total (adult juv) (+ adult juv))
    (scalar () 42)
    (lst () '(2 4 6 8 10))))
                                     
 dim: 5 rows x 7 cols
   grp   trt  adult   juv  total  scalar   lst 
     A     a     1.   10.    11.     42.    2. 
     A     b     2.   20.    22.     42.    4. 
     B     a     3.   30.    33.     42.    6. 
     B     b     4.   40.    44.     42.    8. 
     B     b     5.   50.    55.     42.   10. 
```

#### <a name="df-modify-at"></a> procedure: `(dataframe-modify-at df procedure name ...)`  
**returns:** a dataframe where the specified columns (`names`) of dataframe `df` are modified based on `procedure`, which can only take one argument  

#### <a name="df-modify-all"></a> procedure: `(dataframe-modify-all df procedure)`  
**returns:** a dataframe where all columns of dataframe `df` are modified based on `procedure`, which can only take one argument  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
                               
> (dataframe-display (dataframe-modify-at df symbol->string 'grp 'trt))

 dim: 5 rows x 4 cols
   grp   trt  adult   juv 
     a     a     1.   10. 
     a     b     2.   20. 
     b     a     3.   30. 
     b     b     4.   40. 
     b     b     5.   50. 
  
> (define df2 (make-dataframe '((a 1 2 3)
                                (b 4 5 6)
                                (c 7 8 9))))
                                
> (dataframe-display
   (dataframe-modify-all df2 (lambda (x) (* x 100))))

 dim: 3 rows x 3 cols
     a     b     c 
  100.  400.  700. 
  200.  500.  800. 
  300.  600.  900. 
```

#### <a name="df-aggregate"></a> procedure: `(dataframe-aggregate df group-names new-names names procedure ...)`  
**returns:** a dataframe where the dataframe `df` is split according to list of `group-names` and aggregated according to the `procedure` applied to column(s) `names`

#### <a name="df-aggregate*"></a> procedure: `(dataframe-aggregate* df group-names (new-name names expr) ...)`  
**returns:** a dataframe where the dataframe `df` is split according to list of `group-names` and aggregated according to the `expr` applied to column(s) `names`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display
   (dataframe-aggregate
    df
    '(grp)
    '(adult-sum juv-sum)
    '((adult) (juv))
    (lambda (adult) (apply + adult))
    (lambda (juv) (apply + juv)))) 
                                                                
 dim: 2 rows x 3 cols
   grp  adult-sum  juv-sum 
     a         3.      30. 
     b        12.     120. 

> (dataframe-display
   (dataframe-aggregate*
    df
    (grp)
    (adult-sum (adult) (apply + adult))
    (juv-sum (juv) (apply + juv))))
                                        
 dim: 2 rows x 3 cols
   grp  adult-sum  juv-sum 
     a         3.      30. 
     b        12.     120. 

> (dataframe-display
   (dataframe-aggregate
    df
    '(grp trt)
    '(adult-sum juv-sum)
    '((adult) (juv))
    (lambda (adult) (apply + adult))
    (lambda (juv) (apply + juv))))

 dim: 4 rows x 4 cols
   grp   trt  adult-sum  juv-sum 
     a     a         1.      10. 
     a     b         2.      20. 
     b     a         3.      30. 
     b     b         9.      90. 

> (dataframe-display
   (dataframe-aggregate*
    df
    (grp trt)
    (adult-sum (adult) (apply + adult))
    (juv-sum (juv) (apply + juv))))
                                        
 dim: 4 rows x 4 cols
   grp   trt  adult-sum  juv-sum 
     a     a         1.      10. 
     a     b         2.      20. 
     b     a         3.      30. 
     b     b         9.      90. 
```

## Thread first and thread last

#### <a name="thread-first"></a> procedure: `(-> expr ...)`  
**returns:** an object derived from passing result of previous expression `expr` as input to *first* argument of the next `expr`  

#### <a name="thread-last"></a> procedure: `(->> expr ...)`  
**returns:** an object derived from passing result of previous expression `expr` as input to *last* argument of the next `expr`  

```
> (define (mean ls) (/ (apply + ls) (length ls)))

> (-> '(1 2 3) (mean) (+ 10))
12

> (define x (-> '(1 2 3) (->> (apply +))))
> x
6

> (-> '((grp a a b b b)
        (trt a b a b b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (make-dataframe)
      (dataframe-modify*
       (total (adult juv) (+ adult juv)))
      (dataframe-display))
      
 dim: 5 rows x 5 cols
   grp   trt  adult   juv  total 
     a     a     1.   10.    11. 
     a     b     2.   20.    22. 
     b     a     3.   30.    33. 
     b     b     4.   40.    44. 
     b     b     5.   50.    55. 
  
> (-> '((grp a a b b b)
        (trt a b a b b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (make-dataframe)
      (dataframe-split 'grp)
      (->> (map (lambda (df)
                  (dataframe-modify*
                   df
                   (juv-mean () (mean ($ df 'juv)))))))
      (->> (apply dataframe-bind))
      (dataframe-filter* (juv juv-mean) (> juv juv-mean))
      (dataframe-display))
       
 dim: 2 rows x 5 cols
   grp   trt  adult   juv  juv-mean 
     a     b     2.   20.       15. 
     b     b     5.   50.       40. 
```


