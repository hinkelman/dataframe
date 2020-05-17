# Chez Scheme Dataframe Library

A dataframe record type for Chez Scheme with procedures to select, drop, and rename columns, and filter, sort, split, bind, append, modify, and aggregate dataframes. 

Related blog posts:  
[A dataframe record type for Chez Scheme](https://www.travishinkelman.com/posts/dataframe-record-type-for-chez-scheme/)  
[Select, drop, and rename dataframe columns in Chez Scheme](https://www.travishinkelman.com/posts/select-drop-rename-dataframe-columns-chez-scheme/)  
[Split, bind, and append dataframes in Chez Scheme](https://www.travishinkelman.com/posts/split-bind-append-dataframes-chez-scheme/)  
[Filter, partition, and sort dataframes in Chez Scheme](https://www.travishinkelman.com/posts/filter-partition-and-sort-dataframes-in-chez-scheme/)  

## Installation

Clone or download this repository. Move `dataframe.sls` and `dataframe` folder from downloaded and unzipped folder to one of the directories listed when you run `(library-directories)` in Chez Scheme. For more information on installing Chez Scheme libraries, see blog posts for [macOS and Windows](https://www.travishinkelman.com/posts/getting-started-with-chez-scheme-and-emacs/) or [Ubuntu](https://www.travishinkelman.com/posts/getting-started-with-chez-scheme-and-emacs-ubuntu/).

## Import

`(import (dataframe))`

## Table of Contents  

### Dataframe record type  

[`(make-dataframe alist)`](#make-df)  
[`(dataframe-display df n pad min-width total-width)`](#df-display)  
[`(dataframe-contains df name ...)`](#df-contains)  
[`(dataframe-head df n)`](#df-head)  
[`(dataframe-tail df n)`](#df-tail)  
[`(dataframe-equal? df1 df2 ...)`](#df-equal)  
[`(dataframe-write df path overwrite?)`](#df-write)  
[`(dataframe-read path)`](#df-read)  
[`(dataframe->rowtable df)`](#df-rows)  
[`(rowtable->dataframe rt header?)`](#rows-df)  
[`(dataframe-ref df indices name ...)`](#df-ref)  
[`(dataframe-values df name)`](#df-values)  
[`(dataframe-values-unique df name)`](#df-values-unique)  

### Select, drop, and rename columns  

[`(dataframe-select df name ...)`](#df-select)  
[`(dataframe-drop df name ...)`](#df-drop)  
[`(dataframe-rename df name-pairs)`](#df-rename)  
[`(dataframe-rename-all df names)`](#df-rename-all)  

### Filter and sort  

[`(dataframe-unique df)`](#df-unique)  
[`(filter-expr (names) (expr))`](#filter-expr)  
[`(dataframe-filter df filter-expr)`](#df-filter)  
[`(dataframe-filter-at df procedure name ...)`](#df-filter-at)  
[`(dataframe-filter-all df procedure)`](#df-filter-all)  
[`(dataframe-partition df filter-expr)`](#df-partition)  
[`(sort-expr (predicate name) ...)`](#sort-expr)  
[`(dataframe-sort df sort-expr)`](#df-sort)  

### Split, bind, and append  

[`(dataframe-split df group-name ...)`](#df-split)  
[`(dataframe-bind df1 df2 ...)`](#df-bind)  
[`(dataframe-bind-all missing-value df1 df2 ...)`](#df-bind-all)  
[`(dataframe-append df1 df2 ...)`](#df-append)  

### Modify and aggregate  

[`(modify-expr (new-name (names) (expr)) ...)`](#modify-expr)  
[`(dataframe-modify df modify-expr)`](#df-modify)  
[`(dataframe-modify-at df procedure name ...)`](#df-modify-at)  
[`(dataframe-modify-all df procedure)`](#df-modify-all)  
[`(aggregate-expr (new-name (names) (expr)) ...)`](#aggregate-expr)  
[`(dataframe-aggregate df group-names aggregate-expr)`](#df-aggregate)  

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

#### <a name="df-display"></a> procedure: `(dataframe-display df n pad min-width total-width)`  
**displays:** the dataframe `df` up to `n` rows and the number of columns that fit in `total-width` based on the amount of padding `pad` and the actual (based on contents of column) or minimum column width `min-width`; `pad`, `min-width`, and `total-width` are measured in number of characters; default values: `n = 10`, `pad = 2`, `min-width = 10`, `total-width = 80`  

```
> (define df (make-dataframe (list (cons 'a (iota 15))
                                   (cons 'b (map add1 (iota 15))))))
  
> (dataframe-display df 5)
         a         b
         0         1
         1         2
         2         3
         3         4
         4         5
```

#### <a name="df-contains"></a> procedure: `(dataframe-contains df name ...)`  
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
         a         b         c
         1         4         7
         2         5         8
         3         6         9

> (dataframe-display (dataframe-tail df 2))
         a         b         c
         3         6         9
         1         4      -999
         2         5      -999
         3         6      -999
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
       grp       trt     adult       juv
         b         b         5        50
         b         b         4        40
         a         b         2        20
         b         a         3        30
         a         a         1        10

> (dataframe-write df "df-example.scm" #t)

> (define df2 (dataframe-read "df-example.scm"))

> (dataframe-display df2)
       grp       trt     adult       juv
         b         b         5        50
         b         b         4        40
         a         b         2        20
         b         a         3        30
         a         a         1        10
```

#### <a name="df-rows"></a> procedure: `(dataframe->rowtable df)`  
**returns:** a rowtable from dataframe `df`

```
;; a dataframe is a column-based data structure; a rowtable is a row-based data structure

> (define df (make-dataframe '((a 100 300) (b 4 6) (c 700 900))))

> (dataframe->rowtable df)
((a b c) (100 4 700) (300 6 900))
```

#### <a name="rows-df"></a> procedure: `(rowtable->dataframe rt header?)`  
**returns:** a dataframe from rowtable `rt`; if `header?` is `#f` a header row is created

```
;; a rowtable is a row-based data structure; a dataframe is a column-based data structure

> (dataframe-display (rowtable->dataframe '((a b c) (1 4 7) (2 5 8) (3 6 9)) #t))
         a         b         c
         1         4         7
         2         5         8
         3         6         9

> (dataframe-display (rowtable->dataframe '((1 4 7) (2 5 8) (3 6 9)) #f))
        V0        V1        V2
         1         4         7
         2         5         8
         3         6         9

> (rowtable->dataframe '(("a" "b" "c") (1 4 7) (2 5 8) (3 6 9)) #t)

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-ref"></a> procedure: `(dataframe-ref df indices name ...)`  
**returns:** a dataframe with rows specified by `indices` (zero-based) from dataframe `df`; optionally, can specify column `names` to return; defaults to all columns  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-ref df '(0 2 4)))
       grp       trt     adult       juv
         a         a         1        10
         b         a         3        30
         b         b         5        50

> (dataframe-display (dataframe-ref df '(0 2 4) 'adult 'juv))
     adult       juv
         1        10
         3        30
         5        50
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
         a
         1
         2
         3

> (dataframe-display (dataframe-select df 'c 'b))
         c         b
         7         4
         8         5
         9         6
```

#### <a name="df-drop"></a> procedure: `(dataframe-drop df name ...)`  
**returns:** a dataframe of columns with `names` dropped from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-display (dataframe-drop df 'c 'b))
         a
         1
         2
         3

> (dataframe-display (dataframe-drop df 'a))
         b         c
         4         7
         5         8
         6         9
```

#### <a name="df-rename"></a> procedure: `(dataframe-rename df name-pairs)`  
**returns:** a dataframe with column names from dataframe `df` renamed according to `name-pairs`  

#### <a name="df-rename-all"></a> procedure: `(dataframe-rename-all df names)`  
**returns:** a dataframe with `names` replacing column names from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))

> (dataframe-display (dataframe-rename df '((b Bee) (c Sea))))
         a       Bee       Sea
         1         4         7
         2         5         8
         3         6         9

> (dataframe-display (dataframe-rename-all df '(A B C)))
         A         B         C
         1         4         7
         2         5         8
         3         6         9

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
      Name       Pet
      Paul       Cat
      Mary       Dog
     Peter    Rabbit

> (define df2 (make-dataframe '((grp a a b b b)
                                (trt a b a b b)
                                (adult 1 2 3 4 5)
                                (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-unique (dataframe-select df2 'grp 'trt)))
       grp       trt
         a         a
         a         b
         b         a
         b         b
```

#### <a name="filter-expr"></a> procedure: `(filter-expr (names) (expr))`  
**returns:** a list where the first element is a list of column `names` and the second element is a lambda procedure based on `expr`  

```
> (filter-expr (adult) (> adult 3))
((adult) #<procedure>)

> (filter-expr (grp juv) (and (symbol=? grp 'b) (< juv 50)))
((grp juv) #<procedure>)
```

#### <a name="df-filter"></a> procedure: `(dataframe-filter df filter-expr)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered according to the `filter-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-filter df (filter-expr (adult) (> adult 3))))
       grp       trt     adult       juv
         b         b         4        40
         b         b         5        50

> (dataframe-display
   (dataframe-filter df (filter-expr (grp juv) (and (symbol=? grp 'b) (< juv 50)))))
       grp       trt     adult       juv
         b         a         3        30
         b         b         4        40
```

#### <a name="df-filter-at"></a> procedure: `(dataframe-filter-at df procedure name ...)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to specified columns (`names`)  

#### <a name="df-filter-all"></a> procedure: `(dataframe-filter-all df procedure)`  
**returns:** a dataframe where the rows of dataframe `df` are filtered based on `procedure` applied to all columns  

```
> (define df (make-dataframe '((a 1 "NA" 3)
                               (b "NA" 5 6)
                               (c 7 "NA" 9))))
                               
> (dataframe-display df)
         a         b         c
         1        NA         7
        NA         5        NA
         3         6         9
         
> (dataframe-display (dataframe-filter-at df number? 'a 'c))
         a         b         c
         1        NA         7
         3         6         9
         
> (dataframe-display (dataframe-filter-all df number?))
         a         b         c
         3         6         9
```


#### <a name="df-partition"></a> procedure: `(dataframe-partition df filter-expr)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned according to the `filter-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (define-values (keep drop) (dataframe-partition df (filter-expr (adult) (> adult 3))))

> (dataframe-display keep)
       grp       trt     adult       juv
         b         b         4        40
         b         b         5        50

> (dataframe-display drop)
       grp       trt     adult       juv
         a         a         1        10
         a         b         2        20
         b         a         3        30
```

#### <a name="sort-expr"></a> procedure: `(sort-expr (predicate name) ...)`  
**returns:** a list where the first element is a list of `predicate` procedures and the second element is a list of column `names`  

```
> (sort-expr (string<? trt))
((#<procedure string<?>) (trt))

> (sort-expr (string<? trt) (< adult))
((#<procedure string<?> #<procedure <>) (trt adult))
```

#### <a name="df-sort"></a> procedure: `(dataframe-sort df sort-expr)`  
**returns:** a dataframe where the rows of dataframe `df` are sorted according to the `sort-expr`  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))

> (dataframe-display (dataframe-sort df (sort-expr (string>? trt))))
       grp       trt     adult       juv
         a         b         2        20
         b         b         4        40
         b         b         5        50
         a         a         1        10
         b         a         3        30

> (dataframe-display (dataframe-sort df (sort-expr (string>? trt) (> adult))))
       grp       trt     adult       juv
         b         b         5        50
         b         b         4        40
         a         b         2        20
         b         a         3        30
         a         a         1        10
```

## Split, bind, and append  

#### <a name="df-split"></a> procedure: `(dataframe-split df group-names ...)`  
**returns:** list of dataframes split into unique groups by `group-names` from dataframe `df`  

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
       grp       trt     adult       juv
         a         a         1        10
         a         b         2        20
         b         a         3        30
         b         b         4        40
         b         b         5        50

> (define df1 (make-dataframe '((a 1 2 3) (b 10 20 30) (c 100 200 300))))

> (define df2 (make-dataframe '((a 4 5 6) (b 40 50 60))))

> (dataframe-display (dataframe-bind df1 df2))
         a         b
         1        10
         2        20
         3        30
         4        40
         5        50
         6        60

> (dataframe-display (dataframe-bind df2 df1))
         a         b
         4        40
         5        50
         6        60
         1        10
         2        20
         3        30

> (dataframe-display (dataframe-bind-all -999 df1 df2))
         a         b         c
         1        10       100
         2        20       200
         3        30       300
         4        40      -999
         5        50      -999
         6        60      -999

> > (dataframe-display (dataframe-bind-all -999 df2 df1))
         a         b         c
         4        40      -999
         5        50      -999
         6        60      -999
         1        10       100
         2        20       200
         3        30       300
```

#### <a name="df-append"></a> procedure: `(dataframe-append df1 df2 ...)`  
**returns:** a dataframe formed by appending columns of the dataframes `df1 df2 ...`  

```
> (define df1 (make-dataframe '((a 1 2 3) (b 4 5 6))))

> (define df2 (make-dataframe '((c 7 8 9) (d 10 11 12))))

> (dataframe-display (dataframe-append df1 df2))
         a         b         c         d
         1         4         7        10
         2         5         8        11
         3         6         9        12
  
> (dataframe-display (dataframe-append df2 df1))
         c         d         a         b
         7        10         1         4
         8        11         2         5
         9        12         3         6
```

## Modify and aggregate  

#### <a name="modify-expr"></a> procedure: `(modify-expr (new-name (names) (expr)) ...)`  
**returns:** a list where the first element is a list of new column names `new-name`, the second element is a list of lists of column `names`, and the third element is list of lambda procedures based on `expr`  

```
> (modify-expr (grp (grp) (symbol->string grp))
               (total (adult juv) (+ adult juv)))
((grp total) ((grp) (adult juv)) (#<procedure> #<procedure>))
```

#### <a name="df-modify"></a> procedure: `(dataframe-modify df modify-expr)`  
**returns:** a dataframe where the columns of dataframe `df` are modified or added according to the `modify-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
                               
;; if new name occurs in dataframe, then column is replaced
;; if not, then new column is added

;; if names is empty, 
;;   and expr is a scalar, then the scalar is repeated to match the number of rows in the dataframe
;;   and expr is a list of length equal to number of rows in dataframe, then the list is used as a column

> (dataframe-display
   (dataframe-modify df (modify-expr (grp (grp) (symbol->string grp))
                                     (total (adult juv) (+ adult juv))
                                     (scalar () 42)
                                     (lst () '(2 4 6 8 10)))))
                                     
       grp       trt     adult       juv     total    scalar       lst
         a         a         1        10        11        42         2
         a         b         2        20        22        42         4
         b         a         3        30        33        42         6
         b         b         4        40        44        42         8
         b         b         5        50        55        42        10

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
                               
> (dataframe-alist (dataframe-modify-at df symbol->string 'grp 'trt))
((grp "a" "a" "b" "b" "b")
  (trt "a" "b" "a" "b" "b")
  (adult 1 2 3 4 5)
  (juv 10 20 30 40 50))
  
> (define df2 (make-dataframe '((a 1 2 3)
                                (b 4 5 6)
                                (c 7 8 9))))
                                
> (dataframe-display
   (dataframe-modify-all df2 (lambda (x) (* x 100))))
         a         b         c
       100       400       700
       200       500       800
       300       600       900
```


#### <a name="aggregate-expr"></a> procedure: `(aggregate-expr (new-name (names) (expr)) ...)`  
**returns:** a list where the first element is a list of new column names `new-name`, the second element is a list of lists of column `names`, and the third element is list of lambda procedures based on `expr`  

```
> (aggregate-expr (adult-sum (adult) (apply + adult))
                (juv-sum (juv) (apply + juv)))
((adult-sum juv-sum) ((adult) (juv)) (#<procedure> #<procedure>))
```

#### <a name="df-aggregate"></a> procedure: `(dataframe-aggregate df group-names aggregate-expr)`  
**returns:** a dataframe where the dataframe `df` was split according to list of `group-names` and aggregated according to `aggregate-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
                               
> (dataframe-display
   (dataframe-aggregate df
                        '(grp)
                        (aggregate-expr (adult-sum (adult) (apply + adult))
                                        (juv-sum (juv) (apply + juv)))))
                                        
       grp  adult-sum   juv-sum
         a          3        30
         b         12       120

> (dataframe-display
   (dataframe-aggregate df
                        '(grp trt)
                        (aggregate-expr (adult-sum (adult) (apply + adult))
                                        (juv-sum (juv) (apply + juv)))))
                                        
       grp       trt  adult-sum   juv-sum
         a         a          1        10
         a         b          2        20
         b         a          3        30
         b         b          9        90
```

## [Thread first and thread last](https://lispdreams.wordpress.com/2016/04/10/thread-first-thread-last-and-partials-oh-my/)  

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
      (dataframe-modify
       (modify-expr (total (adult juv) (+ adult juv))))
      (dataframe-display))
      
       grp       trt     adult       juv     total
         a         a         1        10        11
         a         b         2        20        22
         b         a         3        30        33
         b         b         4        40        44
         b         b         5        50        55
  
> (-> '((grp a a b b b)
        (trt a b a b b)
        (adult 1 2 3 4 5)
        (juv 10 20 30 40 50))
      (make-dataframe)
      (dataframe-split 'grp)
      (->> (map (lambda (df)
                  (dataframe-modify
                   df
                   (modify-expr (juv-mean () (mean ($ df 'juv))))))))
      (->> (apply dataframe-bind))
      (dataframe-filter (filter-expr (juv juv-mean) (> juv juv-mean)))
      (dataframe-display))
      
       grp       trt     adult       juv  juv-mean
         a         b         2        20        15
         b         b         5        50        40
```


