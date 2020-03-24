# Chez Scheme Dataframe Library

A dataframe record type for Chez Scheme with procedures to select, drop, and rename columns, and filter, sort, split, append, modify, and aggregate dataframes. 

## Installation and Import

```
$ cd ~/scheme # where '~/scheme' is the path to your Chez Scheme libraries
$ git clone git://github.com/hinkelman/dataframe.git
```

For more information on installing Chez Scheme libraries, see blog posts for [macOS and Windows](https://www.travishinkelman.com/posts/getting-started-with-chez-scheme-and-emacs/) or [Ubuntu](https://www.travishinkelman.com/posts/getting-started-with-chez-scheme-and-emacs-ubuntu/).

Import all `dataframe` procedures: `(import (dataframe df))`

## Table of Contents  

### Dataframe record type  

[`(make-dataframe alist)`](#make-df)  
[`(dataframe-contains df name ...)`](#df-contains)  
[`(dataframe-head df n)`](#df-head)  
[`(dataframe-tail df n)`](#df-tail)  
[`(dataframe-equal? df1 df2 ...)`](#df-equal)  
[`(dataframe-write df path overwrite?)`](#df-write)  
[`(dataframe-read path)`](#df-read)  
[`(dataframe->rowtable df)`](#df-rows)  
[`(rowtable->dataframe rt header?)`](#rows-df)  
[`(dataframe-values df name)`](#df-values)  

### Select, drop, and rename columns  

[`(dataframe-select df name ...)`](#df-select)  
[`(dataframe-drop df name ...)`](#df-drop)  
[`(dataframe-names-update df names)`](#df-names-update)  
[`(dataframe-rename df name-pairs)`](#df-rename)  

### Filter and sort  

[`(dataframe-unique df)`](#df-unique)  
[`(filter-expr (names) (expr))`](#filter-expr)  
[`(dataframe-filter df filter-expr)`](#df-filter)  
[`(dataframe-partition df filter-expr)`](#df-partition)  
[`(sort-expr (predicate name) ...)`](#sort-expr)  
[`(dataframe-sort df sort-expr)`](#df-sort)  

### Append and split  

### Modify and aggregate  

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
(3 . 2)
> (define df (make-dataframe '(("a" 1 2 3) ("b" 4 5 6))))

Exception in (make-dataframe alist): names are not symbols
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

```
> (define df (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))
> (dataframe-head df 3)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3) (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]
```

#### <a name="df-tail"></a> procedure: `(dataframe-tail df n)`  
**returns:** a dataframe with the `n`th tail (zero-based) rows from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3 1 2 3) (b 4 5 6 4 5 6) (c 7 8 9 -999 -999 -999))))
> (dataframe-tail df 2)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 3 1 2 3) (b 6 4 5 6) (c 9 -999 -999 -999)) (a b c) (4 . 3)]
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

```
> (define df (make-dataframe '((grp "b" "b" "a" "b" "a")
                               (trt b b b a a)
                               (adult 5 4 2 3 1)
                               (juv 50 40 20 30 10))))
> df
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((grp "b" "b" "a" "b" "a") (trt b b b a a) (adult 5 4 2 3 1) (juv 50 40 20 30 10)) (grp trt adult juv) (5 . 4)]
> (dataframe-write df "df-example.scm" #t)
```

#### <a name="df-read"></a> procedure: `(dataframe-read path)`  
**returns:** a dataframe read from `path`

```
;; example requires that you first run code for `dataframe-write`
> (define df2 (dataframe-read "df-example.scm"))
> df2
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((grp "b" "b" "a" "b" "a") (trt b b b a a) (adult 5 4 2 3 1) (juv 50 40 20 30 10)) (grp trt adult juv) (5 . 4)]
```

#### <a name="df-rows"></a> procedure: `(dataframe->rowtable df)`  
**returns:** a rowtable from dataframe `df`

```
;; a dataframe is a column-based data structure; a rowtable is a row-based data structure
> (define df (make-dataframe '((a 100 300) (b 4 6) (c 700 900))))
> df
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 100 300) (b 4 6) (c 700 900)) (a b c) (2 . 3)]
> (dataframe->rowtable df)
((a b c) (100 4 700) (300 6 900))
```

#### <a name="rows-df"></a> procedure: `(rowtable->dataframe rt header?)`  
**returns:** a dataframe from rowtable `rt`; if `header?` is `#f` a header row is created

```
;; a rowtable is a row-based data structure; a dataframe is a column-based data structure
> (rowtable->dataframe '((a b c) (1 4 7) (2 5 8) (3 6 9)) #t)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3) (b 4 5 6) (c 7 8 9)) (a b c) (3 . 3)]
> (rowtable->dataframe '((1 4 7) (2 5 8) (3 6 9)) #f)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((V0 1 2 3) (V1 4 5 6) (V2 7 8 9)) (V0 V1 V2) (3 . 3)]
> (rowtable->dataframe '(("a" "b" "c") (1 4 7) (2 5 8) (3 6 9)) #t)

Exception in (make-dataframe alist): names are not symbols
```

#### <a name="df-values"></a> procedure: `(dataframe-values df name)`  
**returns:** a list of values for column `name` from dataframe `df`  

```
> (define df (make-dataframe '((a 100 200 300) (b 4 5 6) (c 700 800 900))))
> (dataframe-values df 'b)
(4 5 6)
> ($ df 'b)                   ; $ is shorthand for dataframe-values; inspired by R, e.g., df$b.
(4 5 6)
> (map (lambda (name) ($ df name)) '(c a))
((700 800 900) (100 200 300))
```

## Select, drop, and rename columns  

#### <a name="df-select"></a> procedure: `(dataframe-select df name ...)`  
**returns:** a dataframe of columns with `names` selected from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-select df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]
> (dataframe-select df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((c 7 8 9) (b 4 5 6)) (c b) (3 . 2)]
```

#### <a name="df-drop"></a> procedure: `(dataframe-drop df name ...)`  
**returns:** a dataframe of columns with `names` dropped from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-drop df 'c 'b)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((a 1 2 3)) (a) (3 . 1)]
> (dataframe-drop df 'a)
#[#{dataframe cicwkcvn4jmyzsjt96biqhpwp-3} ((b 4 5 6) (c 7 8 9)) (b c) (3 . 2)]
```

#### <a name="df-names-update"></a> procedure: `(dataframe-names-update df names)`  
**returns:** a dataframe with `names` replacing column names from dataframe `df`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-names-update df '(A B C))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((A 1 2 3) (B 4 5 6) (C 7 8 9)) (A B C) (3 . 3)]
> (dataframe-names-update df '(A B C D))

Exception in (dataframe-names-update df names): names length must be 3, not 4
```

#### <a name="df-rename"></a> procedure: `(dataframe-rename df name-pairs)`  
**returns:** a dataframe with column names from dataframe `df` renamed according to `name-pairs`  

```
> (define df (make-dataframe '((a 1 2 3) (b 4 5 6) (c 7 8 9))))
> (dataframe-rename df '((b Bee) (c Sea)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((a 1 2 3) (Bee 4 5 6) (Sea 7 8 9)) (a Bee Sea) (3 . 3)]
```

## Filter and sort  

#### <a name="df-unique"></a> procedure: `(dataframe-unique df)`  
**returns:** a dataframe with only the unique rows of dataframe `df`  

```
> (define df (make-dataframe '((Name "Peter" "Paul" "Mary" "Peter") (Pet "Rabbit" "Cat" "Dog" "Rabbit"))))
> (dataframe-unique df)
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((Name "Paul" "Mary" "Peter") (Pet "Cat" "Dog" "Rabbit")) (Name Pet) (3 . 2)]
> (define df2 (make-dataframe '((grp a a b b b)
                                (trt a b a b b)
                                (adult 1 2 3 4 5)
                                (juv 10 20 30 40 50))))
> (dataframe-unique (dataframe-select df2 'grp 'trt))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp a a b b) (trt a b a b)) (grp trt) (4 . 2)]
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
> (dataframe-filter df (filter-expr (adult) (> adult 3)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp b b) (trt b b) (adult 4 5) (juv 40 50)) (grp trt adult juv) (2 . 4)]
> (dataframe-filter df (filter-expr (grp juv) (and (symbol=? grp 'b) (< juv 50))))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp b b) (trt a b) (adult 3 4) (juv 30 40)) (grp trt adult juv) (2 . 4)]
```

#### <a name="df-partition"></a> procedure: `(dataframe-partition df filter-expr)`  
**returns:** two dataframes where the rows of dataframe `df` are partitioned according to the `filter-expr`  

```
> (define df (make-dataframe '((grp a a b b b)
                               (trt a b a b b)
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
> (dataframe-partition df (filter-expr (adult) (> adult 3)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp b b) (trt b b) (adult 4 5) (juv 40 50)) (grp trt adult juv) (2 . 4)]
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp a a b) (trt a b a) (adult 1 2 3) (juv 10 20 30)) (grp trt adult juv) (3 . 4)]
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
**returns:** two dataframes where the rows of dataframe `df` are partitioned according to the `filter-expr`  

```
> (define df (make-dataframe '((grp "a" "a" "b" "b" "b")
                               (trt "a" "b" "a" "b" "b")
                               (adult 1 2 3 4 5)
                               (juv 10 20 30 40 50))))
> (dataframe-sort df (sort-expr (string<? trt)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp "a" "b" "b" "a" "b") (trt "b" "b" "b" "a" "a") (adult 2 4 5 1 3) (juv 20 40 50 10 30)) (grp trt adult juv) (5 . 4)]
> (dataframe-sort df (sort-expr (string<? trt) (< adult)))
#[#{dataframe ip7681h1m7wugezzev2gzpgrk-3} ((grp "b" "b" "a" "b" "a") (trt "b" "b" "b" "a" "a") (adult 5 4 2 3 1) (juv 50 40 20 30 10)) (grp trt adult juv) (5 . 4)]
```

## Append and split  

## Modify and aggregate  
